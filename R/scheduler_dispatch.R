## Scheduling and dispatch: pick a worker for a ready task (locality + resource
## fit), serialize the execute-task payload, send it, and the try_schedule loop that
## drains the ready set. Used by the central scheduler (R/scheduler_run.R).

## Check whether a worker declares resources sufficient to satisfy the op's
## `requires`. Resources come from the launcher for launched/remote workers
## (`worker$launcher$resources`, authoritative) and from the worker itself for
## local workers (`worker$resources`, launcher = NULL). Returns TRUE if
## `requires` is NULL or empty (no constraint), or if every field declared in
## `requires` is matched with semantics: cpu/gpu numeric comparison >=, mem
## parsed via parse_mem_to_bytes() and compared >=, and tags matched by set
## membership (the worker must advertise every required tag). Returns FALSE if
## the worker declares no resources but the op does, or if any field is
## insufficient or missing.
worker_satisfies_requires <- function(worker, requires) {
  if (is.null(requires) || length(requires) == 0) {
    return(TRUE)
  }

  resources <- if (is.null(worker$launcher)) worker$resources else worker$launcher$resources
  if (is.null(resources)) {
    return(FALSE)
  }

  if (!is.null(requires$cpu)) {
    if (is.null(resources$cpu) || resources$cpu < requires$cpu) {
      return(FALSE)
    }
  }

  if (!is.null(requires$mem)) {
    if (is.null(resources$mem)) return(FALSE)
    if (parse_mem_to_bytes(resources$mem) < parse_mem_to_bytes(requires$mem)) {
      return(FALSE)
    }
  }

  if (!is.null(requires$gpu)) {
    if (is.null(resources$gpu) || resources$gpu < requires$gpu) {
      return(FALSE)
    }
  }

  ## tags: capability labels, matched by set membership (AND). The worker must
  ## advertise EVERY tag the task requires. A worker that declares no tags (or
  ## no resources at all) cannot satisfy a tag requirement.
  if (!is.null(requires$tags)) {
    if (is.null(resources$tags) || !all(requires$tags %in% resources$tags)) {
      return(FALSE)
    }
  }

  TRUE
}

select_worker <- function(op, sched_state, available_fifo, worker_list,
                          par_locations = NULL, shared_path = NULL) {
  ## Filter to workers whose launcher resources satisfy op$requires.
  ## Workers without declared resources are kept iff op$requires is NULL.
  if (!is.null(op$requires)) {
    keep <- vapply(seq_len(nrow(available_fifo)), function(i) {
      w_name <- available_fifo$Supervisor_ID[i]
      worker <- Find(function(w) w$name == w_name, worker_list)
      worker_satisfies_requires(worker, op$requires)
    }, logical(1))
    fifo <- available_fifo[keep, , drop = FALSE]
  } else {
    fifo <- available_fifo
  }

  ## Skip workers that are currently receiving any of the files this op needs,
  ## so a concurrent re-transfer cannot interleave with the in-flight write.
  ## Tasks that find no eligible worker simply remain in ready_queue and retry on the next
  ## scheduling event (file_received or RESULT). Local workers never appear in
  ## in_transit because chunked transfer is bypassed for them.
  task_files <- get_task_input_file_paths(op, par_locations, shared_path)
  if (length(task_files) > 0 && nrow(fifo) > 0) {
    keep_fs <- vapply(seq_len(nrow(fifo)), function(i) {
      w_name <- fifo$Supervisor_ID[i]
      wf <- sched_state$worker_files[[w_name]]
      if (is.null(wf)) return(TRUE)
      length(intersect(wf$in_transit, task_files)) == 0
    }, logical(1))
    fifo <- fifo[keep_fs, , drop = FALSE]
  }

  if (nrow(fifo) == 0) return(NA_integer_)
  dep_exe_ids <- unique(c(
    sched_state$deps$dep_map[[op$exe_id]],
    get_dep_exe_ids_from_task(op)
  ))
  slot_counts <- table(fifo$Supervisor_ID)
  if (length(dep_exe_ids) == 0) {
    best <- names(which.max(slot_counts))
    return(which(available_fifo$Supervisor_ID == best)[1])
  }
  ## Named integer vector, indexed by the unique fifo workers.
  locality_scores <- score_workers_for_task(dep_exe_ids,
                                            fifo$Supervisor_ID,
                                            sched_state$data_to_workers)
  max_score <- max(locality_scores)
  if (max_score == 0) {
    best <- names(which.max(slot_counts))
    return(which(available_fifo$Supervisor_ID == best)[1])
  }
  ## Among workers tied at max_score, prefer the one with most free slots.
  candidates_w <- names(locality_scores)[locality_scores == max_score]
  best <- names(which.max(slot_counts[names(slot_counts) %in% candidates_w]))
  return(which(available_fifo$Supervisor_ID == best)[1])
}

execute_single_task <- function(exe_id, chosen_socket, workflow, exe_id_to_idx, task_table, chunk_size,
                              chosen_worker_name, worker_list, sched_state = NULL,
                              task_status_env = NULL, supervisor_info = NULL) {
  task_index <- exe_id_to_idx[[exe_id]]
  task <- workflow$task_list[[task_index]]

  exe_object <- from_task_object_to_exe_object(task, exe_id)

  worker <- Find(function(w) w$name == chosen_worker_name, worker_list)
  is_local_worker <- is.null(worker$launcher)
  exe_object$cmd <- switch(task$type,
    R            = worker$R_cmd,
    python       = worker$python_cmd,
    shell        = worker$R_cmd,
    exe_object$cmd
  )
  if (task$type == "shell") {
    exe_object$shell_interpreter <- worker$shell_cmd
  }

  par_list_data_type <- exe_object$params_type
  par_list_data_type[!par_list_data_type %in% c("file", "file_path", "file_object")] <- "R_object"

  exp_values     <- list()
  exp_msg_types  <- character(0)
  exp_data_types <- character(0)
  exp_names      <- character(0)

  ## CONTRACT, per parameter, uuid-keyed like params_is_aggr: the delivered
  ## FORM the user declared ("bare", "list", "aggregate"), how many
  ## constituents feed the parameter, and for a file that is not a dependency
  ## how it reaches the worker ("bytes", "path") and why. Written here, where
  ## the decisions are taken. Not read by the supervisor yet.
  par_form   <- character(0)
  par_ncon   <- integer(0)
  par_fetch  <- character(0)
  par_reason <- character(0)
  is_aggr_of <- function(uuid) isTRUE(unname(exe_object$params_is_aggr[uuid]))

  for (j in seq_along(task$par_list)) {
    par_obj  <- task$par_list[[j]]
    par_val  <- par_obj$value
    par_name <- names(exe_object$par_list)[j]
    par_uuid <- par_obj$par_uuid

    ## de-reference a dep_object that survived expansion. At this point
    ## par$value can be:
    ##   (a) dep_object(value=output_req_list)  — dynamic dep on a task with
    ##       multiple outputs (e.g. alignment.genome with `{{N}}`)
    ##   (b) dep_object(value=output_req)       — single output post-resolve
    ##   (c) any non-dep value                  — handled normally
    ## Unwrapped to drive the branch matching below; par_obj itself is kept intact.
    if (inherits(par_val, "dep_object")) {
      par_val <- par_val$value
    }

    if (inherits(par_val, "output_req_list")) {
      ## ONE shape for a resolved dependency (as_output_req_list): a list of
      ## constituents, one per producer, a single producer included. One slot
      ## per constituent; the form of the parameter is declared once, below.
      for (req in par_val) {
        ## CONTRACT: a constituent built by the STATIC branch of the task table
        ## selects its result by output_instance and carries no template (the
        ## static producer reports none). Declare the producer's k-th declared
        ## FILE output as its template here, at dispatch, and not in the task
        ## table: there the value re-enters the wildcard engine, whose
        ## extract_dyn_key() reads a brace-less path as a wildcard NAME
        ## (R/object_func.R reorder_wildcard_keys, R/wildcard_engine.R to_expand).
        ## Every supervisor reader goes through find_dynamic_wildcards(), NULL
        ## for a static path as for "". Object outputs keep their empty template.
        if (is_req_static_no_template(req) && req_producer_known(req, exe_id_to_idx)) {
          .k    <- as.integer(req$output_instance)
          .outs <- Filter(function(p) inherits(p, "out_par_object"),
                          workflow$task_list[[exe_id_to_idx[[req$exe_id]]]]$par_list)
          if (.k >= 1L && .k <= length(.outs)) {
            .v <- get_value_from_object(.outs[[.k]])
            if (inherits(.v, "file_path") || inherits(.v, "file_object")) {
              req$dyn_wildcard <- as.character(.v)[1]
            }
          }
        }
        exp_values     <- c(exp_values, list(req))
        exp_msg_types  <- c(exp_msg_types, "parameter_request")
        exp_data_types <- c(exp_data_types, "R_object")
        exp_names      <- c(exp_names, par_name)
      }
      ## A gather is a list by declaration; several producers are a list; one
      ## producer is a list only when it resolves to more than one result, 
      ## and a dependency pinned to ONE result
      ## (output_instance set: the _exp form, one consumer instance per file,
      ## or a static producer that returned a single value) resolves to that
      ## result alone, whatever the output holds. A static producer whose one
      ## object output returned a list of several elements is bound to the
      ## whole result instead, as a dynamic one (R/scheduler_task_table.R).
      .one <- par_val[[1]]
      par_form[par_uuid]   <- if (is_aggr_of(par_uuid)) "aggregate"
                              else if (length(par_val) > 1L) "list"
                              else if (is.null(.one$output_instance) && isTRUE(.one$n_results > 1L)) "list"
                              else "bare"
      par_ncon[par_uuid]   <- length(par_val)
      par_fetch[par_uuid]  <- NA_character_
      par_reason[par_uuid] <- NA_character_
    } else if (inherits(par_obj$value, "dep_object") &&
               inherits(par_val, "aggregation") &&
               length(par_val) >= 1L &&
               !is.null(supervisor_info) && !is.null(sched_state)) {
      ## A static gather over a producer (new_dep + new_file_aggr with a
      ## static wildcard) is bound to its constituents at the notify of each
      ## producer instance (bind_static_gather_constituents, in
      ## R/scheduler_task_table.R) and reaches this point as the list above.
      ## Every producer instance has finished by the time the consumer is
      ## dispatched, so a vector of paths here means no notify of that task
      ## was ever seen: an engine invariant, not a workflow error.
      stop(flumen_fail(
        call  = "execute_single_task()",
        task  = task$task_id,
        error = sprintf("parameter '%s' is a static gather over '%s' with unbound constituents",
                        par_name, as.character(par_obj$value$task_id)[1]),
        fix   = "this is an engine invariant, not a workflow error: report the workflow"),
        call. = FALSE)
    } else {
      ## When par_obj$value was a dep_object, the inner value is what gets
      ## dispatched: pass `par_val` (already unwrapped) instead of the wrapper.
      ## A constituent outside its list is the invariant of as_output_req_list
      ## being false: reported, never dispatched as a plain value.
      if (inherits(par_val, "output_req")) {
        stop(flumen_fail(
          call  = "execute_single_task()",
          task  = task$task_id,
          error = sprintf("parameter '%s' holds a bare output_req, not a list of constituents",
                          par_name),
          fix   = "this is an engine invariant, not a workflow error: report the workflow"),
          call. = FALSE)
      }
      dispatch_val <- if (inherits(par_obj$value, "dep_object")) par_val
                      else exe_object$par_list[[j]]
      msg_t <- "parameter"
      dtype <- if (par_uuid %in% names(par_list_data_type)) par_list_data_type[[par_uuid]] else "R_object"
      ## Which of the four rules below turned a transfer into a path, so the
      ## supervisor can be TOLD instead of guessing it in its error message.
      fetch_reason <- NA_character_
      if (is_local_worker && dtype == "file") { dtype <- "file_path"; fetch_reason <- "local_worker" }
      ## File input wrapped with pre_staged(): the file is pre-staged
      ## on the executing worker (rsync/NFS/container volume) and the master
      ## must NOT read or transfer it. Indexed by the template par_uuid, which
      ## is preserved across wildcard expansion so the flag also applies to all
      ## clones.
      if (!is.null(workflow$par_locations) &&
          isFALSE(workflow$par_locations[[par_uuid]]) &&
          dtype == "file") {
        dtype <- "file_path"; fetch_reason <- "pre_staged"
      }
      ## Shared filesystem via run_workflow(shared_path=): if EVERY resolved path
      ## of this file param lies under a declared shared root, the consumer reads
      ## it in place -- no byte transfer (Model 3). Path-component (frame-strict)
      ## match; all-or-nothing on multi-path params, consistent with the
      ## already-on-worker skip below.
      if (!is.null(workflow$shared_path) && dtype == "file") {
        val <- exe_object$par_list[[j]]
        paths <- param_value_paths(val)
        paths <- paths[nzchar(paths)]
        if (length(paths) > 0 &&
            all(vapply(paths, is_under_shared_path, logical(1), workflow$shared_path))) {
          dtype <- "file_path"; fetch_reason <- "shared_path"
        }
      }
      ## Skip chunked transfer if every path of this file param is already on
      ## the chosen remote worker (completed via a previous file_received).
      ## Sending dtype="file_path" makes the supervisor read the file straight
      ## from disk. All-or-nothing on multi-path params (folder/list): if any
      ## path is missing the whole param is transferred.
      if (!is_local_worker && dtype == "file" && !is.null(sched_state)) {
        wf <- sched_state$worker_files[[chosen_worker_name]]
        if (!is.null(wf) && length(wf$completed) > 0) {
          val <- exe_object$par_list[[j]]
          paths <- param_value_paths(val)
          paths <- paths[nzchar(paths)]
          if (length(paths) > 0 && all(paths %in% wf$completed)) {
            dtype <- "file_path"; fetch_reason <- "already_present"
          }
        }
      }
      slots <- emit_param_slots(dispatch_val, dtype, msg_t, par_name, task$task_id)
      exp_values     <- c(exp_values, slots$values)
      exp_msg_types  <- c(exp_msg_types, slots$msg_types)
      exp_data_types <- c(exp_data_types, slots$data_types)
      exp_names      <- c(exp_names, slots$names)

      par_ncon[par_uuid] <- 0L
      ## A gather is a list by declaration; a plain value is a list only when
      ## it resolves to more than one element, for a
      ## file value its path count.
      par_form[par_uuid] <- if (is_aggr_of(par_uuid)) "aggregate"
                            else if (length(slots$values) > 1L) "list" else "bare"
      par_fetch[par_uuid] <- if (identical(dtype, "file")) "bytes"
                             else if (identical(dtype, "file_path")) "path"
                             else NA_character_
      par_reason[par_uuid] <- if (identical(dtype, "file")) "master_transfer"
                              else if (identical(dtype, "file_path") && is.na(fetch_reason)) "declared_path"
                              else fetch_reason
    }
  }

  ## Least-connections routing: reroute each output_req fetch to a least-loaded
  ## peer holder, if one
  ## holds (exe_id, out_instance); otherwise the request is left pointing at the
  ## producer. Single site covers every emit path above
  ## (output_req_list, single output_req, static-aggregation). The chosen source's
  ## transport triple replaces the producer's, kept coherent from one supervisor_info row.
  if (!is.null(sched_state) && !is.null(supervisor_info)) {
    for (i in seq_along(exp_values)) {
      if (identical(exp_msg_types[i], "parameter_request") &&
          inherits(exp_values[[i]], "output_req")) {
        exp_values[[i]] <- mesh_reroute_output_req(exp_values[[i]], chosen_worker_name,
                                                   sched_state, supervisor_info,
                                                   consuming_exe_id = exe_id)
      }
    }
  }

  ## CONTRACT: declare, per constituent, where it is and how it is obtained,
  ## now that the transport triple is final (mesh rerouting included).
  if (!is.null(supervisor_info)) {
    for (i in seq_along(exp_values)) {
      if (identical(exp_msg_types[i], "parameter_request") &&
          inherits(exp_values[[i]], "output_req")) {
        exp_values[[i]] <- declare_constituent(exp_values[[i]], chosen_worker_name,
                                               supervisor_info, task$type,
                                               workflow$shared_path)
        ## A shell consumer that takes the backtick PREFIX receives one string
        ## in place of the paths, whatever the gather: the form follows.
        if (isTRUE(exp_values[[i]]$shell_prefix)) {
          .u <- exe_object$par_uuids[match(exp_names[i], names(exe_object$par_list))]
          if (!is.na(.u)) par_form[.u] <- "bare"
        }
        ## A constituent read in place under a shared root arrives as a path, so
        ## the consumer's pre-launch check must be able to SAY why no bytes came
        ## if that root turns out not to be visible there. Declared on the
        ## parameter, which is where the supervisor reads it
        ## (params_fetch_reason -> the shared_path hint in
        ## R/supervisor_handlers.R). Only the shared case is declared here: the
        ## others on this branch are fetched, not read in place.
        if (identical(exp_values[[i]]$source, "shared")) {
          .us <- exe_object$par_uuids[match(exp_names[i], names(exe_object$par_list))]
          if (!is.na(.us)) par_reason[.us] <- "shared_path"
        }
      }
    }
  }
  exe_object$params_form           <- par_form
  exe_object$params_n_constituents <- par_ncon
  exe_object$params_fetch          <- par_fetch
  exe_object$params_fetch_reason   <- par_reason

  .sent <- serialize_and_send_exec_task(
    exe_object, exp_values, exp_msg_types, exp_data_types, exp_names,
    exe_id, chosen_socket, chunk_size, task_table, task_status_env,
    chosen_worker_name
  )
  return(list(task_table = .sent$task_table, dispatch_failed = .sent$dispatch_failed))
}

## A single output_req whose result is selected by instance and that carries
## no usable template: the shape the static branch of the task table builds.
is_req_static_no_template <- function(v) {
  inherits(v, "output_req") &&
    !is.null(v$output_instance) && !is.na(v$output_instance) &&
    (is.null(v$dyn_wildcard) || !is.character(v$dyn_wildcard) ||
       !nzchar(as.character(v$dyn_wildcard)[1]))
}
req_producer_known <- function(v, exe_id_to_idx) {
  !is.null(v$exe_id) && !is.na(v$exe_id) && !is.null(exe_id_to_idx) &&
    v$exe_id %in% names(exe_id_to_idx)
}

## Write the contract fields of ONE constituent: where the data is relative to
## the consumer's worker, how the consumer obtains it, and whether a shell
## consumer takes the backtick prefix instead of the paths. The scheduler is
## the only component that knows both ends, so this is written here and read
## by the supervisor, which then has nothing to deduce.
##
## The three rules:
##   - co-local  <=> the source socket is the consumer worker's own;
##   - same host <=> both workers have Host "" in supervisor_info, i.e. both
##                   run on the master's machine;
##   - prefix    <=> the template's dynamic part carries backticks and the
##                   consumer is a shell task (R/supervisor_deps.R).
## Two remote workers on one host therefore declare "remote" / "bytes".
##
## `source` and `fetch` fall to NA when the source worker is unknown (socket
## NA, producer never registered); the supervisor's unresolved-dependency
## error path already covers that case.
declare_constituent <- function(req, chosen_worker_name, supervisor_info, consumer_type,
                                shared_path = NULL) {
  src <- if (!is.null(req$socket) && !is.na(req$socket))
    supervisor_info[supervisor_info$Socket_data_req == req$socket, , drop = FALSE]
  else supervisor_info[FALSE, , drop = FALSE]
  cons <- supervisor_info[supervisor_info$Supervisor_ID == chosen_worker_name, , drop = FALSE]

  if (nrow(src) != 1L || nrow(cons) != 1L) {
    req$source <- NA_character_; req$fetch <- NA_character_
  } else if (identical(src$Supervisor_ID, chosen_worker_name)) {
    req$source <- "colocal";   req$fetch <- "cache"
  } else if (identical(src$Host, "") && identical(cons$Host, "")) {
    req$source <- "same_host"; req$fetch <- "path"
  } else if (constituent_under_shared_path(req, shared_path)) {
    ## Model 3 on a producer-to-consumer edge: the output lies under a shared
    ## root declared at launch, so the consumer reads it in place from the same
    ## absolute path. `fetch = "path"` is the SAME route the same-host case above
    ## already takes -- the consumer asks for "file_path" and the producer ships
    ## the path with no bytes (sup_handle_parameter_request) -- so this widens
    ## when that route is chosen and adds no new transport.
    req$source <- "shared";    req$fetch <- "path"
  } else {
    req$source <- "remote";    req$fetch <- "bytes"
  }

  ## The backtick prefix is the CONSUMER's: it is written in the consumer's
  ## dep pattern, never in the producer's output pattern that dyn_wildcard
  ## carries. So it is read off consumer_pattern (kept by the engine beside
  ## the producer's template), and the prefix text itself is declared, so
  ## that no reader has to look for backticks in any template.
  shell_prefix <- FALSE
  prefix <- NULL
  cp <- req$consumer_pattern
  if (identical(consumer_type, "shell") && !is.null(cp)) {
    dyn <- cp[find_dynamic_wildcards(cp)]
    if (length(dyn) > 0L) {
      suf <- extract_suffix_contents(dyn)
      if (!is.null(suf$indices)) {
        shell_prefix <- TRUE
        prefix <- as.character(suf$values)
      }
    }
  }
  req$shell_prefix <- shell_prefix
  req$prefix <- prefix
  req
}

## Multi-path expansion (cross-machine byte transfer): when dtype is "file"
## AND the resolved value contains >1 path, emit one slot per path, all
## sharing data_names = par_name, keeping the byte-stream layer's one-path-
## per-slot invariant. Empty entries are dropped (warning emitted). For
## dtype != "file" (R_object / file_path / parameter_request) emits a single
## slot with the original dispatch_val. Returns lists of equal length to be
## concatenated to the caller's exp_* accumulators.
emit_param_slots <- function(dispatch_val, dtype, msg_t, par_name, task_id) {
  px_kept <- if (dtype == "file") {
    px <- param_value_paths(dispatch_val)
    keep <- px[nzchar(px)]
    if (length(keep) < length(px)) {
      warning(flumen_warn(
        call      = "execute_single_task()",
        task      = task_id,
        parameter = sprintf("'%s'", par_name),
        warning   = sprintf(paste("%d empty path(s) dropped from a multi-path file value",
                                  "before dispatch"), length(px) - length(keep))),
        call. = FALSE)
    }
    keep
  } else character(0)

  if (length(px_kept) > 1L) {
    list(
      values     = lapply(px_kept, identity),
      msg_types  = rep(msg_t, length(px_kept)),
      data_types = rep(dtype, length(px_kept)),
      names      = rep(par_name, length(px_kept))
    )
  } else if (length(px_kept) == 1L) {
    ## Single non-empty path (either originally single, or reduced to one
    ## after dropping empties). Use the cleaned scalar so the byte-stream
    ## layer sees a length-1 value regardless of dispatch_val's original
    ## shape.
    list(
      values     = list(px_kept[[1]]),
      msg_types  = msg_t,
      data_types = dtype,
      names      = par_name
    )
  } else {
    ## dtype != "file" (R_object / file_path / parameter_request) OR
    ## dtype == "file" with all empty paths (degenerate). Preserve the
    ## original dispatch_val: file_path round-trips its classes via qs2
    ## serialization, R_object encodes via Arrow / qs2 as today.
    list(
      values     = list(dispatch_val),
      msg_types  = msg_t,
      data_types = dtype,
      names      = par_name
    )
  }
}

## Pack the exe_object and the per-param dispatch slots into the
## "execute_task" message envelope, serialize, send chunked to the chosen
## socket, and update the task_table status to "execution". Helper of
## execute_single_task: pure side effect on task_table + socket.
serialize_and_send_exec_task <- function(exe_object, exp_values, exp_msg_types,
                                          exp_data_types, exp_names, exe_id,
                                          chosen_socket, chunk_size,
                                          task_table, task_status_env,
                                          chosen_worker_name) {
  n_exp     <- length(exp_values)

  ## Every parameter dispatched as its own slot is ALSO sitting in
  ## exe_object$par_list, so the envelope would carry the payload TWICE;
  ## sup_handle_execute_task overwrites exe_object$par_list wholesale with the
  ## list rebuilt from the slots.
  ##
  ## Only the entries that travel as slots are emptied; par_uuid, names and
  ## classes stay, since those are structure the supervisor does read.
  ##
  ## By dispatch time par_list is NOT the list of par objects it is at build time:
  ## it is NAMED by parameter and each entry IS the value. So match on names() and
  ## blank the ENTRY -- single-bracket `[i] <- list(NULL)` keeps length and names.
  if (length(exp_names) && length(exe_object$par_list)) {
    .dispatched <- names(exe_object$par_list) %in% as.character(exp_names)
    if (any(.dispatched)) exe_object$par_list[.dispatched] <- list(NULL)
  }

  data      <- c(list(exe_object), exp_values)

  ## Encode a dispatched value in the CONSUMER's codec.
  ##
  ## One question per slot: is this slot a payload the consumer will decode?
  ##   element 1         -- the exe_object; the supervisor itself must read it
  ##   parameter_request -- a placeholder (output_req), an R object by nature
  ##   output parameter  -- a declaration, not payload. is_output_par is per
  ##                        PARAMETER, so map through par_names rather than by
  ##                        slot index, which drifts as soon as one parameter
  ##                        emits several slots (emit_param_slots).
  ##   non-Arrow type    -- Arrow cannot carry it. Leave it qs2 and let the
  ##                        guard in supervisor_lib.R name the type in words.
  ## serialize_data stamps the encoder's own type into data_type as it encodes,
  ## so the wire carries the label without any bookkeeping here.
  to_arrow  <- rep(FALSE, n_exp + 1)
  if(n_exp > 0 && isTRUE(get_wrapper_info(exe_object$type)$use_arrow)){
    is_out <- exe_object$is_output_par
    for(k in seq_len(n_exp)){
      if(!identical(exp_msg_types[k], "parameter")) next
      if(!identical(as.character(exp_data_types[k])[1], "R_object")) next
      w <- match(as.character(exp_names[k])[1], exe_object$par_names)
      if(!is.na(w) && length(is_out) >= w && isTRUE(is_out[w])) next
      v <- exp_values[[k]]
      if(is.raw(v)) next
      v_type <- tryCatch(get_R_type(v), error = function(e) "")
      if(!isTRUE(is_arrow_compatible_type(v_type))) next
      to_arrow[k + 1L] <- TRUE
    }
  }
  data_type <- c("R_object", exp_data_types)
  data      <- serialize_data(data, data_type, to_arrow)

  message <- list(
    sender     = rep("central_scheduler", n_exp + 1),
    msg_id     = exe_id,
    msg_type   = c("execute_task", exp_msg_types),
    data_type  = unlist(data$data_type$data_struct),
    data_names = c("", exp_names),
    data       = data$data,
    is_arrow   = data$data_type$is_arrow,
    file_name  = data$file_name
  )

  ## Deferred: this call runs inside the scheduler's event loop, and a loop that
  ## waits on a send goes deaf on the results uplink. The failure is reported by
  ## the loop through advance_pending_sends() instead.
  send_ok <- send_message_by_chunks("Central scheduler", message, chosen_socket,
                                    chunk_size = chunk_size,
                                    defer = list(
                                      channel = paste0("sock_ctrl_", chosen_worker_name),
                                      context = list(kind   = "dispatch",
                                                     exe_id = exe_id,
                                                     worker = chosen_worker_name)))

  ## A detectably-failed dispatch (send_ok=FALSE: a chunk drained with a timeout
  ## or a dead-but-established pipe) must NOT be marked "execution". Surfaced so
  ## the scheduler aborts and reports (sched_report_dispatch_failure).
  if (!isTRUE(send_ok)) {
    return(list(task_table      = task_table,
                dispatch_failed = list(exe_id = exe_id, worker = chosen_worker_name)))
  }

  ## "dispatched", not "execution": the message has been sent and nobody has
  ## confirmed it. The worker's TASK_EXECUTION writes "execution".
  if (!is.null(task_status_env)) {
    task_table <- set_task_status(task_table, task_status_env, exe_id, "dispatched")
  } else {
    task_table[task_table$Exe_ID == exe_id, "Status"] <- "dispatched"
  }
  ## The Worker is recorded HERE and not only on the worker's confirmation, so a
  ## task that was sent and never acknowledged still names the machine holding
  ## it. sched_check_worker_silence() reads it.
  .wi <- match(exe_id, task_table$Exe_ID)
  if (!is.na(.wi)) task_table$Worker[.wi] <- chosen_worker_name
  list(task_table = task_table, dispatch_failed = NULL)
}

try_schedule <- function(sched_state, workflow, exe_id_to_idx,
                          available_fifo, socket_list, task_table, chunk_size,
                          worker_list, task_status_env = NULL,
                          supervisor_info = NULL) {
  dispatch_failed <- NULL
  ## The dependency ids of an op are re-derived below for EVERY ready op on every
  ## scheduling round, and they cannot change between rounds unless the instance
  ## set does: neither dep_map nor task_list is written here. Cached in
  ## sched_state so it survives across calls; R/scheduler_handlers.R clears it
  ## when an expansion changes the instance set.
  if (is.null(sched_state$dep_ids_cache))
    sched_state$dep_ids_cache <- new.env(parent = emptyenv())
  while (length(sched_state$deps$ready_queue) > 0 && nrow(available_fifo) > 0) {
    ## Pick the ready op with the highest locality score for currently available workers
    ## that also satisfy the op's resource requirements.
    scores <- sapply(sched_state$deps$ready_queue, function(eid) {
      op <- workflow$task_list[[exe_id_to_idx[[eid]]]]
      if (!is.null(op$requires)) {
        compatible <- vapply(unique(available_fifo$Supervisor_ID), function(w_name) {
          worker <- Find(function(w) w$name == w_name, worker_list)
          worker_satisfies_requires(worker, op$requires)
        }, logical(1))
        if (!any(compatible)) return(-Inf)
      }
      ## Boxed in a list: an op with no dependencies yields NULL, and storing the
      ## NULL itself would make every lookup a miss.
      cached <- sched_state$dep_ids_cache[[eid]]
      if (is.null(cached)) {
        cached <- list(v = unique(c(sched_state$deps$dep_map[[eid]],
                                    get_dep_exe_ids_from_task(op))))
        assign(eid, cached, envir = sched_state$dep_ids_cache)
      }
      dep_ids <- cached$v
      if (length(dep_ids) == 0) return(0L)
      sw <- score_workers_for_task(dep_ids,
                                   available_fifo$Supervisor_ID,
                                   sched_state$data_to_workers)
      if (length(sw) == 0) -Inf else max(sw)
    })
    if (all(!is.finite(scores))) break
    ## Locality decides first; among ops that tie on it, serve the one with the
    ## FEWEST eligible workers, so a task pinned by `requires` to one worker is
    ## not left behind by a task that runs anywhere. Inert where nothing is
    ## pinned: every candidate counts every worker and which.min keeps the first.
    ##
    ## Counted over EVERY declared worker, not over the ones free at this
    ## instant: scarcity is a property of the task against the machine park.
    ##
    ## The op selected here is still runnable on some free worker: an op no free
    ## worker satisfies already scored -Inf above and is not in `cand`.
    cand <- which(scores == max(scores))
    if (length(cand) > 1L) {
      n_eligible <- vapply(sched_state$deps$ready_queue[cand], function(eid) {
        op_c <- workflow$task_list[[exe_id_to_idx[[eid]]]]
        w_names <- vapply(worker_list, function(w) w$name, character(1))
        if (is.null(op_c$requires)) length(w_names)
        else sum(vapply(w_names, function(w_name) {
          worker <- Find(function(w) w$name == w_name, worker_list)
          worker_satisfies_requires(worker, op_c$requires)
        }, logical(1)))
      }, numeric(1))
      best_idx <- cand[which.min(n_eligible)]
    } else {
      best_idx <- cand[1]
    }
    .taken     <- deps_take(sched_state, best_idx)
    sched_state <- .taken$sched_state
    exe_id      <- .taken$exe_id
    op <- workflow$task_list[[exe_id_to_idx[[exe_id]]]]
    chosen_idx <- select_worker(op, sched_state, available_fifo, worker_list,
                                workflow$par_locations, workflow$shared_path)
    if (is.na(chosen_idx)) {
      sched_state <- deps_put_back(sched_state, exe_id)
      break
    }
    chosen_worker_name <- available_fifo[chosen_idx, "Supervisor_ID"]
    ## EXECUTE_TASK is sent on the per-worker dispatch socket sock_ctrl_<name>,
    ## which is established at scheduler startup (local: listen IPC) or after
    ## launch_local_and_register_supervisors (remote: listen TCP). The worker pulls
    ## from the
    ## same channel on which it received the handshake.
    chosen_socket <- socket_list[[paste0("sock_ctrl_", chosen_worker_name)]]

    ## Mark input files as in_transit toward the chosen worker BEFORE the send.
    ## Skipped for local workers (no chunked transfer path is taken anyway, since
    ## execute_single_task downgrades dtype to "file_path" for them). Files that
    ## are already in `completed[chosen_worker]` are NOT marked in_transit,
    ## because execute_single_task below downgrades their dtype to file_path and
    ## no transfer happens.
    chosen_worker <- Find(function(w) w$name == chosen_worker_name, worker_list)
    if (!is.null(chosen_worker) && !is.null(chosen_worker$launcher)) {
      task_files <- get_task_input_file_paths(op, workflow$par_locations, workflow$shared_path)
      wf <- sched_state$worker_files[[chosen_worker_name]]
      already_done <- if (!is.null(wf)) wf$completed else character(0)
      to_transfer <- setdiff(task_files, already_done)
      sched_state <- mark_files_in_transit(sched_state, chosen_worker_name, to_transfer)
    }

    .est <- execute_single_task(exe_id, chosen_socket, workflow,
                                   exe_id_to_idx, task_table, chunk_size,
                                   chosen_worker_name, worker_list, sched_state,
                                   task_status_env = task_status_env,
                                   supervisor_info = supervisor_info)
    task_table <- .est$task_table
    ## A detectably-failed dispatch: stop scheduling and hand the failure up so
    ## the state-holding caller aborts and reports.
    if (!is.null(.est$dispatch_failed)) { dispatch_failed <- .est$dispatch_failed; break }
    ## drop = FALSE: available_fifo now has a single column (Supervisor_ID),
    ## so without it R coerces the slice to a vector and the next nrow() check
    ## raises "missing value where TRUE/FALSE needed".
    available_fifo <- available_fifo[-chosen_idx, , drop = FALSE]
  }
  list(sched_state = sched_state, available_fifo = available_fifo,
       task_table = task_table, dispatch_failed = dispatch_failed)
}

