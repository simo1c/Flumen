## The scheduler task table: build it from the execution plan, expand/update it as
## wildcard outputs resolve, and fold a completed task result-notify back into the
## table + workflow shapes. Used by the central scheduler (R/scheduler_run.R).

init_task_table <- function(exec_plan, workflow){
  
  par_levels <- exec_plan$parallel_levels_exec
  task_list <- workflow$task_list
  
  task_table <- data.frame(
    Exe_ID = NULL,
    Op_ID = NULL,
    Level = NULL,
    Worker = NULL,
    Shape = NULL,
    Colour = NULL
  )
  
  for(i in seq_along(par_levels)){

    task_id_level <- par_levels[[i]]
    
    w <- sapply(task_list, function(op){
      return(op$task_id %in% task_id_level)
    })
    
    op_sel <- task_list[w]
    
    level_info <- lapply(op_sel, function(op){
      return(c(op$exe_id, op$task_id, exec_plan$levels[[i]], NA, op$shape, op$colour))
    })
    
    level_info_df <- as.data.frame(do.call(rbind, level_info))
    colnames(level_info_df) <- c("Exe_ID", "Op_ID", "Level", "Worker", "Shape", "Colour")
    task_table <- rbind(task_table, level_info_df)
  }
  
  task_table$Status <- "init_supervisor"
  
  return(task_table)
}

update_task_table <- function(task_list, task_table, replace_op) {

  ## Fast path for when nothing is being replaced, which is most results. It
  ## returns the same object the slow path would: it writes both columns the slow
  ## path can still touch, Colour refreshed from the op and Shape reset to the
  ## exe_id (the table is built with `op$shape`, which the slow path overwrites).
  ##
  ## An id PRESENT in replace_op is not an id being replaced: every op the
  ## expansion visits gets an entry, so an op that was not expanded leaves
  ## `id -> itself` (workflow_prepare.R). The question that decides the work is
  ## whether any id maps to something OTHER than itself. One match() for the whole
  ## table, then a scan that stops at the first real substitution.
  ex_ids   <- as.character(task_table$Exe_ID)
  idx_rep  <- match(ex_ids, names(replace_op))
  has_real <- FALSE
  for(k in which(!is.na(idx_rep))){
    v <- replace_op[[idx_rep[k]]]
    if(length(v) != 1L || !identical(as.character(v), ex_ids[k])){
      has_real <- TRUE
      break
    }
  }

  if(!length(replace_op) || !has_real){
    oc <- lapply(task_list, function(op) op$colour)
    names(oc) <- vapply(task_list, function(op) op$exe_id, character(1))
    task_table$Shape <- as.character(task_table$Exe_ID)
    idx <- match(as.character(task_table$Exe_ID), names(oc))
    hit <- !is.na(idx)
    if(any(hit)) task_table$Colour[hit] <- as.character(unlist(oc[idx[hit]]))
    return(task_table)
  }

  op_colours <- lapply(task_list, function(op){
    op$colour
  })
  
  task_id <- sapply(task_list, function(op){
    op$exe_id
  })
  
  names(op_colours) <- task_id
  
  row_list <- lapply(seq_len(nrow(task_table)), function(i) {
    
    current_row <- task_table[i, , drop = FALSE]
    old_id <- as.character(current_row$Exe_ID)
    
    if (old_id %in% names(replace_op)) {
      new_ids <- replace_op[[old_id]]
    } else {
      new_ids <- old_id
    }
    
    new_block <- current_row[rep(1, length(new_ids)), ]
    new_block$Exe_ID <- new_ids
    new_block$Shape <- new_ids
    
    new_block$Colour <- sapply(new_ids, function(id) {
      if (id %in% names(op_colours)) {
        return(as.character(op_colours[[id]]))
      } else {
        return(as.character(current_row$Colour))
      }
    })
    
    return(new_block)
  })
  
  result_table <- do.call(rbind, row_list)
  rownames(result_table) <- NULL
  
  return(result_table)
}


## A static gather (new_dep(producer, new_file_aggr("out/{s}.txt"))) is
## expanded by the wildcard engine into its concrete paths, one per producer
## instance, and that vector says nothing about WHICH instance produces which
## path. The pairing is written where it is born, at the notify of each producer
## instance: the container becomes an output_req_list, one constituent per path
## in the order of the paths, the first time any instance of the producer task
## finishes, every constituent starting as a placeholder (exe_id NA), and each
## notify fills the constituents whose path that instance declares among its
## outputs. The consumer is dispatched only when every producer instance has
## finished, so a placeholder still there at dispatch is an unresolved
## constituent, which the supervisor refuses. A template that still carries a
## dynamic wildcard cannot be paired by path (the outputs of a dynamic producer
## are patterns, not paths) and keeps the template as its own.
bind_static_gather_constituents <- function(workflow, op_to_expand, finished_task,
                                            res_notify, socket_worker, uses_tcp_worker){
  prod_paths <- normalize_path_lexical(as.character(unlist(lapply(
    Filter(function(p) inherits(p, "out_par_object"), finished_task$par_list),
    function(p) get_value_from_object(p)))))
  prod_paths <- prod_paths[!grepl("[{][{]", prod_paths)]
  n_out      <- length(finished_task$output_files)
  fill <- function(req){
    if(!is.na(req$exe_id)) return(req)
    if(!is.null(find_dynamic_wildcards(req$dyn_wildcard))) return(req)
    key <- normalize_path_lexical(as.character(req$dyn_wildcard)[1])
    if(!(key %in% prod_paths)) return(req)
    req$exe_id   <- res_notify$exe_id
    req$socket   <- socket_worker
    req$uses_tcp <- isTRUE(uses_tcp_worker)
    ## WHICH output of that producer this constituent wants, said with a path
    ## regex (the dispatcher's surrogate for the output's name): only when the
    ## producer declares more than one output.
    ##
    ## Through p2p_pattern_to_regex(), so the anchoring rule lives in one place,
    ## and anchored at "^": a suffix match would also claim a file of a DEEPER
    ## declared output.
    req$output_path_sel <- if(n_out > 1L)
      p2p_pattern_to_regex(as.character(req$dyn_wildcard)[1]) else NULL
    req
  }
  for(i in op_to_expand[!is.na(op_to_expand)]){
    for(j in seq_along(workflow$task_list[[i]]$par_list)){
      v <- workflow$task_list[[i]]$par_list[[j]]$value
      inner <- if(inherits(v, "dep_object")) v$value else v
      ## A DYNAMIC gather ("parts/{s}_{{n}}.txt") is also a dep_object holding
      ## an aggregation before its first notify, and it is bound by the
      ## wildcard engine (branch 3), one constituent per notify: it is not
      ## touched here. Only a gather whose paths are all concrete is static.
      if(inherits(v, "dep_object") && inherits(inner, "aggregation") &&
         identical(v$task_id, finished_task$task_id) &&
         !any(grepl("[{][{]", as.character(inner))) &&
         !any(grepl("[{][{]", as.character(attr(inner, "original_template"))))){
        template <- attr(inner, "original_template")
        if(is.null(template)) template <- as.character(inner)
        template <- template[1]
        paths <- unique(as.character(inner))
        container <- do.call(new_output_req_list, lapply(paths, function(path)
          new_output_req(
            dyn_wildcard     = if(is.null(find_dynamic_wildcards(template))) path else template,
            consumer_pattern = template,
            exe_id           = NA_character_,
            output_instance  = NULL,
            socket           = NA_character_,
            uses_tcp         = FALSE)))
        attr(container, "gather_marker") <- TRUE
      } else if(inherits(inner, "output_req_list") &&
                any(vapply(inner, function(r) is.na(r$exe_id), logical(1)))){
        container <- inner
      } else next
      for(k in seq_along(container)) container[[k]] <- fill(container[[k]])
      workflow$task_list[[i]]$par_list[[j]]$value <- container
    }
  }
  workflow
}


## The plan authorises the fact. A consumer's parameters name the producer
## instance each value came from, and that name is written at the producer's
## notify by two independent rules: the static gather pairs by PATH
## (bind_static_gather_constituents above) and the dynamic one by the VALUE of
## the dynamic wildcard (R/wildcard_engine.R). Neither consults the plan, the
## scheduler's own child -> parents relation, so nothing but coincidence keeps
## the three in agreement. This is where that agreement stops being a
## coincidence: a producer a consumer's parameters name must be a producer the
## plan sent to that consumer.
##
## Scoped to `op_to_expand`, the consumers this notify could have touched, and
## the plan is turned into a hashed set once per consumer.
##
## A consumer with NO plan entry is skipped rather than refused; the fills it
## carries are still checked against every other consumer's plan.
assert_fills_authorised_by_plan <- function(workflow, op_to_expand){
  shapes <- workflow$required_shapes
  if(is.null(shapes) || is.null(op_to_expand)) return(invisible(NULL))
  for(i in op_to_expand[!is.na(op_to_expand)]){
    op <- workflow$task_list[[i]]
    if(is.null(op)) next
    plan <- as.character(shapes[[as.character(op$exe_id)[1]]])
    if(!length(plan)) next
    allowed <- new.env(hash = TRUE, parent = emptyenv())
    for(p in plan) allowed[[p]] <- TRUE
    for(par in op$par_list){
      val <- if(inherits(par$value, "dep_object")) par$value$value else par$value
      if(!inherits(val, "output_req_list")) next
      for(req in val){
        e <- req$exe_id
        if(is.null(e) || !length(e) || is.na(e)) next
        if(is.null(allowed[[as.character(e)[1]]])){
          stop(flumen_fail(
            call  = "result notify",
            task  = as.character(op$task_id)[1],
            error = sprintf(paste("parameter '%s' was bound to producer instance '%s', which the",
                                  "dependency map does not list among this task's parents"),
                            as.character(par$name)[1], as.character(e)[1]),
            fix   = "this is an engine invariant, not a workflow error: report the workflow"),
            call. = FALSE)
        }
      }
    }
  }
  invisible(NULL)
}

process_result_notify_and_update_workflow <- function(workflow,
                                                      sup_info,
                                                      res_notify,
                                                      exe_id_to_idx = NULL,
                                                      required_by = NULL,
                                                      cumulative_replace_op = NULL){

## op_to_expand = indices of tasks in workflow$required_shapes whose parent set
## contains the just-finished res_notify$exe_id. The reverse index
## `required_by[[parent_exe_id]] = c(child_exe_id, ...)` gives it in O(1) plus a
## loop over the children. Falls back to a scan over required_shapes for callers
## that do not provide the index.
op_to_expand <- if(!is.null(required_by)){
    children <- required_by[[res_notify$exe_id]]
    if(is.null(children)) integer(0)
    else match(children, names(workflow$required_shapes))
  } else {
    which(sapply(workflow$required_shapes, function(parent_exe_id){
      res_notify$exe_id %in% parent_exe_id
    }))
  }

  ### Must handle parameters-to-expand correctly, creating wildcards properly.
  ### If an operation input is NOT _exp, do not pass a wildcard with multiple
  ### elements -- otherwise it would be expanded anyway. The issue is that
  ### multiple operations can depend on the same output parameter of another
  ### operation but not all of them define an expansion parameter -- hence
  ### op_to_expand and wildcard_list must be managed correctly.

  socket_worker   <- sup_info[sup_info$Supervisor_ID == res_notify$Supervisor_ID, "Socket_data_req"]
  host_worker     <- sup_info[sup_info$Supervisor_ID == res_notify$Supervisor_ID, "Host"]
  uses_tcp_worker <- sup_info[sup_info$Supervisor_ID == res_notify$Supervisor_ID, "Uses_tcp"]
  if (length(socket_worker) == 0) {
    stop(flumen_fail(
      call  = "result notify",
      error = sprintf(paste("worker '%s' is not in supervisor_info; the known workers are %s"),
                      res_notify$Supervisor_ID,
                      paste(shQuote(sup_info$Supervisor_ID), collapse = ", ")),
      fix   = paste0("the worker_id passed to start_worker() must match the 'name' given to ",
                     "new_worker(), e.g. new_worker(name = \"ipc2\") is started with ",
                     "Rscript -e 'flumen::start_worker(master_url = \"tcp://host:61000,61001\", ",
                     "worker_id = \"ipc2\")'")), call. = FALSE)
  }
  
  out_req_list <- lapply(1:res_notify$n_outputs, function(out_instance){
    new_output_req(
      res_notify$dyn_wildcard,
      res_notify$exe_id,
      out_instance,
      socket_worker,
      uses_tcp_worker,
      ## one result per instance until the dynamic branch below regroups the
      ## results by declared output and overwrites it with the group size
      n_results = 1L,
      result_path = if(length(res_notify$output_files) >= out_instance &&
                       !is.na(res_notify$output_files[out_instance]))
                      as.character(res_notify$output_files[out_instance]) else NULL
    )
  })

  names(out_req_list) <- paste0(res_notify$exe_id, "_", 1:res_notify$n_outputs)
  wildcard_name <- names(get_wildcards(res_notify$dyn_wildcard))

  if(is.null(wildcard_name)){

    ## Static task: no expansion needed.
    ## Directly replaces dep_object with output_req in the par_list of
    ## dependent tasks, without renaming exe_ids.
    ## Also handles the case where the dep was declared with a pre-specified
    ## value (new_dep(task, value=new_file(...))): par$value$value carries the
    ## concrete path produced by the expansion, and
    ## we match it against res_notify$dyn_wildcard so multi-instance producers
    ## (e.g. alignment_1/_2/_3 emitting sam/3019.sam, sam/3022.sam, sam/3023.sam)
    ## land on the right consumer instance and not on every dep_object that
    ## happens to share the parent task_id.
    if(length(out_req_list) > 0 && length(op_to_expand) > 0){
      ## Lookup task index by exe_id. exe_id_to_idx (named integer vector) maps
      ## exe_id -> task_list position; single-bracket [name] yields NA on miss.
      ## Falls back to the which() scan for callers with no index.
      finished_idx <- if(!is.null(exe_id_to_idx)){
        i <- exe_id_to_idx[res_notify$exe_id]
        if(is.na(i)) integer(0) else as.integer(i)
      } else {
        which(sapply(workflow$task_list, function(op) op$exe_id == res_notify$exe_id))
      }
      if(length(finished_idx) > 0){
        finished_task    <- workflow$task_list[[finished_idx]]
        finished_task_id <- finished_task$task_id
        ## Resolved output identity of the finished producer instance: the concrete
        ## path for file outputs, the object name for object outputs, taken from its
        ## OUTPUT parameter(s). res_notify$dyn_wildcard carries this identity only for
        ## DYNAMIC ({{}}) producers, which are routed to the dynamic branch below; a
        ## STATIC-only producer (this branch) instead carries the per-parameter
        ## logical dyn_wildcard flag, so matching a dep's explicit value against it
        ## always fails. Matching against the real output identity lets them
        ## resolve to an output_req (P2P fetch), like value-less deps.
        prod_outs    <- Filter(function(p) inherits(p, "out_par_object"), finished_task$par_list)
        prod_out_ids <- normalize_path_lexical(as.character(unlist(lapply(
          prod_outs, function(p) get_value_from_object(p)))))
        ## Parallel to prod_out_ids: the declared output each id belongs to, and
        ## whether it is an object output. Several OBJECT outputs are told apart
        ## by the label the producer states per result (the names of the list it
        ## returned), as in the dynamic branch; a position would follow the order
        ## the list was written in, and fell back to the first output when the
        ## producer reported one result, with exit 0.
        prod_out_names <- unlist(lapply(prod_outs, function(p)
          rep(as.character(p$name)[1], length(get_value_from_object(p)))))
        prod_out_obj   <- unlist(lapply(prod_outs, function(p)
          rep(!inherits(p$value, "file_path"), length(get_value_from_object(p)))))
        n_obj_outs     <- sum(vapply(prod_outs, function(p) !inherits(p$value, "file_path"),
                                     logical(1)))
        lab_vec_static <- as.character(res_notify$output_par_names)
        ## Static gathers over this producer: the constituents of THIS instance
        ## are bound now (see bind_static_gather_constituents).
        workflow <- bind_static_gather_constituents(workflow, op_to_expand, finished_task,
                                                    res_notify, socket_worker, uses_tcp_worker)
        for(i in op_to_expand){
          for(j in seq_along(workflow$task_list[[i]]$par_list)){
            par <- workflow$task_list[[i]]$par_list[[j]]
            if("dep_object" %in% class(par$value) &&
               par$value$task_id == finished_task_id){
              dep_inner <- par$value$value
              ## Normalize both sides lexically so cosmetic path variants
              ## (./x vs x, double slash, backticks already stripped by
              ## expansion) do not break the match. match_ok is deliberately
              ## permissive: the third clause is the dyn_wildcard match, and the
              ## in_out clause adds the static explicit-value case.
              di_norm   <- normalize_path_lexical(as.character(dep_inner))
              in_out    <- !is.null(dep_inner) && length(di_norm) > 0 &&
                           all(di_norm %in% prod_out_ids)
              matched_k <- if(in_out) match(di_norm[1], prod_out_ids) else NA_integer_
              match_ok  <- is.null(dep_inner) || in_out ||
                           identical(di_norm,
                                     normalize_path_lexical(as.character(res_notify$dyn_wildcard)))
              if (match_ok) {
                ## One dependency, one result. Single-output producer -> always
                ## out_req_list[[1]] (k == 1); only a multi-output identity match
                ## selects a different instance.
                if(!is.na(matched_k) && n_obj_outs > 1L && isTRUE(prod_out_obj[matched_k])){
                  ## One of several object outputs: the result whose label is
                  ## that output's name, and output_sel so the delivery checks
                  ## it, refusing by name when the producer stated no labels.
                  .out_name <- prod_out_names[matched_k]
                  k <- match(.out_name, lab_vec_static)
                  if(is.na(k) || k > length(out_req_list)) k <- 1L
                  .req <- out_req_list[[k]]
                  .req$output_sel <- .out_name
                  workflow$task_list[[i]]$par_list[[j]]$value <- as_output_req_list(.req)
                  next
                }
                if(n_obj_outs == 1L && length(out_req_list) > 1L &&
                   (is.na(matched_k) || isTRUE(prod_out_obj[matched_k]))){
                  ## The one object output of this producer, returned as a list
                  ## of several elements: the dependency asks for the WHOLE
                  ## result (no output_instance) and states how many results it
                  ## holds, exactly as a plain dependency on a dynamic output
                  ## does, so the delivery returns the list -- one fetch, also
                  ## across workers. Taking the first delivered list(1, 2) as 1.
                  .req <- out_req_list[[1]]
                  .req["output_instance"] <- list(NULL)
                  .req$n_results <- length(out_req_list)
                  workflow$task_list[[i]]$par_list[[j]]$value <- as_output_req_list(.req)
                  next
                }
                k <- if(is.null(dep_inner) || is.na(matched_k) ||
                        matched_k > length(out_req_list)) 1L else matched_k
                workflow$task_list[[i]]$par_list[[j]]$value <- as_output_req_list(out_req_list[[k]])
              }
            }
          }
        }
      }
    }


    ## The static branch's fills are complete here: check them against the plan
    ## before the workflow leaves this function.
    assert_fills_authorised_by_plan(workflow, op_to_expand)

    return(list(
      workflow = workflow,
      replace_op = NULL
    ))

  }else{

    ## Dynamic task: expansion via wildcards.
    ##
    ## A producer may declare SEVERAL dynamic outputs (reads_{{r}} AND
    ## metrics_{{m}}), which res_notify$dyn_wildcard cannot express: the wrapper
    ## reports one pattern, the first declared. The notify needs no new field:
    ## the producer's declared output patterns are here in the task list, so the
    ## split is reconstructed locally and the wire format is untouched.
    ##
    ## Each output gets its own wildcard and its own pattern, plus an output_sel
    ## naming it when the output is NOT a file_path, so the delivery ships only
    ## that output. File outputs need no selector: their patterns are matched
    ## against the delivered paths by resolve_file_param_paths(). A single
    ## dynamic output whose {{wc}} takes N values keeps one wildcard holding all
    ## N results.
    dyn_outs <- list()
    if(!is.null(exe_id_to_idx)){
      .i <- exe_id_to_idx[res_notify$exe_id]
      if(!is.na(.i))
        dyn_outs <- Filter(
          function(p) inherits(p, "out_par_object") &&
                      any(grepl("[{][{]", as.character(get_value_from_object(p)))),
          workflow$task_list[[as.integer(.i)]]$par_list)
    }

    ## WHICH RESULTS BELONG TO WHICH DECLARED OUTPUT. out_req_list has one entry
    ## per RESULT, and it doubles as the VALUE SET of the dynamic wildcard, so
    ## binding a wildcard to the whole list makes a consumer fan out once per
    ## result of the WHOLE TASK instead of once per file of ITS output.
    ##
    ## The producer states, per result, which declared output it came from, so
    ## the grouping is read rather than guessed.
    lab_vec   <- as.character(res_notify$output_par_names)
    dyn_names <- vapply(dyn_outs, function(p) as.character(p$name)[1], character(1))
    ## The labels must EXPLAIN this producer: one per result, and every declared
    ## dynamic output present among them. When they do not (a producer that
    ## states nothing, e.g. several object outputs returned unnamed), the
    ## construction below stands and the rejection downstream still fires.
    labels_explain <- length(lab_vec) == length(out_req_list) &&
                      length(dyn_names) > 0L && all(nzchar(dyn_names)) &&
                      all(dyn_names %in% lab_vec)

    ## `pat` below takes EVERY declared path of an output, not just the first:
    ## from the first alone the narrowing that tells a BUNDLE output apart
    ## cannot be derived.
    w_list <- if(labels_explain && length(dyn_outs) > 1L){
      do.call(new_wildcard_list, lapply(seq_along(dyn_outs), function(i){
        pat <- as.character(get_value_from_object(dyn_outs[[i]]))
        mine <- out_req_list[lab_vec == dyn_names[i]]   ## keeps the exe_id_i names
        for(.k in seq_along(mine)){
          mine[[.k]]$dyn_wildcard <- pat
          mine[[.k]]$n_results    <- length(mine)
          ## Named for FILE outputs too: resolve_file_param_paths narrows by
          ## pattern on the consumer's own worker, and nothing does so on the
          ## P2P path.
          mine[[.k]]$output_sel <- dyn_names[i]
        }
        new_wildcard(names(get_wildcards(pat)), mine)
      }))
    } else if(labels_explain){
      ## ONE declared dynamic output: same wildcard name as before, and only the
      ## results that belong to it. The producer's other outputs are reached by
      ## their own patterns, not by this wildcard's value set.
      mine <- out_req_list[lab_vec == dyn_names[1]]
      for(.k in seq_along(mine)){
        mine[[.k]]$output_sel <- dyn_names[1]
        mine[[.k]]$n_results  <- length(mine)
      }
      new_wildcard_list(new_wildcard(wildcard_name, mine))
    } else if(length(dyn_outs) > 1L && length(dyn_outs) == length(out_req_list)){
      ## LAST RESORT, for a producer that states nothing: pair declared outputs
      ## with results BY POSITION. It holds only when every declared output
      ## produced exactly one result, which is why it cannot be the main rule;
      ## it is what makes the rejection downstream name the output it could not
      ## find.
      do.call(new_wildcard_list, lapply(seq_along(dyn_outs), function(i){
        pat <- as.character(get_value_from_object(dyn_outs[[i]]))
        one <- out_req_list[i]                    ## keeps the exe_id_i name
        one[[1]]$dyn_wildcard <- pat
        if(!inherits(dyn_outs[[i]]$value, "file_path"))
          one[[1]]$output_sel <- dyn_names[i]
        new_wildcard(names(get_wildcards(pat)), one)
      }))
    } else {
      for(.k in seq_along(out_req_list)) out_req_list[[.k]]$n_results <- length(out_req_list)
      new_wildcard_list(new_wildcard(wildcard_name, out_req_list))
    }

    ## Nothing consumes this producer: the expansion below is guarded by
    ## `i %in% op_to_expand`, so it would walk every instance and re-register
    ## every one of them to hand back what it was given.
    if(!length(op_to_expand[!is.na(op_to_expand)])){
      return(list(workflow = workflow, replace_op = NULL))
    }

    ## Dependencies on the producer's NON-dynamic outputs.
    ## The wildcard list above is built from dyn_outs alone, and "the producer's
    ## other outputs are reached by their own patterns" holds CO-LOCALLY, where
    ## the path is already on the consumer's disk, but not across the network,
    ## where nobody transfers it. The producer's labels say which result belongs
    ## to which output, so the pairing is READ instead of guessed.
    if(length(op_to_expand[!is.na(op_to_expand)]) > 0 &&
       length(lab_vec) == length(out_req_list) && !is.null(exe_id_to_idx)){
      .pi <- exe_id_to_idx[res_notify$exe_id]
      if(!is.na(.pi)){
        .prod <- workflow$task_list[[as.integer(.pi)]]
        .stat <- Filter(function(p) inherits(p, "out_par_object") &&
                          !any(grepl("[{][{]", as.character(get_value_from_object(p)))),
                        .prod$par_list)
        for(.so in .stat){
          .out_name <- as.character(.so$name)[1]
          .pat  <- normalize_path_lexical(as.character(get_value_from_object(.so))[1])
          .mine <- out_req_list[lab_vec == .out_name]
          if(!length(.mine)) next
          .req <- .mine[[1]]
          ## The pattern MUST be THIS output's: every entry of out_req_list is
          ## born with the dyn_wildcard the producer reports, which is the
          ## dynamic one. The dynamic branch does the same for its own outputs
          ## (mine[[.k]]$dyn_wildcard <- pat).
          .req$dyn_wildcard <- as.character(get_value_from_object(.so))[1]
          .req$output_sel <- .out_name
          for(.i in op_to_expand[!is.na(op_to_expand)]){
            for(.j in seq_along(workflow$task_list[[.i]]$par_list)){
              .par <- workflow$task_list[[.i]]$par_list[[.j]]
              if(!inherits(.par$value, "dep_object")) next
              if(!identical(.par$value$task_id, .prod$task_id)) next
              .di <- normalize_path_lexical(as.character(.par$value$value))
              if(length(.di) == 1L && identical(.di, .pat))
                workflow$task_list[[.i]]$par_list[[.j]]$value <- as_output_req_list(.req)
            }
          }
        }
      }
    }

    ## A static gather over the STATIC outputs of this dynamic producer: the
    ## constituents of this instance are bound now, as in the static branch.
    if(!is.null(exe_id_to_idx)){
      .pi_g <- exe_id_to_idx[res_notify$exe_id]
      if(!is.na(.pi_g))
        workflow <- bind_static_gather_constituents(workflow, op_to_expand,
                                                    workflow$task_list[[as.integer(.pi_g)]],
                                                    res_notify, socket_worker, uses_tcp_worker)
    }

    ## Before re-expansion, not after: prepare_workflow_execution rebuilds the
    ## task list and the shapes with fresh ids, so the indices in op_to_expand
    ## no longer name the instances whose parameters were just filled.
    assert_fills_authorised_by_plan(workflow, op_to_expand)

    work_exe <- prepare_workflow_execution(workflow$task_list, workflow$DAG, op_to_expand, w_list, workflow$required_shapes,
                                           cumulative_replace_op = cumulative_replace_op)

    workflow$task_list <- work_exe$task_list
    workflow$required_shapes <- work_exe$required_shapes

    return(list(
      workflow = workflow,
      replace_op = work_exe$replace_op
    ))

  }
}



## The task table on disk, appended rather than rewritten.
##
## Re-serialising it whole on every dispatch and every result would copy the
## unchanged rows to disk to say what they already said; the rows that changed
## since the last write are one or two.
##
## So: one line per changed row, appended. An instance appears once per state it
## passed through, and flumen_status() keeps its last line. The file is TSV, so a
## run can be watched with `tail -f` from a terminal instead of an R session, and
## a reader that arrives mid-append sees a possibly truncated final line, which
## it drops rather than parsing into a row of NAs.
##
## `full = TRUE` truncates and writes everything. Used at the start, and after an
## expansion: expansion REPLACES rows, so an id can disappear from the table, and
## an appended file has no way to say "this one is gone".
FLUMEN_TASK_TABLE_FILE <- "central_scheduler_task_table_log.tsv"
FLUMEN_DIRTY_KEY       <- ".flumen_dirty"

## Mark one instance as needing to be written out. Kept in the status env
## because that is the one object every mutation site already holds: the
## dispatch path has no access to the scheduler state, only to this. The key
## cannot collide with an exe_id, which is a UUID.
mark_task_row_dirty <- function(env, exe_id){
  if(is.null(env)) return(invisible(NULL))
  cur <- env[[FLUMEN_DIRTY_KEY]]
  env[[FLUMEN_DIRTY_KEY]] <- c(cur, exe_id)
  invisible(NULL)
}

persist_task_table <- function(state, full = FALSE){
  tt <- state$task_table
  if(is.null(tt) || !nrow(tt)) return(invisible(FALSE))
  cols <- intersect(c("Exe_ID", "Op_ID", "Level", "Worker", "Status"), names(tt))
  env  <- state$task_status_env
  path <- FLUMEN_TASK_TABLE_FILE

  dirty <- if(is.null(env)) NULL else env[[FLUMEN_DIRTY_KEY]]
  if(!full && !file.exists(path)) full <- TRUE
  rows <- if(full) seq_len(nrow(tt)) else match(unique(dirty), as.character(tt$Exe_ID))
  rows <- rows[!is.na(rows)]

  if(!is.null(env)) env[[FLUMEN_DIRTY_KEY]] <- NULL
  if(!full && !length(rows)) return(invisible(FALSE))

  ## Sanitise each FIELD before joining: doing it after would strip the very
  ## separators just inserted. A missing value is written as an EMPTY field, not
  ## as the text "NA", which flumen_status() would read back as a string.
  block <- tt[rows, cols, drop = FALSE]
  txt   <- do.call(paste, c(lapply(block, function(x){
                              v <- gsub("[\r\n\t]", " ", as.character(x))
                              v[is.na(x)] <- ""
                              v
                            }), sep = "\t"))
  con   <- file(path, open = if(full) "wt" else "at")
  on.exit(close(con), add = TRUE)
  if(full) writeLines(paste(cols, collapse = "\t"), con)
  writeLines(txt, con)
  invisible(TRUE)
}
