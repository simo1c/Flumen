## Dependency resolution for the supervisor: given a wrapper waiting on an input that is
## another op's output, materialise that output into the wrapper's par_list -- either from
## the local result cache (producer ran on this worker) or via a peer-to-peer transfer
## (producer on another worker). Includes the P2P-transfer completion path: merge the
## received chunks, notify the scheduler of received files, fan out the resolved param to
## all coalesced waiters, ACK the producer, and free the recv-pool port. Used by the
## supervisor via R/supervisor_handlers.R (sup_handle_wrapper_ready /
## sup_handle_parameter_requested).

## The delivered FORM of a parameter, as the scheduler declared it from the
## user's declaration ("aggregate", "list", "bare"; R/scheduler_dispatch.R,
## params_form, uuid-keyed). NA when the message carries none, which a
## version-matched deployment cannot produce: callers treat NA as an error,
## never as a licence to infer the form from the data.
declared_form <- function(exe_object, w_param){
  u <- exe_object$par_uuids[w_param]
  f <- exe_object$params_form
  if(is.na(u) || is.null(f) || !(u %in% names(f))) return(NA_character_)
  as.character(f[[u]])[1]
}

## What the container still WAITS for: every output_req left in a dependency
## parameter, as (parameter, producer) pairs. Empty when every constituent has
## been delivered. This is the fact "is the wrapper's input complete"; the
## rows of wrapper_status are a separate bookkeeping of the same fact, and a
## wrapper is launched only when the two agree (see the two launch sites).
container_pending <- function(exe_object){
  out <- list()
  for(nm in names(exe_object$par_list)){
    v <- exe_object$par_list[[nm]]
    if(inherits(v, "output_req")){
      out[[length(out) + 1L]] <- c(par = nm, exe_id = as.character(v$exe_id)[1])
    } else if(inherits(v, "output_req_list")){
      for(.it in v) if(inherits(.it, "output_req"))
        out[[length(out) + 1L]] <- c(par = nm, exe_id = as.character(.it$exe_id)[1])
    }
  }
  out
}

## Pure: for every dependency parameter, compare the constituents the
## container holds (one for an output_req, the length of an output_req_list)
## with params_n_constituents, uuid-keyed and aligned with par_names through
## par_uuids. Returns one message per disagreement, none when all agree.
## Parameters the message declares no count for are not judged.
constituent_count_mismatch <- function(exe_object){
  decl <- exe_object$params_n_constituents
  ## A message without the declared counts comes from a scheduler of another
  ## build, and is reported as such rather than accepted unchecked.
  if(is.null(decl))
    return(sprintf(paste("the dispatch message declares no constituent count",
                         "(params_n_constituents): the scheduler and this worker come from",
                         "different builds of flumen")))
  out <- character(0)
  for(i in seq_along(exe_object$par_names)){
    nm <- exe_object$par_names[i]; u <- exe_object$par_uuids[i]
    if(is.na(u) || !(u %in% names(decl))) next
    d <- as.integer(decl[[u]])
    if(is.na(d) || d < 1L) next
    ## The container is keyed by the slot name, which is the human name on
    ## one build path and the uuid on another (exe_object_from_task,
    ## name_source; shell tasks arrive uuid-keyed): look up by either.
    v <- exe_object$par_list[[nm]]
    if(is.null(v)) v <- exe_object$par_list[[u]]
    n <- if(inherits(v, "output_req_list")) sum(vapply(v, inherits, logical(1), "output_req"))
         else if(inherits(v, "output_req")) 1L else 0L
    if(n != d) out <- c(out, sprintf("parameter '%s' declared %d constituent(s), the container holds %d", nm, d, n))
  }
  out
}

pending_message <- function(exe_object, wrapper_id, pending){
  sprintf(paste("task '%s' (wrapper %s): every dependency row reads ready, but the input still",
                "waits for %d constituent(s): %s. The wrapper is not launched on a partial input."),
          if(!is.null(exe_object$task_id)) exe_object$task_id else wrapper_id, wrapper_id,
          length(pending),
          paste(vapply(pending, function(p) paste0(p[["par"]], " <- ", p[["exe_id"]]), character(1)),
                collapse = ", "))
}

update_co_local_dependency <- function(reg_tasks,
                                       msg_sender,
                                       param_name,
                                       exe_id_to_req,
                                       wrapper_status,
                                       result_cache){

  exe_object <- reg_tasks[[msg_sender]]$exe_object
  w_param <- which(names(exe_object$par_list) == param_name)
  
  actual_param <- exe_object$par_list[[w_param]]
  ## The template is a field of THIS constituent, read by the producer's
  ## identity: in a gather every constituent carries the pattern of its own
  ## producer instance ("parts/s1_r1_{{n}}.txt" next to "parts/s1_r2_{{n}}.txt"),
  ## so the template of a sibling matches none of this producer's files.
  .own <- output_req_for(actual_param, exe_id_to_req)
  if(is.null(.own)){
    return(list(reg_tasks = reg_tasks, wrapper_status = wrapper_status,
                error = sprintf(paste(
                  "parameter '%s' of wrapper '%s' has no constituent from producer '%s',",
                  "yet a row of wrapper_status waits for it: the container and the rows",
                  "disagree, which is an engine invariant, not a workflow error"),
                  param_name, msg_sender, exe_id_to_req)))
  }
  dyn_wild <- .own$dyn_wildcard
  ## The result instance asked for is a field of the constituent as well
  ## (NULL means the whole result).
  instance <- .own$output_instance

  results <- cache_get(result_cache, exe_id_to_req)
  result_instance <- get_result_instance(results, instance)

  parameter_values      <- result_instance$result
  parameter_values_type <- result_instance$result_type
  result_is_arrow       <- result_instance$result_is_arrow
  result_elem_names     <- result_instance$result_elem_names

  ## Per-sample filter, co-local path: producer ran on this same worker (no
  ## transfer), but the consumer must still see ONLY its selected files.
  co_sel <- output_req_selectors(actual_param, exe_id_to_req)
  if((sel_active(co_sel$output_sel) || sel_active(co_sel$output_path_sel) ||
      sel_active(co_sel$file_sel)) && is.list(parameter_values) &&
     length(parameter_values) == length(parameter_values_type)){
    .cf <- p2p_filter_param(parameter_values, parameter_values_type, result_is_arrow,
                            result_elem_names, result_instance$result_group_names,
                            co_sel$output_sel, co_sel$output_path_sel, co_sel$file_sel)
    parameter_values      <- .cf$param_req
    parameter_values_type <- .cf$data_type
    result_is_arrow       <- .cf$is_arrow
    result_elem_names     <- .cf$elem_names
  }


  ## The container's form is DECLARED (params_form): "aggregate" and "list"
  ## are delivered as a list, "bare" as one value. Never inferred from how many
  ## results the cache holds, which answers a different question.
  .form <- declared_form(exe_object, w_param)
  if(is.na(.form)){
    return(list(reg_tasks = reg_tasks, wrapper_status = wrapper_status,
                error = sprintf("parameter '%s' of wrapper '%s': the dispatch message declares no form (params_form) for it",
                                param_name, msg_sender)))
  }
  is_multi <- .form %in% c("aggregate", "list")
  ## "wrapped:X" is the marker meaning "comes from user list-return" -- the list is
  ## preserved end to end.
  is_from_list <- startsWith(parameter_values_type[1], "wrapped")
  need_deserialize <- is_multi || is_from_list || parameter_values_type[1] %in% c("file", "file_path")

  if(!is_multi && !is_from_list && !parameter_values_type[1] %in% c("file", "file_path")){
    parameter_values <- parameter_values[[1]]
  }
  
  if(parameter_values_type[1] %in% c("file", "file_path")){
    n_in <- length(parameter_values)
    res <- resolve_file_param_paths(parameter_values, dyn_wild,
                                    exe_object$type, param_name,
                                    shell_prefix = isTRUE(.own$shell_prefix),
                                    prefix       = .own$prefix)
    ## WHICH files this constituent receives is DECLARED: the producer labels
    ## every result with its output, and the selectors (output_sel,
    ## output_path_sel, file_sel) narrow it above, exactly as on the P2P path,
    ## where no template is ever consulted. The template match here is
    ## therefore a CHECK, not a filter: every file the selectors let through
    ## must match the constituent's own template, and a file that does not is
    ## a selector the scheduler failed to declare or a template that belongs
    ## to another producer. The shell PREFIX branch replaces the paths by one
    ## string and is not a match at all.
    ## Reported, not stop()ped: a stop inside a handler takes the supervisor
    ## down (inst/scripts/supervisor.R).
    n_out <- length(res$parameter_values)
    if(!isTRUE(.own$shell_prefix) && n_in > 0L && n_out < n_in){
      .files <- tryCatch(as.character(unlist(lapply(parameter_values, function(v)
                  if(is.raw(v)) qs2::qs_deserialize(v) else v))), error = function(e) "<unreadable>")
      .kept  <- tryCatch(as.character(unlist(lapply(res$parameter_values, function(v)
                  if(is.raw(v)) qs2::qs_deserialize(v) else v))), error = function(e) character(0))
      return(list(reg_tasks = reg_tasks, wrapper_status = wrapper_status,
                  error = sprintf(paste(
                    "parameter '%s' of task '%s': the template '%s' declared for producer '%s'",
                    "matches %d of the %d file(s) the selectors let through (%s).",
                    "A constituent is delivered whole or refused: the files it would have dropped are %s."),
                    param_name, if(!is.null(exe_object$task_id)) exe_object$task_id else msg_sender,
                    paste(dyn_wild, collapse = "|"), exe_id_to_req, n_out, n_in,
                    paste(.files, collapse = ", "),
                    paste(setdiff(.files, .kept), collapse = ", "))))
    }
    note <- NULL
    parameter_values      <- res$parameter_values
    parameter_values_type <- res$parameter_values_type
    exe_object$file_transfer[[param_name]] <- c(
      exe_object$file_transfer[[param_name]],
      res$file_transfer_addition
    )
    ## All branches above rebuild parameter_values_type with a new length.
    ## Keep result_is_arrow aligned so params_element_types/is_arrow stay
    ## consistent. File results are never Arrow (get_result_from_msg forces
    ## is_arrow=FALSE).
    result_is_arrow <- rep(FALSE, length(parameter_values_type))
  }


  par_list <- update_param_wildcard_with_value(
    exe_object$par_list,
    parameter_values,
    w_param,
    is_arrow   = result_is_arrow,
    data_type  = parameter_values_type,
    elem_names = result_elem_names,
    exe_id     = exe_id_to_req
  )

  names(par_list) <- names(exe_object$par_list)


  # For multi-output, keep "wrapped" as params_type sentinel so that
  # expand_wrapped_params expands par_list correctly during preparation.
  params_type_val <- if(is_multi) "wrapped" else parameter_values_type[1]

  params_type <- update_param_names_with_value(
    exe_object$params_type,
    params_type_val,
    w_param
  )

  ## The codec of every element travels on the element itself (pinned in
  ## update_param_wildcard_with_value and read back at prepare time); the
  ## per-parameter flag is the default for a container, never a codec.
  exe_object$params_is_arrow[w_param] <- FALSE

  par_names <- exe_object$par_names
  par_uuids <- exe_object$par_uuids

  
  exe_object$par_list      <- par_list
  exe_object$params_type   <- params_type
  exe_object$par_names     <- par_names
  exe_object$par_uuids     <- par_uuids
  w_param <- which(exe_object$par_uuids == param_name)
  exe_object$params_type[w_param][exe_object$params_type[w_param] == "file"] <- "file_path"

  reg_tasks[[msg_sender]]$exe_object <- exe_object
  
    wrapper_status[!is.na(wrapper_status$Param_dependency_wrapper_ID) &
                     wrapper_status$Param_dependency_wrapper_ID == exe_id_to_req & 
                     wrapper_status$Status != "finished" &
                     wrapper_status$Wrapper_ID == msg_sender, "Param_dependency"] <- NA
    
    wrapper_status[!is.na(wrapper_status$Param_dependency_wrapper_ID) & 
                     wrapper_status$Param_dependency_wrapper_ID == exe_id_to_req  & 
                     wrapper_status$Status != "finished" &
                     wrapper_status$Wrapper_ID == msg_sender, "Status"] <- "ready"

  return(
    list(
      reg_tasks = reg_tasks,
      wrapper_status = wrapper_status,
      note = if(exists("note", inherits = FALSE)) note else NULL
    )
  )
}

## Apply a resolved P2P parameter to a single waiter (consumer wrapper). Kept
## out of the `parameter_requested` handler of supervisor.R so the same logic can
## be invoked once per waiter when the P2P response arrives, and also from a
## cache hit in the `ready` handler when an earlier P2P transfer already brought
## the file on disk.
##
## reg_tasks / wrapper_status are supervisor state, passed by value and returned.
## wrapper_id / par_name / exe_id_req identify which wrapper and dep to update.
## par_req is the payload, a list of raw blobs or R objects per parameter
## instance, with par_req_types, par_req_is_arrow and par_req_elem_names holding
## the per-instance dtype, arrow flag and element names. is_from_list_cross says
## the payload comes from a producer's list return, which complete_p2p_transfer
## derives from the prefix of par_req_types[1]. received_paths is consumed only
## when data_type collapses to "file_path", and ignored otherwise.
##
## Returns list(reg_tasks, wrapper_status). Touches no socket, no file and no
## log: it writes to in-memory state only.
apply_p2p_resolution <- function(reg_tasks, wrapper_status,
                                  wrapper_id, par_name, exe_id_req,
                                  par_req, par_req_types, par_req_is_arrow,
                                  is_from_list_cross,
                                  received_paths,
                                  par_req_elem_names = NULL){

  ## The container's form is DECLARED (params_form), as on the co-local path:
  ## "aggregate" and "list" take the "wrapped" sentinel, so that
  ## expand_wrapped_params expands the container into one row per element at
  ## prepare_code_and_parameters time; "bare" takes the element's own type.
  ## A payload the producer itself labelled as a list return ("wrapped:X") stays
  ## a list: that label is the producer's data.
  w_param <- which(names(reg_tasks[[wrapper_id]]$exe_object$par_list) == par_name)
  actual_param_curr <- reg_tasks[[wrapper_id]]$exe_object$par_list[[w_param]]
  .form <- declared_form(reg_tasks[[wrapper_id]]$exe_object, w_param)
  if(is.na(.form)){
    return(list(reg_tasks = reg_tasks, wrapper_status = wrapper_status,
                error = sprintf("parameter '%s' of wrapper '%s': the dispatch message declares no form (params_form) for it",
                                par_name, wrapper_id)))
  }

  ## The bare "wrapped" sentinel means "a list whose elements each carry their
  ## own type label": update_param_wildcard_with_value pins that label on
  ## every element, and prepare reads it back. The container is a list of
  ## constituents in every case, so the sentinel follows the declared form
  ## alone. Collapse "file" to "file_path" because the bytes are already on
  ## disk by the time we apply the resolution.
  data_type <- if(.form %in% c("aggregate", "list") || is_from_list_cross) "wrapped"
               else par_req_types[1]
  if(data_type == "file"){
    data_type <- "file_path"
  }

  w <- w_param
  ## Whether a shell consumer takes the backtick PREFIX instead of the paths
  ## is DECLARED on the constituent (shell_prefix, prefix), read by producer
  ## identity. Never inferred from backticks in the template, which is the
  ## producer's pattern and does not carry the consumer's backticks.
  .own_req <- output_req_for(actual_param_curr, exe_id_req)
  backtick_remote <- !is.null(.own_req) && isTRUE(.own_req$shell_prefix)
  suffix_curr <- if(backtick_remote) list(values = as.character(.own_req$prefix)) else NULL

  if(data_type == "file_path"){
    reg_tasks[[wrapper_id]]$exe_object$file_transfer[[par_name]] <- c(
      reg_tasks[[wrapper_id]]$exe_object$file_transfer[[par_name]],
      as.list(received_paths)
    )
  }

  ## Materialise file-path values (qs2 -> native) so the standard serialize_data
  ## flow at dispatch applies the consumer's codec: Arrow (from_R_to_arrow) for a
  ## Python wrapper, qs2 for R/shell. File outputs ship qs2-serialised from the
  ## producer (wrapper.R forces to_arrow=FALSE for file tasks), so par_req
  ## still holds raw qs2 bytes here.
  ##
  ## Applies to BOTH a single file_path dep AND an AGGREGATED file dep: the
  ## latter collapses to data_type "wrapped" (output_req_list) above, so it can't
  ## be gated on data_type -- its par_req constituents are nonetheless qs2 file
  ## paths (per par_req_types). Decode only FILE-typed constituents so object
  ## aggregations are left untouched for the wrapper. par_req and par_req_types
  ## are built in lockstep (complete_p2p_transfer).
  .is_file_ty <- function(t) !is.na(t) && t %in% c("file", "file_path")
  if(is.raw(par_req)){
    if(length(par_req_types) >= 1L && .is_file_ty(par_req_types[1]))
      par_req <- qs2::qs_deserialize(par_req)
  }else if(is.list(par_req)){
    par_req <- lapply(seq_along(par_req), function(i){
      x  <- par_req[[i]]
      ty <- if(i <= length(par_req_types)) par_req_types[i] else NA_character_
      if(is.raw(x) && .is_file_ty(ty)) qs2::qs_deserialize(x) else x
    })
  }

  ## Backtick prefix -> directory-string param ONLY for shell consumers (the
  ## {name} placeholder becomes the static dir prefix). A non-shell (R/Python)
  ## consumer must receive the resolved file PATHS, even when the producer's
  ## output pattern carries backticks (which get inherited into dyn_wildcard
  ## during dep matching). Mirrors the co-local branch of
  ## resolve_file_param_paths, which reads the same field. The prefix enters the
  ## container through the same door as any value: the constituent of THIS
  ## producer, so the container stays a list of constituents.
  if(backtick_remote){
    par_req          <- as.list(suffix_curr$values)
    par_req_types    <- rep("character", length(par_req))
    par_req_is_arrow <- rep(FALSE, length(par_req))
    par_req_elem_names <- NULL
    data_type <- "character"
  }
  par_list <- update_param_wildcard_with_value(
    reg_tasks[[wrapper_id]]$exe_object$par_list,
    par_req,
    w_param,
    is_arrow   = par_req_is_arrow,
    data_type  = par_req_types,
    elem_names = par_req_elem_names,
    exe_id     = exe_id_req
  )

  reg_tasks[[wrapper_id]]$exe_object$par_list <- par_list
  reg_tasks[[wrapper_id]]$exe_object$params_type[[w]] <- data_type
  ## The per-parameter flag is the default for a container, never a codec: the
  ## codec travels on each element, as on the co-local path above.
  reg_tasks[[wrapper_id]]$exe_object$params_is_arrow[[w]] <- FALSE

  ## Clearing Param_dependency is what marks the row RESOLVED, as
  ## update_co_local_dependency() does on the co-local path. A row that keeps its
  ## parameter uuid is pushed back to "wait_parameter" by a LATER constituent of
  ## the same aggregated dependency issuing its own request.
  .sel <- wrapper_status$Param_dependency_wrapper_ID == exe_id_req &
            !is.na(wrapper_status$Param_dependency) &
            wrapper_status$Param_dependency == par_name &
            wrapper_status$Wrapper_ID == wrapper_id
  wrapper_status[.sel, "Param_dependency"] <- NA
  wrapper_status[.sel, "Status"]           <- "ready"

  list(reg_tasks = reg_tasks, wrapper_status = wrapper_status)
}

get_result_from_msg <- function(msg){

  results <- list()

  for(i in seq_along(msg)){

    if(msg[[i]]$msg_type == "result"){

      .ty0 <- sub("^wrapped:", "", msg[[i]]$data_type)
      if(.ty0 %in% c("file", "file_path")){

        ## Match the file type AFTER stripping the "wrapped:" marker, and pick the
        ## decoder from the is_arrow flag rather than from the type name: the R
        ## wrapper sends file paths as qs2 bytes, the Python wrapper Arrow-encodes
        ## them and tags them "wrapped:file_path".
        res          <- decode_message_data(msg[[i]]$data, .ty0, isTRUE(msg[[i]]$is_arrow))
        data_type    <- "file"
        is_arrow_flag <- FALSE

      } else if(isTRUE(msg[[i]]$is_arrow)){

        # Keep Arrow bytes directly in cache (do not convert to qs2)
        res          <- raw_payload(msg[[i]]$data)
        data_type    <- msg[[i]]$data_type
        is_arrow_flag <- TRUE

      } else {

        res          <- raw_payload(msg[[i]]$data)
        data_type    <- msg[[i]]$data_type
        is_arrow_flag <- FALSE

      }

      res_el <- list(
        result    = res,
        data_type = data_type,
        is_arrow  = is_arrow_flag,
        ## The producer's own name for THIS element (the names of a returned
        ## named list / dict), reattached downstream as list names or dict keys.
        elem_name = msg[[i]]$elem_name,
        ## Which declared OUTPUT this result belongs to. Distinct from elem_name
        ## in ROLE: elem_name is the producer's per-element name, reattached
        ## downstream as a list name / dict key, while this names the output the
        ## element came from and is what a consumer selects by.
        ## elem_name must NOT feed this: for a DYNAMIC object output the names
        ## of the returned list are the WILDCARD VALUES (list(a=,b=,c=) -> three
        ## instances a/b/c), not the declared output's name. Empty when the
        ## producer states none.
        output_par_name = msg[[i]]$data_names
      )

      results <- c(results, list(res_el))

    }
  }
  return(
    new_result(results)
  )
}

## Resolve the per-instance paths to inject into a co-local file/file_path
## parameter. Three cases, in the order the body decides them: a shell consumer
## taking the backtick prefix, a template with no dynamic wildcard to match on,
## and the match of the delivered paths against the template. Returns a list
## with parameter_values, parameter_values_type, and file_transfer_addition
## (paths to append to exe_object$file_transfer).
## Pure helper of update_co_local_dependency: no side effects.
resolve_file_param_paths <- function(parameter_values, dyn_wild,
                                      exe_type, param_name,
                                      shell_prefix = FALSE, prefix = NULL) {
  ## Shell consumer taking the backtick PREFIX, as DECLARED by the scheduler on
  ## the constituent: the prefix substitutes into the command line (e.g.
  ## bowtie2-build --output-prefix idx/X) while the per-instance files travel
  ## via file_transfer. First, because the prefix replaces the paths outright
  ## and there is nothing to match.
  if (isTRUE(shell_prefix)) {
    pv <- as.list(as.character(prefix))
    return(list(
      parameter_values       = pv,
      parameter_values_type  = named_type_vector("character", length(pv), param_name),
      file_transfer_addition = as.list(unlist(parameter_values))
    ))
  }

  dyn_wild_indexes <- find_dynamic_wildcards(dyn_wild)
  dyn_wild <- dyn_wild[dyn_wild_indexes]

  if (length(dyn_wild) == 0) {
    ## Pure wildcard (no file path template): pass all result files directly.
    pv <- as.list(unlist(parameter_values))
    return(list(
      parameter_values       = pv,
      parameter_values_type  = named_type_vector("file_path", length(pv), param_name),
      file_transfer_addition = pv
    ))
  }

  ## R/python consumers, and shell consumers without a prefix, receive the
  ## actual paths: match the result file list against the template. Any
  ## backtick a template might still carry is a marker, not path text.
  dyn_wild_for_match <- strip_backticks(dyn_wild)

  ## parameter_values may be raw blobs (serialized) - deserialize to strings for matching
  parameter_values_str <- lapply(parameter_values, function(v) {
    if (is.raw(v)) qs2::qs_deserialize(v) else v
  })

  parameter_values_matched <- as.list(unlist(lapply(dyn_wild_for_match, function(param) {
    extract_multiple_wildcard_values(param, unlist(parameter_values_str))$matches
  })))

  w_matched <- which(unlist(parameter_values_str) %in% unlist(parameter_values_matched))

  list(
    parameter_values       = parameter_values[w_matched],   ## keep raw blobs for update_param_wildcard_with_value
    parameter_values_type  = named_type_vector("file_path", length(w_matched), param_name),
    file_transfer_addition = as.list(unlist(parameter_values_matched))
  )
}

## Handle a fully-received `parameter_requested` message on a consumer
## supervisor: merge the chunks, notify the scheduler of any received files,
## resolve ALL coalesced waiters from this single P2P transfer (fan-out), ACK
## the producer (`p2p_transfer_done` -> it closes its per-transfer PUSH), free
## the recv pool port, and close the recv/request sockets. `pending_resolutions_env`
## is an env, mutated in place; the rest of the mutated state is returned.
complete_p2p_transfer <- function(par_req_chunked_message, msg_id,
                                  socket_list, message_list,
                                  reg_tasks, wrapper_status, pending_resolutions_env,
                                  p2p_recv_dial, p2p_busy_ports,
                                  recv_socket, msg_sender, worker_name, chunk_size,
                                  result_cache = NULL){

  msg <- merge_all_chunks(par_req_chunked_message[[msg_id]])
  par_req_chunked_message[[msg_id]] <- NULL

  ## P2P file transfer just completed: notify scheduler so it can mark these
  ## files as completed for this worker. Only fires for parameter_requested
  ## entries that travelled with data_type="file"; "file_path" hops carry no bytes.
  received_files_p2p <- character(0)
  for(j in seq_along(msg)){
    if(isTRUE(msg[[j]]$msg_type == "parameter_requested") &&
       isTRUE(msg[[j]]$data_type == "file")){
      received_files_p2p <- c(received_files_p2p, msg[[j]]$file_name)
    }
  }
  if(length(received_files_p2p)){
    send_file_received_to_central_scheduler(socket_list[["sock_res"]], worker_name, received_files_p2p)
  }

  par_req            <- list()
  par_req_is_arrow   <- logical(0)
  par_req_types      <- character(0)
  par_req_elem_names <- character(0)

  for(i in seq_along(msg)){
    if(msg[[i]]$msg_type == "parameter_requested"){
      par_req            <- c(par_req, list(msg[[i]]$data))
      par_req_is_arrow   <- c(par_req_is_arrow, isTRUE(msg[[i]]$is_arrow))
      par_req_types      <- c(par_req_types, msg[[i]]$data_type)
      ## Per-element name travels in the envelope's elem_name field.
      dn_i <- msg[[i]]$elem_name
      par_req_elem_names <- c(par_req_elem_names, if(is.null(dn_i)) "" else as.character(dn_i))
    }else{
      decoded <- decode_message_data(msg[[i]]$data, msg[[i]]$data_type, msg[[i]]$is_arrow)
      if(msg[[i]]$msg_type == "parameter_requested_name"){
        par_name <- decoded
      }else if(msg[[i]]$msg_type == "parameter_requested_exe_id"){
        exe_id_req <- decoded
      }else{
        wrapper_id <- decoded
      }
    }
  }

  is_multi_cross <- length(par_req) > 1
  ## "wrapped:X" = "comes from user list-return" -- list preserved end-to-end
  ## even for length=1 (same logic as update_co_local_dependency).
  is_from_list_cross <- length(par_req_types) > 0 && startsWith(par_req_types[1], "wrapped")

  if(length(par_req) == 1 && !is_from_list_cross){
    par_req <- par_req[[1]]
  }

  ## Compute the per-message paths once: only file-typed parameter_requested
  ## entries carry on-disk paths.
  actual_files <- if(par_req_types[1] == "file"){
    unlist(lapply(
      msg[sapply(msg, function(m) m$msg_type == "parameter_requested")],
      function(m) m$file_name
    ))
  } else character(0)

  ## Coalesced fan-out: complete the pending P2P entry and retrieve ALL waiters
  ## that joined on this output_req. A single P2P transfer satisfies N concurrent
  ## consumer wrappers; without this loop only the first waiter would be unblocked.
  par_val_req_origin <- reg_tasks[[wrapper_id]]$exe_object$par_list[[par_name]]
  ## The instance asked for is a field of the constituent of THIS producer,
  ## read as the request read it (sup_handle_wrapper_ready).
  .own_origin <- output_req_for(par_val_req_origin, exe_id_req)
  out_instance_origin <- if(is.null(.own_origin)) NULL else .own_origin$output_instance
  ## Reconstruct the SAME key the request used, including this exe_id's three
  ## narrowings (else request and response keys diverge -> stuck waiters).
  is_list_origin <- inherits(par_val_req_origin, "output_req_list")
  sel_origin <- output_req_selectors(par_val_req_origin, exe_id_req)
  p2p_key <- p2p_make_key(exe_id_req, out_instance_origin, is_list_origin,
                          sel_origin$output_sel, sel_origin$output_path_sel,
                          sel_origin$file_sel)

  ## Serve-from-received: re-cache the just-received output under a composite
  ## "exe_id#out_instance" key so THIS worker -- which only RECEIVED the data,
  ## did not produce it -- can later serve it to a peer. Here we only make it
  ## servable, the scheduler's holder index and fetch routing consume it. Only a
  ## FULL output is cached: a transfer narrowed by ANY of the three selectors is
  ## a PARTIAL copy, skipped to avoid advertising an incomplete holder.
  ## Reconstruction mirrors get_result_from_msg.
  if(!is.null(result_cache)){
    is_full <- !sel_active(sel_origin$output_sel) &&
               !sel_active(sel_origin$output_path_sel) &&
               !sel_active(sel_origin$file_sel)
    ## Re-cache + holder advertise: ONLY for a FULL copy. A per-sample-filtered
    ## (partial) copy must not be cached under the whole-output holder_key nor
    ## advertised.
    if(is_full){
      recv_results <- list()
      for(i in seq_along(msg)){
        if(isTRUE(msg[[i]]$msg_type == "parameter_requested")){
          dt <- msg[[i]]$data_type
          dt0 <- sub("^wrapped:", "", dt)
          if(dt0 %in% c("file", "file_path")){
            res <- decode_message_data(msg[[i]]$data, dt0, isTRUE(msg[[i]]$is_arrow)); dt <- "file"; ia <- FALSE
          } else {
            res <- raw_payload(msg[[i]]$data); ia <- isTRUE(msg[[i]]$is_arrow)
          }
          recv_results <- c(recv_results, list(list(
            result = res, data_type = dt, is_arrow = ia, elem_name = msg[[i]]$elem_name,
            output_par_name = msg[[i]]$data_names)))
        }
      }
      if(length(recv_results)){
        holder_key <- paste0(exe_id_req, "#",
                             if(is.null(out_instance_origin)) "" else out_instance_origin)
        try(cache_set(result_cache, holder_key, new_result(recv_results)), silent = TRUE)
      }
    }
    ## Serve-release: fire for EVERY completed serve (full OR partial) so the
    ## scheduler decrements serving_count at the source. is_full tells it whether
    ## to ALSO record this worker in the holder index, decoupling "-1 the serve"
    ## from "mark_holder".
    try(send_data_received_to_central_scheduler(
          socket_list[["sock_res"]], worker_name, exe_id_req, out_instance_origin, is_full),
        silent = TRUE)
  }

  waiters <- p2p_complete_request(
    pending_resolutions_env, p2p_key,
    paths              = actual_files,
    par_req            = par_req,
    par_req_types      = par_req_types,
    par_req_is_arrow   = par_req_is_arrow,
    par_req_elem_names = par_req_elem_names,
    is_multi_cross     = is_multi_cross,
    is_from_list_cross = is_from_list_cross
  )
  ## Defensive: empty waiters (response without prior registration) -> fall back
  ## to the wrapper the producer addressed. Should not happen in correct usage.
  if(length(waiters) == 0L){
    waiters <- list(list(wrapper_id = wrapper_id, par_name = par_name))
  }
  refused <- list()
  for(w in waiters){
    ## Skip waiters whose wrapper crashed mid-wait.
    if(is.null(reg_tasks[[w$wrapper_id]])){
      to_log(paste0("[P2P-WARN] waiter ", w$wrapper_id,
                    " no longer in reg_tasks (crashed?), skipping"), worker_name)
      next
    }
    .res <- apply_p2p_resolution(
      reg_tasks, wrapper_status,
      w$wrapper_id, w$par_name, exe_id_req,
      par_req, par_req_types, par_req_is_arrow,
      is_from_list_cross, actual_files,
      par_req_elem_names = par_req_elem_names
    )
    if(!is.null(.res$error)){
      ## No state here to notify from: handed back to the caller, which
      ## reports it to the scheduler and leaves the waiter unlaunched.
      refused[[length(refused) + 1L]] <- list(wrapper_id = w$wrapper_id, message = .res$error)
      to_log(paste0("DELIVERY REFUSED: ", .res$error), worker_name)
      next
    }
    reg_tasks      <- .res$reg_tasks
    wrapper_status <- .res$wrapper_status
    ## Launch when the rows say ready AND the container agrees (no output_req
    ## left waiting): the rows are a bookkeeping of the same fact, and the two
    ## are required to agree before a wrapper receives its input.
    if(all(wrapper_status[wrapper_status$Wrapper_ID == w$wrapper_id, "Status"] == "ready")){
      .pend <- container_pending(reg_tasks[[w$wrapper_id]]$exe_object)
      if(length(.pend)){
        .m <- pending_message(reg_tasks[[w$wrapper_id]]$exe_object, w$wrapper_id, .pend)
        refused[[length(refused) + 1L]] <- list(wrapper_id = w$wrapper_id, message = .m)
        to_log(paste0("LAUNCH REFUSED: ", .m), worker_name)
        next
      }
      message <- prepare_code_and_parameters_for_wrapper(reg_tasks[[w$wrapper_id]], worker_name, w$wrapper_id)
      send_message_by_chunks(worker_name, message, socket_list[[w$wrapper_id]],
                             chunk_size = chunk_size,
                             defer = list(channel = w$wrapper_id))
      wrapper_status[wrapper_status$Wrapper_ID == w$wrapper_id, "Status"] <- "execution"
      to_log(paste0("Code and parameters sent to ", w$wrapper_id), worker_name)
    }
  }

  ## ACK the producer (on the still-open request PUSH, socket #2) so it closes
  ## the per-transfer PUSH it opened toward us; then close our recv socket (#1)
  ## and request socket (#2), and free the recv pool port. The ACK is tiny ->
  ## send_aio + immediate close reliably flushes it.
  ack_dial      <- p2p_recv_dial[[recv_socket]]
  send_req_name <- paste0("send_req_", par_name, "_", exe_id_req)
  if(!is.null(ack_dial) && send_req_name %in% names(socket_list)){
    ack_data <- serialize_data(list(ack_dial), "R_object", FALSE)
    ack_msg  <- new_message(sender = worker_name, msg_type = "p2p_transfer_done",
                            data_type = unlist(ack_data$data_type$data_struct),
                            data = ack_data$data, is_arrow = ack_data$data_type$is_arrow,
                            file_name = ack_data$file_name)
    flumen_send_aio(socket_list[[send_req_name]], ack_msg$message, mode = "raw")
    socket_list <- close_socket(socket_list, send_req_name)
    to_log(paste0("p2p_transfer_done sent to ", msg_sender, " for ", ack_dial), worker_name)
  }
  if(!is.null(ack_dial) && grepl("^tcp://", ack_dial)){
    p2p_busy_ports <- setdiff(p2p_busy_ports, as.integer(sub(".*:", "", ack_dial)))
  }
  p2p_recv_dial[[recv_socket]] <- NULL
  socket_list  <- close_socket(socket_list, recv_socket)
  message_list <- close_message(message_list, recv_socket)

  list(par_req_chunked_message = par_req_chunked_message,
       socket_list = socket_list, message_list = message_list,
       reg_tasks = reg_tasks, wrapper_status = wrapper_status,
       p2p_recv_dial = p2p_recv_dial, p2p_busy_ports = p2p_busy_ports,
       refused = refused)
}
