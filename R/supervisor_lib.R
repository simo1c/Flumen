## Fixed metadata fields the supervisor prepends to every code+parameters
## dispatch message (one row each in the envelope, in this exact order).
## Defined once so all wrapper/supervisor sites share the same offset.
FIXED_DISPATCH_FIELD_NAMES <- c("shell_interpreter", "user_code",
                                "output_files", "is_output_par",
                                "dyn_wildcards",
                                "output_auto_mkdir",
                                "output_par_names",
                                "output_object_par_names",
                                ## the FORM each parameter is delivered in
                                ## ("bare", "list", "aggregate"), one entry per
                                ## declared parameter in order of first
                                ## appearance among the slots, so a reader
                                ## builds the container instead of counting
                                ## same-named slots.
                                "params_form")
## New fields go at the END: every wrapper reads the earlier ones by position
## (msg[[2]]..msg[[9]] in R, merged[1]..merged[8] in Python), so appending
## leaves those indices untouched. inst/scripts/objects.py mirrors this vector.
N_FIXED_DISPATCH_FIELDS <- length(FIXED_DISPATCH_FIELD_NAMES)


## Return a named vector of `type` replicated `n` times, named after `name`.
## Used when rewriting parameter_values_type after file-type normalization.
named_type_vector <- function(type, n, name){
  out <- rep(type, n)
  names(out) <- rep(name, n)
  out
}

init_wrapper_status_table <- function(){
  
  wrapper_status <- data.frame(
    Wrapper_ID = NULL, 
    Op_ID = NULL, 
    Param_dependency = NULL,
    Param_dependency_wrapper_ID = NULL,
    Supervisor_dependency = NULL,
    Status = NULL
  )
  
  return(wrapper_status)
  
}

send_ready_to_central_scheduler <- function(socket,
                                            worker_name,
                                            n_concurrent_task,
                                            self_host = NULL){

  ## Field 4 carries this worker's flumen version so the scheduler can refuse a
  ## run whose workers do not match it (see check_worker_version). A worker too
  ## old to know about the field simply sends three: its ABSENCE is the signal.
  sender <- rep(worker_name, 4)
  msg_type <- c("ready", "n_concurrent_task", "self_host", "flumen_version")
  data_type <- rep("R_object", 4)
  to_arrow <- rep(FALSE, 4)
  data <- list(0, n_concurrent_task,
               if(is.null(self_host)) "" else as.character(self_host),
               flumen_version_string())
  data <- serialize_data(data, data_type, to_arrow)
  
  ready_message <- new_message(
    sender = sender,
    msg_type = msg_type,
    data_type = unlist(data$data_type$data_struct),
    data = data$data,
    is_arrow = data$data_type$is_arrow,
    file_name = data$file_name
  )
  
  flag <- flumen_send_aio(socket, ready_message$message, mode = "raw")

  to_log(paste0("READY message sent to central scheduler for ", n_concurrent_task, " concurrent tasks"), worker_name)
  return(flag)
}

## Notify the scheduler that one or more files finished a chunked transfer on
## this worker. The scheduler uses this to move files from `in_transit` to
## `completed` for this worker and unblock pending tasks that share them.
## Asynchronous fire-and-forget over sock_res. Safe to ignore on schedulers
## that do not yet handle the `file_received` msg_type.
send_file_received_to_central_scheduler <- function(socket,
                                                    worker_name,
                                                    file_paths){
  if(length(file_paths) == 0) return(invisible(NULL))

  data <- serialize_data(list(as.character(file_paths)), "R_object", FALSE)

  msg <- new_message(
    sender    = worker_name,
    msg_type  = "file_received",
    data_type = unlist(data$data_type$data_struct),
    data      = data$data,
    is_arrow  = data$data_type$is_arrow,
    file_name = data$file_name
  )

  flag <- flumen_send_aio(socket, msg$message, mode = "raw")
  to_log(paste0("file_received notification sent for ", length(file_paths),
                " file(s): ", paste(file_paths, collapse = ", ")), worker_name)
  return(flag)
}

## Holder visibility. Notify the scheduler that this worker now
## holds a COMPLETE copy of (exe_id, out_instance): it received the full output via
## P2P and re-cached it (complete_p2p_transfer), so it can serve that output to a
## peer. The scheduler records it in its holder index, which the fetch routing reads.
## out_instance travels as a string ("" = the whole output / aggregated request),
## matching the composite cache key used by the supervisor and the serve fallback.
## Asynchronous fire-and-forget over sock_res; safe to ignore on older schedulers.
## `is_full` splits the message's two jobs: the scheduler ALWAYS releases the
## serving_count for this completed serve, but records the sender as a holder ONLY
## when is_full=TRUE. A per-sample-filtered (partial) copy must not be advertised
## as a servable holder, yet its serve must still be released.
send_data_received_to_central_scheduler <- function(socket,
                                                    worker_name,
                                                    exe_id,
                                                    out_instance,
                                                    is_full = TRUE){
  out_instance_str <- if(is.null(out_instance)) "" else as.character(out_instance)
  data <- serialize_data(list(as.character(exe_id), out_instance_str, isTRUE(is_full)),
                         c("R_object", "R_object", "R_object"), c(FALSE, FALSE, FALSE))
  msg <- new_message(
    sender    = rep(worker_name, 3),
    msg_id    = rep("data_received", 3),
    msg_type  = rep("data_received", 3),
    data_type = unlist(data$data_type$data_struct),
    data      = data$data,
    is_arrow  = data$data_type$is_arrow,
    file_name = data$file_name
  )
  flumen_send_aio(socket, msg$message, mode = "raw")
}

register_task_to_execute <- function(registered_op = list(),
                                   task_id,
                                   pid,
                                   socket,
                                   exe_object){

  registered_op[[task_id]] <- list(task_id = task_id,
                                 pid = pid,
                                 socket = socket,
                                 exe_object = exe_object)
  return(registered_op)
}


get_wrapper_info <- function(task_type){

  switch (task_type,
          R = {
            ext <- ".R"
            wrapper_file <- "wrapper.R"
            use_arrow <- FALSE
          },
          shell = {
            ext <- ".R"
            wrapper_file <- "wrapper_shell.R"
            use_arrow <- FALSE
          },
          python = {
            ext <- ".py"
            wrapper_file <- "wrapper.py"
            use_arrow <- TRUE
          }
  )

  ## Resolve script path from installed package; fallback to CWD for dev mode.
  wrapper <- system.file("scripts", wrapper_file, package = "flumen")
  if(!nzchar(wrapper)) wrapper <- wrapper_file

  return(
    list(
      ext       = ext,
      wrapper   = wrapper,
      use_arrow = use_arrow
    )
  )

}

expand_wrapped_params <- function(my_list, param_type) {

  ## "wrapped:X" (list-return marker with per-element type) must be treated
  ## like "wrapped" for count/expansion: same semantic "value is a list of elements".
  is_wrapped_prefix <- startsWith(as.character(param_type), "wrapped:")
  is_wrapped_list <- (param_type %in% c("wrapped", "file", "file_path") | is_wrapped_prefix) & sapply(my_list, is.list)

  ## Composite producer + multi-element output: each producer instance
  ## contributes a sub-list (e.g., its own file paths via {{out}}, or its own
  ## multi-output objects). Without flattening, the consumer would receive a
  ## list-of-lists -- one sub-list per producer -- where a flat list of elements
  ## is expected, and the per-slot sub-list would later re-expand inside
  ## serialize_data, growing data_type beyond the outer envelope's column length
  ## and crashing arrow::arrow_table with "All columns must have the same length".
  ## Restricted to identical(class(x), "list") so data.frame (which inherits
  ## from list) and other structured types are preserved as single elements.
  for (i in which(is_wrapped_list)) {
    v <- my_list[[i]]
    if (length(v) > 0 &&
        all(vapply(v, function(x) identical(class(x), "list"), logical(1)))) {
      my_list[[i]] <- unlist(v, recursive = FALSE)
    }
  }

  count <- rep(1, length(my_list))
  names(count) <- names(my_list)
  count[is_wrapped_list] <- lengths(my_list[is_wrapped_list])

  par_list_nested <- lapply(seq_along(my_list), function(i) {
    if (is_wrapped_list[i]) {
      my_list[[i]]
    } else {
      list(my_list[[i]])
    }
  })

  if (!is.null(names(my_list))) {
    names(par_list_nested) <- names(my_list)
  }

  ## elem_names_vec: per-element names ACTUALLY set by the producer (via
  ## names() on a returned named list / dict), kept separately from
  ## names(par_list). Avoids using unlist's auto-generated mangled names
  ## ("xs1", "xs.first", etc.) which would falsely trigger downstream
  ## dict-grouping for tasks that never named their outputs.
  elem_names_vec <- character()
  for (i in seq_along(par_list_nested)) {
    inner <- par_list_nested[[i]]
    inner_n <- names(inner)
    if (is.null(inner_n)) inner_n <- rep("", length(inner))
    elem_names_vec <- c(elem_names_vec, inner_n)
  }

  par_list <- unlist(par_list_nested, recursive = FALSE)

  return(list(par_list = par_list, count = count,
              elem_names = elem_names_vec))
}

prepare_code_and_parameters_for_wrapper <- function(reg_tasks_wrapper,
                                                    worker_name,
                                                    msg_id){

  ## codec for this wrapper's interpreter: python ops use Arrow end-to-end, R/shell
  ## ops keep qs2. Sourced per-wrapper from its op type (not a shared global), so a
  ## mix of python + R/shell ops running concurrently on one worker stays correct.
  use_arrow <- isTRUE(get_wrapper_info(reg_tasks_wrapper$exe_object$type)$use_arrow)

  ## The container a parameter is delivered in is DECLARED (params_form,
  ## uuid-keyed, aligned with par_list by POSITION through par_uuids -- the
  ## resolvers drop the name of the slot they replace, so names are not
  ## reliable). A dependency
  ## parameter is a list of constituents, one per producer, each holding the
  ## elements that producer delivered; "aggregate" and "list" hand that list
  ## to the expansion below, which flattens it to one slot per element.
  ## "bare" is one constituent by contract (params_n_constituents) and is
  ## delivered as its one element; a producer's own list return ("wrapped:X",
  ## the producer's label, preserved end to end on every path) stays a list
  ## even at one element, as the single-object-wrap rule below expects.
  ## A dispatch message without params_form comes from a scheduler of another
  ## build, and the containers it would produce are not the declared ones, so
  ## the run stops here. Read once, for this block and for the form below.
  .uuids <- reg_tasks_wrapper$exe_object$par_uuids
  .form  <- reg_tasks_wrapper$exe_object$params_form
  if(is.null(.form) || !all(.uuids %in% names(.form))){
    stop(sprintf(paste("task '%s': the dispatch message declares no form for %s.",
                       "Fix: reinstall the same flumen build everywhere."),
                 reg_tasks_wrapper$exe_object$task_id,
                 if(is.null(.form)) "any parameter"
                 else paste(shQuote(setdiff(.uuids, names(.form))), collapse = ", ")),
         call. = FALSE)
  }
  .par_list_decl <- reg_tasks_wrapper$exe_object$par_list
  for(i in seq_along(.par_list_decl)){
    v <- .par_list_decl[[i]]
    if(identical(as.character(.form[[.uuids[i]]]), "bare") && inherits(v, "output_req_list")){
      elems <- unlist(unclass(v), recursive = FALSE)
      lab   <- if(length(elems)) attr(elems[[1]], "flumen_elem_type") else NULL
      from_list <- !is.null(lab) && startsWith(as.character(lab)[1], "wrapped:")
      .par_list_decl[[i]] <- if(length(elems) == 1L && !from_list) elems[[1]] else elems
    }
  }

  par_list_info <- expand_wrapped_params(
    .par_list_decl,
    reg_tasks_wrapper$exe_object$params_type
  )

  par_list      <- par_list_info$par_list
  wrapped_count <- par_list_info$count
  elem_names_from_producer <- par_list_info$elem_names
  params_type   <- rep(reg_tasks_wrapper$exe_object$params_type, wrapped_count)
  params_type[params_type == "file"] <- "file_path"
  params_is_arrow <- rep(reg_tasks_wrapper$exe_object$params_is_arrow, wrapped_count)
  n_params      <- length(par_list)

  # Parameters in cache can be:
  #   - raw Arrow bytes  (params_is_arrow[i] == TRUE)
  #   - raw qs2 bytes    (params_is_arrow[i] == FALSE)
  #   - native R objects (not raw)
  #
  # Strategy: raw bytes pass unchanged; the is_arrow flag tells the wrapper how
  # to decode them (TRUE -> from_arrow_binary_to_R, FALSE -> qs2::qs_deserialize).
  # Python op (use_arrow=TRUE): all Arrow, qs2 bytes get re-encoded.
  # R op (use_arrow=FALSE): Arrow bytes stay Arrow (is_arrow=TRUE propagated from
  #   params_is_arrow), qs2 bytes stay qs2 (is_arrow=FALSE). Eliminates the
  #   useless Arrow->R->qs2 round-trip in the supervisor.

  sender <- rep(worker_name, n_params + N_FIXED_DISPATCH_FIELDS)
  ## msg_type slot count after the leading "shell_interpreter" + "code" pair
  ## must match (N_FIXED_DISPATCH_FIELDS - 2) + n_params. Bump in lockstep
  ## with FIXED_DISPATCH_FIELD_NAMES additions.
  msg_type <- c("shell_interpreter", "code",
                rep("parameter", n_params + N_FIXED_DISPATCH_FIELDS - 2L))

  output_files     <- reg_tasks_wrapper$exe_object$output_files
  ## output_auto_mkdir: parallel logical vector to output_files, consumed by
  ## the wrapper to gate create_dir_structure(f) per-output. Built with
  ## output_files in R/object_func.R, in the same iteration, so the two cannot
  ## fall out of step; a task_object missing it, or carrying a vector of
  ## another length, comes from another build of flumen.
  output_auto_mkdir <- reg_tasks_wrapper$exe_object$output_auto_mkdir
  if(is.null(output_auto_mkdir) ||
     length(output_auto_mkdir) != length(output_files)){
    stop(sprintf(paste("task '%s': %s auto_mkdir flag(s) for %d declared output(s).",
                       "Fix: reinstall the same flumen build everywhere."),
                 reg_tasks_wrapper$exe_object$task_id,
                 if(is.null(output_auto_mkdir)) "no" else as.character(length(output_auto_mkdir)),
                 length(output_files)), call. = FALSE)
  }
  ## output_par_names: parallel vector to output_files naming the DECLARED OUTPUT
  ## each entry came from. Built alongside output_files in R/object_func.R, in
  ## the same iteration over par_list, so the two cannot fall out of step and
  ## the wrapper has nothing to infer.
  output_par_names <- reg_tasks_wrapper$exe_object$output_par_names
  ## output_object_par_names: the declared OBJECT outputs by name, in
  ## declaration order. Built in the same loop in R/object_func.R, and NOT
  ## parallel to output_files, so it is neither masked nor length-checked
  ## against it here.
  output_object_par_names <- reg_tasks_wrapper$exe_object$output_object_par_names
  is_output_par    <- reg_tasks_wrapper$exe_object$is_output_par
  dyn_wildcards    <- reg_tasks_wrapper$exe_object$dyn_wildcard
  par_names        <- rep(reg_tasks_wrapper$exe_object$par_names, wrapped_count)

  ## A single new_object dep resolves to a 1-constituent output_req_list, which
  ## the resolver marks "wrapped" exactly like a 1-element aggregation. The
  ## marker is stripped for a single-element NON-aggregation param and delivered
  ## bare; genuine gathers (params_is_aggr) keep the list shape even at length 1.
  ##
  ## A slot that arrived as already-encoded bytes carries the label of whoever
  ## encoded it: the producing wrapper for an aggregation constituent, the
  ## scheduler for a dispatched value. Render that label, do not recompute one:
  ## params_type holds class(value)[1] of a flumen-internal S3 class, so
  ## recomputing would say "object" or "expansion" where the consumer expects
  ## "vector".
  ##
  ## Placed before the strip / add_wrap block on purpose: those rules decide the
  ## CONTAINER (bare value vs list), orthogonal to the element type. The
  ## attributes are removed here so they never reach the wire.
  for(i in seq_along(par_list)){
    slot_val  <- par_list[[i]]
    slot_type <- attr(slot_val, "flumen_elem_type")
    if(is.null(slot_type)) next
    base_type <- sub("^wrapped:?", "", slot_type)
    ## A label with no type left after the marker is the bare "wrapped"
    ## sentinel, which names no type and must never be turned into "raw", the
    ## contract for the user's own bytes. Producers of such elements are decoded
    ## upstream (R/wildcard_dsl.R), so reaching here means an unexpected label:
    ## leave the slot exactly as delivery computed it.
    if(!nzchar(base_type)) next
    params_type[i] <- if(isTRUE(attr(slot_val, "flumen_elem_wrapped")))
                        paste0("wrapped:", base_type) else base_type
    params_is_arrow[i] <- isTRUE(attr(slot_val, "flumen_elem_is_arrow"))
    attr(slot_val, "flumen_elem_type")     <- NULL
    attr(slot_val, "flumen_elem_is_arrow") <- NULL
    attr(slot_val, "flumen_elem_wrapped")  <- NULL
    par_list[[i]] <- slot_val
  }

  ## "aggregate" and "list" are delivered as a list, "bare" as one value.
  ## .form and .uuids were read, and required, at the top of this function.
  params_form_vec    <- as.character(.form[.uuids])
  param_is_list_form <- params_form_vec %in% c("aggregate", "list")
  slot_is_list_form <- rep(param_is_list_form, wrapped_count)
  slot_origin_count <- rep(wrapped_count, wrapped_count)
  strip <- (!slot_is_list_form) & startsWith(params_type, "wrapped") & (slot_origin_count == 1L)
  if(any(strip)){
    stripped <- sub("^wrapped:?", "", params_type[strip])
    stripped[!nzchar(stripped)] <- "R_object"
    params_type[strip] <- stripped
  }
  ## Symmetric counterpart to `strip`: an AGGREGATION param (params_is_aggr)
  ## that resolved to a SINGLE bare constituent must still be delivered as a
  ## length-1 LIST -- the `_aggr` contract is "always a list", regardless of how
  ## many upstream instances exist. The shape-based detection in the resolvers
  ## (inherits(., "output_req_list")) cannot see this at 1 instance, so
  ## params_type is left bare and aggregate_params (serialize.R) delivers it
  ## unwrapped. Forcing the "wrapped:" marker makes aggregate_params wrap it into
  ## a list of 1, matching the multi-instance shape. FILE gathers are included
  ## for the same reason.
  ##
  ## slot_origin_count counts SLOTS, not elements: a standalone `_aggr` over a
  ## static wildcard arrives as ONE slot already holding N paths, so the slot
  ## value must be a single element too (slot_len).
  ## That length test applies to FILE types only: there the slot value is the
  ## character vector of paths, so its length IS the constituent count, while an
  ## object slot holds the SERIALIZED payload, whose length is a byte count.
  ##
  ## A file slot whose value still holds a "{{...}}" TEMPLATE is not a resolved
  ## path: the wrapper resolves it by globbing its directory, and only for an
  ## ATOMIC file_path parameter (inst/scripts/wrapper.R). Wrapping such a
  ## slot hides it from that branch, the glob never runs, and the unresolved
  ## template reaches the body, where the wrapper's guard stops it with
  ## "input '<p>' was not resolved". Leave those atomic here; the wrapper turns
  ## them into a list AFTER resolving them, so the gather still arrives as a list.
  is_file_slot <- params_type %in% c("file", "file_path", "file_object")
  slot_len     <- vapply(par_list, length, integer(1))
  ## has_template is consulted ONLY for file slots (see add_wrap below): for any
  ## other slot the `!is_file_slot |` branch already decides the element. `|` is
  ## vectorised, not short-circuiting, so it is restricted to file slots rather
  ## than run as.character() over an OBJECT slot's serialized payload.
  has_template <- is_file_slot
  if(any(is_file_slot))
    has_template[is_file_slot] <- vapply(par_list[is_file_slot], function(v)
                      any(grepl("{{", as.character(v), fixed = TRUE)), logical(1))
  add_wrap <- slot_is_list_form & (slot_origin_count == 1L) &
              !startsWith(params_type, "wrapped") &
              (!is_file_slot | (slot_len == 1L & !has_template))
  if(any(add_wrap)){
    params_type[add_wrap] <- paste0("wrapped:", params_type[add_wrap])
  }

  ## The form the WRAPPER is told is the form of the container it must build
  ## from the slots it receives, which is the declared one except
  ## for a file slot still carrying a "{{...}}" TEMPLATE: the wrapper resolves
  ## such a slot by globbing its directory, and only while the parameter is an
  ## ATOMIC file_path (inst/scripts/wrapper.R). It then makes the list itself
  ## from the "aggregation" class. Telling it "list" there would hide the slot
  ## from that branch and the unresolved template would reach the body.
  .slot_par <- rep(seq_along(wrapped_count), wrapped_count)
  .globbed  <- is_file_slot & has_template
  if(any(.globbed))
    params_form_vec[unique(.slot_par[.globbed])] <- "bare"

  # Per-element names, from expand_wrapped_params. They travel in the envelope's
  # elem_name field so the consumer wrapper can reattach them to its aggregated
  # par_list, while data_names carries the consumer's param name for grouping.
  # "" when the producer did not name its outputs.
  elem_name_vec <- if(!is.null(elem_names_from_producer)
                      && length(elem_names_from_producer) == n_params)
                     elem_names_from_producer else rep("", n_params)
  elem_name_vec[is.na(elem_name_vec)] <- ""

  data <- c(
    list(reg_tasks_wrapper$exe_object$shell_interpreter),
    list(reg_tasks_wrapper$exe_object$user_code),
    list(output_files),
    list(is_output_par),
    list(dyn_wildcards),
    list(output_auto_mkdir),
    list(output_par_names),
    list(output_object_par_names),
    ## The declared form of every parameter, in the order of the declared
    ## parameters (the slots below are emitted in that order, so the wrapper
    ## pairs the two by the order of first appearance of each name). A
    ## parameter that expands to no slot at all has no name in the message,
    ## so its form is left out too, keeping the two aligned.
    list(as.character(params_form_vec)[wrapped_count > 0L]),
    par_list
  )

  ## Types of the N_FIXED_DISPATCH_FIELDS fixed fields, in their declared order:
  ## shell_interpreter / user_code / output_files are character, is_output_par /
  ## dyn_wildcards / output_auto_mkdir are logical, output_par_names,
  ## output_object_par_names and params_form are character.
  data_type <- c("character", "character", "character",
                 "logical", "logical", "logical",
                 "character", "character", "character", params_type)
  # The N_FIXED_DISPATCH_FIELDS fixed fields use use_arrow; parameters propagate
  # params_is_arrow, decoded as in the table above. A Python op is all TRUE.
  ## A Python wrapper reads Arrow and nothing else, and the to_arrow line below
  ## flags every parameter as Arrow by INTENT: a payload that is already qs2
  ## stays qs2 (flumen never re-serialises) and travels mislabelled, so the
  ## wrapper feeds it to pa.ipc.open_stream and dies on "Invalid IPC stream:
  ## negative continuation token" -- for every type is_arrow_compatible_type()
  ## rejects (sparse matrix, S4, nested list, complex vector). Refused here
  ## rather than converted: re-encoding would move the payload through
  ## qs2 -> R -> Arrow inside the supervisor, which flumen does not do.
  ##
  ## THREE conditions, each load-bearing:
  ##   is.raw()             -- !params_is_arrow ALSO means "still a native R
  ##                           object about to be Arrow-encoded here", which is
  ##                           fine, so rawness is what separates the two.
  ##   !params_is_arrow     -- Arrow bytes are raw too, and they are correct.
  ##   !is_passthrough_type -- paths and raw legitimately travel non-Arrow.
  ##                           Tested on the TYPE, never on is_output_par: that
  ##                           vector is per-SLOT and misaligns as soon as a slot
  ##                           expands into several constituents.
  if(use_arrow){
    .off  <- N_FIXED_DISPATCH_FIELDS
    .bad  <- which(vapply(seq_along(params_type), function(i){
      j <- .off + i
      j <= length(data) && is.raw(data[[j]]) &&
        !isTRUE(params_is_arrow[i]) && !is_passthrough_type(params_type[i])
    }, logical(1)))
    if(length(.bad)){
      i <- .bad[1L]
      stop(sprintf("parameter '%s' of task '%s': %s",
                   par_names[i],
                   reg_tasks_wrapper$exe_object$task_id,
                   arrow_message_sentence(arrow_unsupported_message(params_type[i]))),
           call. = FALSE)
    }
  }

  to_arrow <- if(use_arrow) rep(TRUE, n_params + N_FIXED_DISPATCH_FIELDS)
              else c(rep(FALSE, N_FIXED_DISPATCH_FIELDS), params_is_arrow)

  data <- serialize_data(data, data_type, to_arrow)

  code_and_params_message <- list(
    sender = sender,
    msg_id = rep(msg_id, n_params + N_FIXED_DISPATCH_FIELDS),
    msg_type = msg_type,
    data_type = unlist(data$data_type$data_struct),
    data_names = c(FIXED_DISPATCH_FIELD_NAMES, par_names),
    elem_name  = c(rep("", N_FIXED_DISPATCH_FIELDS), elem_name_vec),
    data = data$data,
    is_arrow = data$data_type$is_arrow,
    file_name = data$file_name
  )
  
  return(code_and_params_message)
}

close_sockets <- function(socket_list){
  lapply(socket_list, function(sock) close(sock))
}

## Spawn a wrapper process for a task. Returns an integer PID.
spawn_wrapper <- function(cmd, args){
  sys::exec_background(cmd, as.character(args))
}

## Single snapshot of the process table; returns, for the subtree rooted at
## `pid`: the descendant PIDs (recursive ppid walk, plus `pid` itself) and the
## distinct process-GROUP ids the subtree spans, plus this process's own pgid.
##
## Process groups are the re-parenting-invariant handle. The ppid walk alone
## has a hole: if an intermediate process exits, its children are re-parented
## to init (ppid -> 1) and drop out of the walk. But re-parenting does NOT
## change a process's pgid -- so killing the GROUPS the subtree spans catches
## members the ppid walk missed. On Linux `sys::exec_background` /
## `sys::exec_internal` each place their child in a fresh process group, so the
## wrapper and each task tool already have their own group (no spawn-side help
## needed); the walk just needs to find one member of a group to learn its id.
proc_tree <- function(pid){
  pid <- as.integer(pid)
  out <- suppressWarnings(system2("ps", c("-eo", "pid=,ppid=,pgid="),
                                  stdout = TRUE, stderr = FALSE))
  m  <- regmatches(out, regexec(
          "^[[:space:]]*([0-9]+)[[:space:]]+([0-9]+)[[:space:]]+([0-9]+)", out))
  ok <- lengths(m) == 4L
  P  <- as.integer(vapply(m[ok], `[`, character(1), 2L))   ## pid
  PP <- as.integer(vapply(m[ok], `[`, character(1), 3L))   ## ppid
  PG <- as.integer(vapply(m[ok], `[`, character(1), 4L))   ## pgid
  desc     <- integer(0)
  frontier <- pid
  repeat {
    kids <- setdiff(P[PP %in% frontier], c(desc, pid))
    if(length(kids) == 0L) break
    desc     <- c(desc, kids)
    frontier <- kids
  }
  tree_pids <- c(desc, pid)
  own       <- PG[P == Sys.getpid()]
  list(pids     = tree_pids,
       pgids    = unique(PG[P %in% tree_pids]),
       own_pgid = if(length(own)) own[1] else NA_integer_)
}

## Terminate `pid` and its entire descendant subtree. Used to tear down a
## wrapper (-> the wrapper + its task tool + the tool's subprocesses) or a
## local supervisor (-> the supervisor + its wrappers + their tools).
terminate_process <- function(pid){
  pid <- as.integer(pid)
  if(is.na(pid) || pid <= 0L) return(invisible(NULL))

  if(.Platform$OS.type == "windows"){
    ## taskkill /T terminates the whole child tree.
    system2("taskkill", c("/F", "/T", "/PID", as.character(pid)),
            stdout = FALSE, stderr = FALSE)
  } else {
    tree <- proc_tree(pid)
    ## SIGKILL each process group the subtree spans -- but never our own group
    ## (it holds this supervisor / scheduler) nor the special groups 0/1.
    groups <- setdiff(tree$pgids, c(tree$own_pgid, 0L, 1L, NA_integer_))
    for(g in groups)
      system2("kill", c("-s", "KILL", "--", paste0("-", g)),
              stdout = FALSE, stderr = FALSE)
    ## ...and SIGKILL every PID directly -- backstop for any process whose
    ## group was excluded above as our own.
    for(p in tree$pids)
      suppressWarnings(tools::pskill(p, tools::SIGKILL))
  }
  ## reap `pid` (a direct child of the caller) to avoid a zombie.
  tryCatch(sys::exec_status(pid, wait = TRUE), error = function(e) NULL)
  invisible(NULL)
}

clean_termination <- function(all_pid, socket_list, cache = NULL){
  ## Sends issued by the event loop are queued, not waited on, so a message
  ## still on its way out would die with the sockets closed below. Bounded, so
  ## teardown cannot hang on a peer that is already gone.
  try(flush_pending_sends(3), silent = TRUE)
  if(!is.null(cache)) cache_reset(cache)
  if(length(all_pid) > 0){
    for(pid in all_pid){
      terminate_process(pid)
    }
  }
  close_sockets(socket_list)
}

## Worker-side dead-man's-switch predicate. A remote supervisor that never
## establishes its control channel to the scheduler (wrong / unreachable
## master_host, firewall) can never be told to terminate: the master kills remote
## workers ONLY over that channel (their PID lives on another host, so the
## master's per-PID clean_termination cannot reach them). This returns TRUE iff
## it is a remote supervisor that has NEVER connected and the connect deadline
## has elapsed -- the caller then self-terminates. `ever_connected` is the
## latched cv_value(pipe-connect cv) > 0 from a pipe_notify on sock_ctrl; a
## worker that connected even once is never flagged.
supervisor_orphaned <- function(is_remote, ever_connected, elapsed_secs, deadline_secs){
  isTRUE(is_remote) && !isTRUE(ever_connected) && elapsed_secs > deadline_secs
}

## `consumer_uses_arrow` is the executing wrapper's encoding (get_wrapper_info()
## $use_arrow). It defaults to TRUE, i.e. "decode everything".
get_param_info_from_exe_task_msg <- function(msg, consumer_uses_arrow = TRUE){

  par_names <- sapply(msg, function(par){
    return(par$data_names)
  })

  par_list <- lapply(msg, function(par){
    if(isTRUE(par$data_type == "file")){
      ## File transferred: the bytes were already consumed by
      ## push_chunked_message, which wrote them to disk at file_name. The
      ## wrapper expects the path string as the parameter value.
      return(par$file_name)
    }
    ## A parameter slot is either something this supervisor must UNDERSTAND or
    ## something it must merely FORWARD, and the envelope already says which:
    ##  - a dependency placeholder always travels as msg_type "parameter_request";
    ##  - file-typed slots carry paths that the pre-launch existence check in
    ##    sup_handle_execute_task and the wrapper both read as strings.
    ## Everything else is the user's DATA, which only the wrapper ever reads, so
    ## it is forwarded undecoded rather than decoded and re-serialised here.
    ##
    ## A Python wrapper reads Arrow and nothing else. The scheduler encodes a
    ## dispatched value in the consumer's codec (R/scheduler_dispatch.R), so
    ## bytes that are ALREADY Arrow are forwarded to an Arrow consumer as they
    ## are; the decode below is left for the payloads the scheduler could not
    ## encode (a type Arrow cannot carry), where it is what makes the re-encode,
    ## and the resulting error message, possible.
    .dt <- as.character(par$data_type)[1]
    must_understand <- isTRUE(par$msg_type == "parameter_request") ||
                       (!is.na(.dt) && grepl("file", .dt, fixed = TRUE))
    ## The label travels whenever the wire carries a REAL type, for every
    ## consumer. Pinned only on the way to an Arrow one, an R consumer gets bare
    ## bytes and delivery falls back to params_type, the very thing the paragraph
    ## above forbids: the bytes say one thing and params_type says another.
    ##
    ## "R_object" is excluded because it is not a type: it is the message tag a
    ## slot carries when nothing encoded it, and pinning it would replace one
    ## non-type with another. Those slots are left alone.
    if(!must_understand && (isTRUE(par$is_arrow) || !identical(.dt, "R_object"))){
      .bytes <- raw_payload(par$data)
      attr(.bytes, "flumen_elem_type")     <- .dt
      attr(.bytes, "flumen_elem_is_arrow") <- isTRUE(par$is_arrow)
      attr(.bytes, "flumen_elem_wrapped")  <- FALSE
      return(.bytes)
    }
    if(!must_understand && !isTRUE(consumer_uses_arrow)){
      return(raw_payload(par$data))
    }
    return(decode_message_data(par$data, par$data_type, par$is_arrow))
  })
  
  names(par_list) <- par_names
  
  ## Per-name data_type, read by group_by_name() and by the file re-classing
  ## in sup_handle_execute_task, both of which ask one question only: is this
  ## parameter a FILE parameter? The answer is per slot and it is "yes" only
  ## when every slot of the name says so; a name with one slot of another
  ## kind is not a file parameter. A uniform group keeps its own type.
  par_names_unique <- par_names[!duplicated(par_names)]
  par_data_types_per_slot <- sapply(msg, function(par){
    return(par$data_type)
  })
  par_data_types <- vapply(par_names_unique, function(nm){
    slots <- as.character(par_data_types_per_slot[par_names == nm])
    if (all(slots %in% c("file", "file_path"))) slots[1]
    else if (all(slots == slots[1])) slots[1]
    else "R_object"
  }, character(1))
  names(par_data_types) <- par_names_unique

  return(
    list(
      par_list       = par_list,
      par_data_types = par_data_types
    )
  )

}

## One row per constituent of every dependency parameter, derived from the
## container itself: a parameter is a dependency when its value is an
## output_req (one producer) or an output_req_list (one per producer).
update_wrapper_status <- function(wrapper_status,
                                  par_list,  ## for param dependency
                                  wrapper_id,
                                  task_id,
                                  status){

  dep_names <- names(par_list)[vapply(par_list, function(v)
    inherits(v, "output_req") || inherits(v, "output_req_list"), logical(1))]

  if(length(dep_names) > 0){

    entries_list <- lapply(dep_names, function(par_name) {

      par_obj <- par_list[[par_name]]
      
      if(inherits(par_obj, "output_req_list")) {

        sub_entries <- lapply(par_obj, function(sub_item) {
          data.frame(
            Wrapper_ID = wrapper_id,
            Op_ID = task_id,
            Param_dependency = par_name,
            Param_dependency_wrapper_ID = sub_item$exe_id,
            Supervisor_dependency = sub_item$socket,
            Status = "wait_ready",
            stringsAsFactors = FALSE
          )
        })

        return(do.call(rbind, sub_entries))

      } else {
        exe_id_val <- if (length(par_obj$exe_id) > 0) par_obj$exe_id else NA_character_
        socket_val <- if (length(par_obj$socket) > 0) par_obj$socket else NA_character_
        return(data.frame(
          Wrapper_ID = wrapper_id,
          Op_ID = task_id,
          Param_dependency = par_name,
          Param_dependency_wrapper_ID = exe_id_val,
          Supervisor_dependency = socket_val,
          Status = "wait_ready",
          stringsAsFactors = FALSE
        ))
      }
    })
    
    wrapper_entry <- do.call(rbind, entries_list)
    
  } else {
    wrapper_entry <- data.frame(
      Wrapper_ID = wrapper_id, 
      Op_ID = task_id, 
      Param_dependency = NA,
      Param_dependency_wrapper_ID = NA,
      Supervisor_dependency = NA,
      Status = status,
      stringsAsFactors = FALSE
    ) 
  }
  
  wrapper_status <- rbind(wrapper_status, wrapper_entry)
  return(wrapper_status)
}

