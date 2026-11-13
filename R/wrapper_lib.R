## Shared support code for the R-runtime wrapper subprocesses:
## inst/scripts/wrapper.R (R user functions) and inst/scripts/wrapper_shell.R (shell
## scripts). Both dial the supervisor, announce READY, receive a (chunked) task, run
## it, and send a (chunked) result back. The connection, result-send and
## error-handling steps common to the two live here. The Python wrapper,
## inst/scripts/wrapper.py, has its own equivalents -- it cannot use these R
## functions.

## Dial the supervisor's per-wrapper PAIR socket and announce READY. On dial failure
## the wrapper has no socket to report the error on, so log + quit(1). Returns the
## open socket.
wrapper_connect <- function(socket_path, task_id){
  s <- tryCatch(
    nanonext::socket("pair", dial = socket_path),
    error = function(e){
      to_log(paste0("Failed to dial supervisor socket '", socket_path, "': ", conditionMessage(e)), task_id)
      quit(status = 1)
    })
  ready <- serialize_data(list(0), "R_object", FALSE)
  msg   <- new_message(sender = task_id, msg_id = task_id, msg_type = "ready",
                       data_type = unlist(ready$data_type$data_struct), data = ready$data,
                       is_arrow = ready$data_type$is_arrow)
  flumen_send_aio(s, msg$message, mode = "raw")
  s
}

## Receive one complete (possibly chunked) dispatch message and return it merged.
## A wrapper process handles exactly one task, so this returns once.
##
## Blocks on call_aio() instead of polling nanonext::unresolved(): polling spins a
## core without advancing the async op. call_aio() also makes a chunked message
## complete deterministically. The loop lives here so every wrapper shares one
## copy of it, rather than each carrying its own.
wrapper_receive_task_message <- function(s){
  res_chunked_message <- list()
  code <- nanonext::recv_aio(s, mode = "raw")
  repeat {
    nanonext::call_aio(code)
    if(!nanonext::unresolved(code)){
      msg      <- decode_message_metadata(code$data)
      msg_id   <- msg[[1]]$msg_id
      chunk    <- msg[[1]]$chunk
      n_chunks <- msg[[1]]$n_chunks
      res_chunked_message <- push_chunked_message(res_chunked_message, msg, msg_id, chunk)
      if(length(res_chunked_message[[msg_id]]) == n_chunks)
        return(merge_all_chunks(res_chunked_message[[msg_id]]))
      ## Message still incomplete: arm the receive for the next chunk. The aio
      ## being replaced has already resolved and been consumed.
      code <- nanonext::recv_aio(s, mode = "raw")
    }
  }
}

## Decode the fixed dispatch fields of a task message, create the parent directory
## of each declared output, and snapshot those directories before the task runs.
## Slots 2..N_FIXED_DISPATCH_FIELDS are the fields FIXED_DISPATCH_FIELD_NAMES names
## from `user_code` on; slot 1 carries the envelope metadata and, for a shell task,
## the interpreter, which the shell wrapper reads itself. Returns the decoded fields
## plus the snapshot.
wrapper_prepare_outputs <- function(msg){

  func_code      <- decode_message_data(msg[[2]]$data, msg[[2]]$data_type, msg[[2]]$is_arrow)
  out_files      <- decode_message_data(msg[[3]]$data, msg[[3]]$data_type, msg[[3]]$is_arrow)
  is_output_par  <- decode_message_data(msg[[4]]$data, msg[[4]]$data_type, msg[[4]]$is_arrow)
  dyn_wildcards  <- decode_message_data(msg[[5]]$data, msg[[5]]$data_type, msg[[5]]$is_arrow)
  ## Per-output auto_mkdir flag (msg[[6]]); parallel vector to out_files,
  ## populated by R/supervisor_lib.R from exe_object$output_auto_mkdir.
  out_auto_mkdir <- decode_message_data(msg[[6]]$data, msg[[6]]$data_type, msg[[6]]$is_arrow)
  ## Per-output declared NAME (msg[[7]]); parallel vector to out_files, built
  ## with it in R/object_func.R. Read, never inferred: which output a produced
  ## file belongs to is part of the declaration, and a wrapper that guessed it
  ## from the message shape would be a second construction of something the
  ## engine already knows.
  out_par_names      <- decode_message_data(msg[[7]]$data, msg[[7]]$data_type, msg[[7]]$is_arrow)
  ## Declared OBJECT outputs by name (msg[[8]]), one entry each, in declaration
  ## order. It answers a different question from the three vectors above and is
  ## indexed on a different thing, so it is deliberately NOT filtered by the
  ## keep_files mask below: masking a vector that is not parallel to out_files
  ## would shift it by however many file patterns the task declared.
  out_object_par_names <- decode_message_data(msg[[8]]$data, msg[[8]]$data_type, msg[[8]]$is_arrow)
  ## The FORM each parameter is delivered in (msg[[9]]): "bare", "list" or
  ## "aggregate", one entry per declared parameter in slot order. It is what
  ## the scheduler declared from the user's declaration; the wrapper reads it to
  ## build the parameter's container rather than counting same-named slots.
  ## A message with fewer fixed fields than this build expects comes from a
  ## supervisor of another build, and its parameter slots start where this
  ## wrapper reads a fixed field. Stop, rather than read one thing for another.
  if(length(msg) < N_FIXED_DISPATCH_FIELDS)
    stop(sprintf(paste("the dispatch message carries %d fixed field(s), expected %d.",
                       "Fix: reinstall the same flumen build everywhere."),
                 length(msg), N_FIXED_DISPATCH_FIELDS), call. = FALSE)
  params_form <- decode_message_data(msg[[9]]$data, msg[[9]]$data_type, msg[[9]]$is_arrow)

  ## Filter empty-sentinel entries from out_files AND keep the two parallel
  ## vectors aligned by the same mask.
  keep_files     <- out_files != ""
  out_files      <- out_files[keep_files]
  out_auto_mkdir <- out_auto_mkdir[keep_files]
  out_par_names      <- out_par_names[keep_files]

  if(length(out_files)){
    for(i in seq_along(out_files)){
      f <- strip_backticks(out_files[i])
      ## Gate the parent-dir pre-create on the per-output auto_mkdir flag.
      ## Default TRUE keeps the convenience of writing straight out (the task
      ## can writeLines/saveRDS without dir.create); explicit FALSE leaves the
      ## directory untouched so a tool that creates it itself (cellranger
      ## --id=foo etc.) does not collide with a flumen-managed pre-create.
      if(isTRUE(out_auto_mkdir[i])){
        dir_created <- create_dir_structure(f)
      }
    }
  }

  ## The names present in each output directory BEFORE the task runs. This is
  ## NOT how flumen decides what the task produced: a declared output that
  ## already exists, or a dynamic pattern that already matches something, is
  ## refused before the body runs (wrapper_refuse_preexisting_outputs), so after
  ## the body every file the pattern matches is the task's own. The names are
  ## kept only for the courtesy warning about files no declared pattern matches.
  ## No mtime, no size.
  out_dirs_snapshot <- list()
  if(length(out_files)){
    for(f in out_files){
      f_clean <- strip_backticks(f)
      d <- dirname(f_clean)
      rec <- is_recursive_wildcard(f_clean)
      key <- paste0(d, "\t", rec)
      if(!key %in% names(out_dirs_snapshot)){
        out_dirs_snapshot[[key]] <- list(
          files = if(dir.exists(d)) list.files(d, recursive = rec) else character(0))
      }
    }
  }

  list(func_code         = func_code,
       out_files         = out_files,
       out_par_names         = out_par_names,
       out_object_par_names = out_object_par_names,
       is_output_par     = is_output_par,
       dyn_wildcards     = dyn_wildcards,
       params_form       = params_form,
       out_dirs_snapshot = out_dirs_snapshot)
}

## A file a task declares it will write must not exist before the task runs. For
## a static output that is the path itself; for a dynamic pattern it is any file
## in its directory that the pattern already matches. Either is refused here,
## before the body runs, with the file named, and the workflow stops.
##
## With nothing matching before the body, everything matching after is the
## task's own by construction. Re-running in a directory that still holds the
## previous outputs therefore stops at the first producer: remove the outputs,
## or write elsewhere.
##
## Shared by the R and the shell wrapper; wrapper.py mirrors it.
wrapper_refuse_preexisting_outputs <- function(out_files, task_id, s, chunk_size,
                                               max_named = 5L){
  for(f in out_files){
    f_clean <- strip_backticks(f)
    d    <- dirname(f_clean)
    base <- basename(f_clean)
    is_dyn <- grepl(DYN_WILDCARD_PATTERN, base, perl = TRUE)
    present <- if(is_dyn){
      if(!dir.exists(d)) character(0)
      else {
        m <- extract_multiple_wildcard_values(base, list.files(d, recursive = is_recursive_wildcard(f_clean)))$matches
        file.path(d, m)
      }
    } else if(file.exists(f_clean)) f_clean else character(0)
    if(!length(present)) next
    shown <- present
    more  <- length(shown) - max_named
    if(more > 0L) shown <- c(shown[seq_len(max_named)], sprintf("and %d more", more))
    err <- sprintf(
      "instance '%s' output '%s' already exists: %s. Fix: remove it or write to another path.",
      task_id, f, paste(shown, collapse = ", "))
    to_log(paste0("[WR_ERR] ", err), task_id)
    send_error_message(task_id, s, chunk_size, err)
    close(s)
    quit(status = 1)
  }
  invisible(TRUE)
}

## Which declared output each OBJECT result belongs to, the object-side twin of
## the per-file attribution wrapper_collect_declared_outputs() carries. Shared by
## the R and the shell wrapper so one rule exists, not two; the Python wrapper
## mirrors it in inst/scripts/objects.py::object_output_labels.
##
## The discriminator is HOW MANY object outputs the task DECLARED, which is what
## msg[[8]] states. It is not the number of results: a single dynamic object
## output returning a list of three produces three results that all belong to
## that one output.
wrapper_object_output_labels <- function(out_object_par_names, n_results,
                                         elem_name = NULL){
  if(length(out_object_par_names) == 1L){
    ## ONE declared object output: every returned value belongs to it, whatever
    ## the return's own element names say. Under a dynamic output those names
    ## are the WILDCARD VALUES (list(a=, b=, c=) means three instances), never
    ## the output's name, so reading them here would put a wildcard value in the
    ## field that says which output a result came from.
    return(rep(as.character(out_object_par_names), n_results))
  }
  if(length(out_object_par_names) > 1L &&
     !is.null(elem_name) && length(elem_name) == n_results){
    ## SEVERAL declared object outputs: the contract is that the names of the
    ## returned list ARE the output names, so the producer has already stated
    ## the attribution and this only moves it into the grouping slot.
    return(as.character(elem_name))
  }
  ## Several declared object outputs and an unnamed return. The label stays
  ## empty: that return is ambiguous and is refused downstream by name, so no
  ## positional guess is made here.
  rep("", n_results)
}

## Resolve every declared output to the files the task actually produced, and return
## them as one character vector. The contract itself lives in
## R/fs.R::collect_declared_output(); the reporting stays here because only the
## wrapper knows its socket and how it must die.
wrapper_collect_declared_outputs <- function(out_files, task_id, out_dirs_snapshot,
                                             s, chunk_size, out_par_names){
  files_new <- c()
  ## Which declared output produced each file, taken from out_par_names[i] as
  ## the loop goes, so a consumer downstream can tell one output's files from
  ## another's.
  owner_new <- character(0)
  for(i in seq_along(out_files)){
    out_res <- collect_declared_output(out_files[i], task_id)
    if(!is.null(out_res$error)){
      to_log(paste0("[WR_ERR] ", out_res$error), task_id)
      send_error_message(task_id, s, chunk_size, out_res$error)
      close(s)
      quit(status = 1)
    }
    files_new <- c(files_new, out_res$files)
    owner_new <- c(owner_new, rep(out_par_names[i], length(out_res$files)))
  }
  ## Here rather than inside the loop: the check needs what ALL the declared
  ## outputs matched, or two patterns in one directory each report the other's
  ## files as leftovers.
  warn_unmatched_produced_files(out_files, files_new, task_id, out_dirs_snapshot)
  ## The attribution rides an attribute: every caller wraps this in as.list()
  ## and passes it on as the result, so an attribute reaches them without any
  ## of them having to be rewritten to carry a pair.
  attr(files_new, "flumen_output_name") <- owner_new
  files_new
}

## The text a shell task receives for one parameter value. A classed atomic
## value (factor, Date, POSIXct, difftime, integer64, ...) becomes its own
## as.character() form -- "2024-03-15", "lo", "1099511627776" -- instead of the
## storage unlist() / c() leave behind (19797, factor codes, bit patterns). A
## list (an aggregated value) is converted element by element. Anything without
## a class is returned as it is, so plain numbers, strings and logicals render
## exactly as before.
shell_arg_text <- function(v){
  if (is.list(v) && !is.data.frame(v))
    return(lapply(v, shell_arg_text))
  if (is.atomic(v) && is.object(v)) {
    out <- as.character(v)
    names(out) <- names(v)
    return(out)
  }
  v
}

## Number of outputs the result envelope must declare. `data_type` is NULL when the
## caller has not assigned one yet (an R task with no file output types its result
## further down), which is what the caller's `exists("data_type")` expresses.
##
## A task whose outputs are FILES produces exactly one file per declared static
## output, so the count is unambiguous and must be reported: the scheduler sizes
## its output_req list on n_outputs, and a short list makes every dep collapse
## onto the first output. Objects stay on the conservative rule, where a returned
## list is genuinely ambiguous between one list-output and N outputs -- unless
## the task declares SEVERAL object outputs: then the contract is a named list
## whose names are the output names, one element per output, and reporting 1
## made every consumer receive the first output whatever it asked for.
wrapper_n_outputs <- function(out_dyn_par, result, data_type = NULL,
                              n_object_outputs = 1L){
  n_outputs <- 1
  if(!is.null(out_dyn_par)){
    if("list" %in% class(result)){
      n_outputs <- length(result)
    }
  }
  if(is.null(out_dyn_par) && isTRUE(n_object_outputs > 1L) &&
     "list" %in% class(result)){
    n_outputs <- length(result)
  }
  ## ONE static object output returning a list: the wrapper sends the list as
  ## its elements, one result each, and reporting 1 delivered the first element
  ## alone -- list(1, 2) reached the consumer as 1. Counted as under a dynamic
  ## output, whose plain dependency reassembles the elements into the list.
  ## An empty list keeps the old count: it is a separate case.
  if(is.null(out_dyn_par) && isTRUE(n_object_outputs == 1L) &&
     "list" %in% class(result) && length(result) >= 1L){
    n_outputs <- length(result)
  }
  if(!is.null(data_type) && length(data_type) > 0 &&
     all(data_type == "file_path") && is.list(result)){
    n_outputs <- length(result)
  }
  n_outputs
}

## Build the result message (the producer's data + the dyn_wildcard / n_outputs trailer
## rows) and send it back to the supervisor in chunks. `data` is the list of result
## elements, `data_type` their per-element types, `to_arrow` their per-element Arrow
## flags, all of length N and NOT including the 2 trailer rows. `data_names` is the
## grouping slot and no wrapper sets it, so it stays "" per element.
wrapper_send_result <- function(task_id, s, chunk_size, data, data_type, dyn_wildcard,
                                n_outputs, to_arrow, data_names = NULL,
                                elem_name = NULL){
  n_result  <- length(data)
  if(is.null(data_names)) data_names <- rep("", n_result)
  ## elem_name: per-element names within the data_names group, for example the
  ## names of a user-returned named list. Sent so consumers can preserve them
  ## across IPC aggregation.
  if(is.null(elem_name))   elem_name   <- rep("", n_result)
  ser <- serialize_data(c(data, list(dyn_wildcard), list(n_outputs)),
                        c(data_type, "vector", "vector"),
                        c(to_arrow, FALSE, FALSE))
  message <- list(
    sender     = rep(task_id, n_result + 2L),
    msg_id     = rep(task_id, n_result + 2L),
    msg_type   = c(rep("result", n_result), "dyn_wildcard", "n_outputs"),
    data_type  = unlist(ser$data_type$data_struct),
    data_names = c(data_names, "", ""),
    elem_name  = c(elem_name,  "", ""),
    data       = ser$data,
    is_arrow   = ser$data_type$is_arrow,
    file_name  = ser$file_name)
  send_message_by_chunks(task_id, message, s, chunk_size)
}

## Catch-all for errors in the wrapper itself, outside the user-code / parse
## tryCatch: notify the supervisor so the workflow aborts cleanly instead of hanging
## on a result that never comes, then close the socket + quit(1).
wrapper_fatal <- function(e, task_id, s, chunk_size, what = "wrapper error"){
  to_log(paste0("[WR_ERR] ", what, ": ", conditionMessage(e)), task_id)
  try(send_error_message(task_id, s, chunk_size, paste0(what, ": ", conditionMessage(e))), silent = TRUE)
  try(close(s), silent = TRUE)
  quit(status = 1)
}
