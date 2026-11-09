args_trailing <- commandArgs(trailingOnly = TRUE)
task_id <- args_trailing[1]
socket_path <- args_trailing[2]
chunk_size <- as.numeric(args_trailing[3])

suppressPackageStartupMessages(library(flumen))
## Internals are reached with flumen::: instead of attach(asNamespace())

s <- flumen:::wrapper_connect(socket_path, task_id)

tryCatch({

## Receive the dispatch message. Shared with wrapper.R (R/wrapper_lib.R), so the
## blocking receive is written once. A failure at this point concerns the infrastructure, 
## not task processing, and is reported by the outer wrapper_fatal below.
msg <- flumen:::wrapper_receive_task_message(s)

## Catch-all for uncaught R errors during task processing. Without it a non-wrapped
## error kills the wrapper silently and the supervisor waits forever for a result
## that never comes, stalling the workflow. Mirrors the structured error path used
## below for exec and shell failures. temp_sh_path is declared here so the handler
## can clean it up when the failure falls between create_temp_sh_script() and the
## natural cleanup point.
temp_sh_path <- NULL
tryCatch({

## Slot 1 carries the envelope metadata and, for a shell task, the interpreter.
shell_interpreter <- flumen:::decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)

declared          <- flumen:::wrapper_prepare_outputs(msg)
func_code         <- declared$func_code
out_files         <- declared$out_files
out_par_names         <- declared$out_par_names
out_object_par_names <- declared$out_object_par_names
is_output_par     <- declared$is_output_par
dyn_wildcards     <- declared$dyn_wildcards
out_dirs_snapshot <- declared$out_dirs_snapshot

## A declared output that already exists is refused before anything runs.
flumen:::wrapper_refuse_preexisting_outputs(out_files, task_id, s, chunk_size)

if(length(msg) > flumen:::N_FIXED_DISPATCH_FIELDS){
  par_list <- lapply(msg[-seq_len(flumen:::N_FIXED_DISPATCH_FIELDS)], function(par){
    ## par$data_type is an atomic string here (unlist in
    ## prepare_code_and_parameters_for_wrapper), with no $data_struct. A wrong reach
    ## is silent when is_arrow=FALSE: the qs2 branch never reads it, so R never
    ## forces the error.
    par <- flumen:::decode_message_data(par$data, par$data_type, par$is_arrow)
    return(par)
  })
}else{
  par_list <- list()
}

par_list_names <- sapply(msg[-seq_len(flumen:::N_FIXED_DISPATCH_FIELDS)], function(par){
  return(par$data_names)
})


## Output parameter with dynamic wildcard
w_dyn_par <- which(is_output_par & dyn_wildcards)
out_dyn_par <- NULL

if(length(w_dyn_par)){
  out_dyn_par <- par_list_names[w_dyn_par]
}

dyn_wildcard <- ""
if(length(w_dyn_par)){
  dyn_wildcard <- as.character(par_list[[w_dyn_par[1]]])
}


names(par_list) <- par_list_names

## Shell ops accept files (new_file()) and atomic values, scalar or vector, and
## lists whose elements are all atomic (a shell op's N wildcard files). A
## data.frame, matrix, array or raw cannot be marshalled as a CLI argument.
## The check runs on R-decoded values, so it covers Python sources too, which
## arrive as the corresponding R type.
for (.i in seq_along(par_list)) {
  .par <- par_list[[.i]]
  .pname <- par_list_names[.i]
  .invalid <- if (is.data.frame(.par)) "data.frame"
    else if (is.matrix(.par)) "matrix"
    else if (is.array(.par)) "array"
    else if (is.raw(.par)) "raw bytes"
    ## An AGGREGATED raw arrives as a LIST of raw vectors: is.raw() is FALSE
    ## on the list, and raw counts as atomic, so it slipped past both guards
    ## and the task stalled with no diagnostic at all. Bytes cannot become an
    ## argv token either way -- reject them here, loudly, like the scalar case.
    else if (is.list(.par) && length(.par) &&
             all(vapply(.par, is.raw, logical(1)))) "raw bytes"
    else if (is.list(.par) && !all(vapply(.par, is.atomic, logical(1)))) "list with non-atomic elements"
    else NULL
  if (!is.null(.invalid)) {
    err_msg <- sprintf(
      "[shell op %s] cannot consume parameter '%s' as %s. Shell ops accept only files (new_file()) or simple atomic values. Declare the upstream op's output as new_file() instead.",
      task_id, .pname, .invalid
    )
    to_log(err_msg, task_id)
    flumen:::send_error_message(task_id, s, chunk_size, err_msg)
    close(s)
    quit(status = 1)
  }
}

par_list <- lapply(par_list, function(par){

  new_par <- c()

  if("file_path" %in% class(par)){

    dyn_wild <- flumen:::find_dynamic_wildcards(par)
    suffix <- flumen:::extract_suffix_contents(par)
    dyn_wild <- setdiff(dyn_wild, suffix)

    if(!is.null(dyn_wild)){
      dir_names <- dirname(par[dyn_wild])
      file_matched <- flumen:::extract_multiple_wildcard_values(basename(par[dyn_wild]), list.files(dirname(par[dyn_wild])))$matches
      new_par <- c(new_par, file.path(dir_names, file_matched))
    }

    if(!is.null(suffix$indices)){
      new_par <- c(new_par, suffix$values)
    }

    other <- setdiff(seq_along(par), c(dyn_wild, suffix$indices))

    if(length(other)){
      new_par <- c(new_par, par[other])
    }

    new_par

  }else{
    par
  }
})

## command_repr holds a readable form of what is about to run, so a worker's failure
## log shows the resolved script or argv without a debug re-run.
## A classed value (factor, Date, POSIXct, difftime, integer64, ...) is rendered to
## text here, once, for both the script and the command line: both build their text
## with unlist()/c(), which strip the class and keep the storage, so a Date would
## reach the tool as 19797 and a factor as its codes, silently. Plain types are left
## untouched.
par_list <- lapply(par_list, flumen:::shell_arg_text)

command_repr <- ""
if("sh_script" %in% class(func_code)){

  ## A multi-file parameter arrives as N slots sharing one name, merged here into one
  ## length-N value: as.environment() below would otherwise keep only the last,
  ## silently. The value renders as space-separated paths for the {name} placeholder.
  par_list <- lapply(
    split(par_list, factor(names(par_list), levels = unique(names(par_list)))),
    function(g) {
      v <- unlist(g, use.names = FALSE)
      if (length(v) > 1) paste(v, collapse = " ") else v
    })

  ## Substitute {param} placeholders WITHOUT glue, which treats every {...} as an R
  ## expression and so collides with bash brace syntax (${var}, ${v%%p}, brace groups,
  ## brace expansion). Only {declared_param} tokens are replaced, in three steps:
  ##   1. hide doubled {{ }} behind sentinels, so the matcher cannot see a {name}
  ##      nested inside one and substitute it;
  ##   2. replace a brace-wrapped declared param not preceded by '$', leaving bash
  ##      ${var} alone;
  ##   3. restore the sentinels AS WRITTEN, doubled.
  ## Every other brace is left untouched.
  ##
  ## Step 3 must not restore a single brace. Treating {{ }} as an escape for a literal
  ## brace leaves a nested awk/jq/sed block that closes two levels at once, "...{...}}",
  ## one brace short on the worker, failing with "missing } near end of file", an error
  ## that accuses the tool rather than the rendering. No escape is needed: ${VAR} is
  ## already protected by the '$' guard in step 2.
  to_glue <- as.character(func_code)
  to_glue <- gsub("}}", "\002", gsub("{{", "\001", to_glue, fixed = TRUE), fixed = TRUE)
  if (length(par_list)) {
    .m <- gregexpr("(?<![$])\\{([A-Za-z0-9_.]+)\\}", to_glue, perl = TRUE)
    regmatches(to_glue, .m) <- lapply(regmatches(to_glue, .m), function(toks)
      vapply(toks, function(tok) {
        nm <- sub("^\\{(.*)\\}$", "\\1", tok)
        if (nm %in% names(par_list))
          paste(as.character(par_list[[nm]]), collapse = " ") else tok
      }, character(1)))
  }
  to_glue <- gsub("\002", "}}", gsub("\001", "{{", to_glue, fixed = TRUE), fixed = TRUE)
  temp_sh_path <- flumen:::create_temp_sh_script(to_glue)
  command_repr <- paste0("--- generated script ---\n", to_glue)
  func_code <- shell_interpreter
  args <- c(temp_sh_path)

}else if("sh_cmd" %in% class(func_code)){

  ## An OBJECT output means flumen captures the command's standard output as the
  ## result: there is no path to pass, and the value is still the unresolved
  ## "{{name}}" pattern, so passing it makes `ls` try to list a file called
  ## {{listing}}. A FILE output does belong on the command line (fastp -o
  ## trimmed.fq), so only the object case is dropped.
  ##
  ## The type is read per slot from the message, which keeps it aligned with par_list
  ## when one declared parameter occupies several slots. is_output_par cannot be used:
  ## unlike par_names and params_type it is not slot-replicated (R/supervisor_lib.R),
  ## so indexing it against par_list drifts by the extra slots earlier parameters took.
  arg_types <- vapply(msg[-seq_len(flumen:::N_FIXED_DISPATCH_FIELDS)],
                      function(p) as.character(p$data_type)[1], character(1))

  par_list_names[duplicated(par_list_names)] <- ""

  args <- unlist(lapply(seq_along(par_list), function(i) {
    ## "flumen_object" is the marker class of new_object(), and "object" is
    ## accepted beside it: a scheduler sending either string has its object
    ## slots skipped, instead of pasted onto the command line as arguments.
    if (i <= length(arg_types) && arg_types[i] %in% c("flumen_object", "object")) return(NULL)
    if (!is.null(par_list_names[i]) && par_list_names[i] != "") {
      return(c(par_list_names[i], par_list[[i]]))
    } else {
      return(par_list[[i]])
    }
  }))
  command_repr <- paste0("--- command ---\n",
                         paste(c(func_code, args), collapse = " "))
}

## error = FALSE makes sys::exec_internal return list(status, stdout, stderr) on a
## non-zero exit instead of calling stop(): the default stops before the captured
## output is returned, discarding stdout and stderr. The post-check branch below
## forwards the real stderr through send_error_message(..., stderr =, status =), so
## the user sees the tool's own error and not just "bash status N".
## Two failure modes:
##   (1) launch failure (fork/exec): tryCatch fires, conditionMessage(e) is all
##       the diagnostic there is;
##   (2) the command ran and exited non-zero: result$status != 0, stdout and
##       stderr captured.
result <- tryCatch(
  sys::exec_internal(func_code, args = args, error = FALSE),
  error = function(e){
    to_log(paste0("[WR_ERR] exec_internal threw: ",
                  conditionMessage(e), "\n", command_repr), task_id)
    if(!is.null(temp_sh_path)) try(unlink(temp_sh_path), silent = TRUE)
    flumen:::send_error_message(task_id, s, chunk_size, conditionMessage(e))
    close(s)
    quit(status = 1)
  }
)

## Cleanup of the temporary script (exec_internal completed, success or status != 0)
if(!is.null(temp_sh_path)) try(unlink(temp_sh_path), silent = TRUE)

if(!is.null(result$status) && result$status != 0){
  stderr_str <- tryCatch(rawToChar(result$stderr), error = function(e) "")
  stdout_str <- tryCatch(rawToChar(result$stdout), error = function(e) "")

  ## Worker-side log: structured diagnostic so a failed shell task can
  ## be debugged from the worker log alone (full script/command, full
  ## stderr, full stdout). Only emitted on failure -- no overhead for
  ## successful tasks.
  log_blocks <- c(
    sprintf("[WR_ERR] shell exit status=%d", result$status),
    command_repr,
    if(nchar(stderr_str) > 0) paste0("--- stderr ---\n", stderr_str),
    if(nchar(stdout_str) > 0) paste0("--- stdout ---\n", stdout_str)
  )
  to_log(paste(log_blocks, collapse = "\n"), task_id)

  ## Scheduler-side: short message + stderr/status fields. execution.R
  ## (run_workflow) appends "\n  status  : N" and
  ## "\n  stderr  : <text>" to the user-facing stop() message.
  flumen:::send_error_message(
    task_id, s, chunk_size,
    sprintf("Shell command failed with exit status %d", result$status),
    stderr = stderr_str,
    status = result$status
  )
  close(s)
  quit(status = 1)
}

result_group_names <- NULL
if(length(out_files)){

  ## Keep the collected vector before as.list(): the per-file declared-output
  ## name rides an attribute, and as.list() drops it.
  collected <- flumen:::wrapper_collect_declared_outputs(out_files, task_id,
                                                out_dirs_snapshot, s, chunk_size,
                                                out_par_names)
  result_group_names <- attr(collected, "flumen_output_name")
  result    <- as.list(collected)
  data_type <- rep("file_path", length(result))

}else{
  ## An empty stdout is a payload of zero bytes, delivered as such. Do not substitute
  ## a value: list(0) tagged "R_object" is relabelled "raw" by serialize_data, so an
  ## R consumer receives the 72 bytes of qs2's encoding of 0 and a Python consumer a
  ## refusal.
  result <- list(if(length(result$stdout)) result$stdout else raw(0))
  data_type <- "raw"
}

## An output parameter with dynamic wildcard
n_outputs <- flumen:::wrapper_n_outputs(out_dyn_par, result, data_type)

# save(result, "result", task_id)


## data_names is the GROUPING slot: which declared output each result belongs to.
## An OBJECT output here is the command's standard output, so there is exactly one
## result, belonging to the declared object output named in msg[[8]]. The helper is
## the one wrapper.R calls and needs no per-slot indexing: is_output_par is not
## slot-replicated, so the names could not be recovered from the parameter rows.
if(!length(out_files)){
  result_group_names <- flumen:::wrapper_object_output_labels(out_object_par_names,
                                                              length(result))
}
flag <- flumen:::wrapper_send_result(task_id, s, chunk_size, result, data_type, dyn_wildcard, n_outputs, to_arrow = rep(FALSE, length(result)), data_names = result_group_names)

}, error = function(e){
  err_str <- conditionMessage(e)
  to_log(paste0("[WR_ERR] uncaught R error in task processing: ", err_str), task_id)
  if(!is.null(temp_sh_path)) try(unlink(temp_sh_path), silent = TRUE)
  flumen:::send_error_message(task_id, s, chunk_size, err_str)
  close(s)
  quit(status = 1)
})

}, error = function(e) flumen:::wrapper_fatal(e, task_id, s, chunk_size, what = "shell wrapper error"))

## Keepalive: the wrapper waits for SIGTERM from the supervisor.
Sys.sleep(Inf)
