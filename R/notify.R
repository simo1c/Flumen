## Notification objects exchanged between supervisor and scheduler, plus the
## output-request envelopes used by P2P parameter resolution.


## Notification sent from supervisor to scheduler when an op finishes
## successfully. Carries the dyn_wildcard so the scheduler can perform the
## downstream substitution and update the execution graph.
new_result_notify <- function(supervisor_ID,
                              task_id,
                              exe_id,
                              dyn_wildcard = NULL,
                              n_outputs = 1,
                              output_par_names = NULL,
                              output_files = NULL){

  notify <- list(
    Supervisor_ID = supervisor_ID,
    task_id = task_id,
    exe_id = exe_id,
    dyn_wildcard = dyn_wildcard,
    n_outputs = n_outputs,
    ## Which declared output each RESULT belongs to, parallel to the results the
    ## producer sent. n_outputs counts results, not declared outputs, so without
    ## this the scheduler cannot tell one output's results from another's.
    output_par_names = output_par_names,
    ## The path each FILE result holds, parallel to the results (NA for an
    ## object result). Lets the wildcard engine drop, at expansion, a
    ## per-file instance whose file a per-sample filter excludes (see
    ## process_expansion_item); NULL from a producer that does not state it.
    output_files = output_files
  )

  class(notify) <- "result_notify"
  return(notify)
}

## Error notification sent from supervisor to scheduler. Carries all context the
## user needs to diagnose which op failed and why.
new_error_notify <- function(supervisor_ID,
                             task_id,
                             exe_id,
                             message,
                             stderr = "",
                             status = NA_integer_){

  notify <- list(
    Supervisor_ID = supervisor_ID,
    task_id         = task_id,
    exe_id        = exe_id,
    message       = as.character(message),
    stderr        = as.character(stderr),
    status        = as.integer(status)
  )

  class(notify) <- "error_notify"
  return(notify)
}

new_output_req <- function(dyn_wildcard,
                           exe_id,
                           output_instance,
                           socket = NULL,
                           uses_tcp = FALSE,
                           output_sel = NULL,
                           output_path_sel = NULL,
                           file_sel = NULL,
                           n_results = NULL,
                           source = NULL,
                           fetch = NULL,
                           shell_prefix = NULL,
                           prefix = NULL,
                           consumer_pattern = NULL,
                           result_path = NULL){

  ## THREE narrowings, three fields, ONE writer each: they are composable
  ## questions, not alternatives. Each is NULL when it does not apply, and a
  ## reader applies whichever are set IN SEQUENCE. All three take part in the
  ## P2P coalescing key, so consumers asking for different subsets of the same
  ## producer do not share a fetch.
  output_req <- list(
    dyn_wildcard = dyn_wildcard,
    exe_id = exe_id,
    output_instance = output_instance,
    socket = socket,
    ## WHERE the data is, relative to the consumer, is `source` below.
    uses_tcp = uses_tcp,
    ## WHICH DECLARED OUTPUT, by name. Written by R/scheduler_task_table.R for
    ## a producer declaring several dynamic outputs.
    output_sel = output_sel,
    ## WHICH DECLARED OUTPUT, said with a path instead of a name: a suffix
    ## regex over the produced path. Written by R/scheduler_dispatch.R, which
    ## has no name at hand for a FILE output. TEMPORARY by design: it is the
    ## surrogate the label makes unnecessary, and the next stage deletes this
    ## field rather than reinterpreting it.
    output_path_sel = output_path_sel,
    ## WHICH SUBSET OF THAT OUTPUT'S FILES: a basename regex derived at
    ## injection from the consumer's specialized dep pattern (e.g.
    ## "dir/{sample}_{{any}}" with sample=85 -> "^85_.*$"). Written by
    ## R/wildcard_engine.R.
    file_sel = file_sel,
    ## CONTRACT FIELDS: what the scheduler DECLARES so that the supervisor has
    ## nothing left to deduce. NULL until written.
    ##   n_results     how many results the named output holds on this producer
    ##                 instance; written by R/scheduler_task_table.R where the
    ##                 results are grouped by declared output
    ##   source        "colocal" (the producer ran on the consumer's worker),
    ##                 "same_host" (another worker on the master's machine) or
    ##                 "remote"; written by declare_constituent() in
    ##                 R/scheduler_dispatch.R once the transport is final
    ##   fetch         how the consumer obtains it: "cache", "path" or "bytes"
    ##   shell_prefix  TRUE when a shell consumer receives the backtick prefix
    ##                 instead of the resolved paths
    ##   prefix        that prefix, the text between the consumer's backticks
    ##   consumer_pattern  the consumer's own resolved dep pattern, written by
    ##                 the wildcard engine beside the producer's dyn_wildcard;
    ##                 the only place the consumer's backticks survive
    n_results = n_results,
    ## The path of the file result this request designates (output_instance),
    ## from the producer's notify; NULL for an object result or when unknown.
    result_path = result_path,
    source = source,
    fetch = fetch,
    shell_prefix = shell_prefix,
    prefix = prefix,
    consumer_pattern = consumer_pattern
  )

  class(output_req) <- "output_req"
  return(output_req)
}

new_output_req_list <- function(...) {
  reqs <- list(...)
  class(reqs) <- "output_req_list"

  return(reqs)
}

## THE one shape of a resolved dependency, on the scheduler as on the
## supervisor: a list of constituents, one output_req per producer, a single
## producer included. Every site that binds a producer's output_req to a
## consumer's parameter goes through here, so no reader has to ask whether
## it holds one constituent or a list of them.
as_output_req_list <- function(x) {
  if (inherits(x, "output_req_list")) return(x)
  if (inherits(x, "output_req")) return(new_output_req_list(x))
  if (is.list(x) && length(x) > 0L &&
      all(vapply(x, inherits, logical(1), "output_req"))) {
    return(do.call(new_output_req_list, unname(x)))
  }
  stop(flumen_fail(
    call  = "as_output_req_list()",
    error = sprintf("a resolved dependency must be an output_req or a list of them, got '%s'",
                    class(x)[1]),
    fix   = "this is an engine invariant, not a workflow error: report the workflow"),
    call. = FALSE)
}

new_result <- function(result) {
  class(result) <- "result_object"
  return(result)
}

get_result_instance <- function(result, instance = NULL){

  if(is.null(instance)){

    results <- lapply(seq_along(result), function(i){
      result[[i]]$result
    })

    result_type <- sapply(seq_along(result), function(i){
      result[[i]]$data_type
    })

    result_is_arrow <- sapply(seq_along(result), function(i){
      isTRUE(result[[i]]$is_arrow)
    })

    result_elem_names <- sapply(seq_along(result), function(i){
      x <- result[[i]]$elem_name
      if(is.null(x)) "" else as.character(x)
    })

    result_group_names <- sapply(seq_along(result), function(i){
      x <- result[[i]]$output_par_name
      if(is.null(x)) "" else as.character(x)
    })

  }else{

    results <- list(result[[instance]]$result)
    result_type <- result[[instance]]$data_type
    result_is_arrow <- isTRUE(result[[instance]]$is_arrow)
    result_elem_names <- {
      x <- result[[instance]]$elem_name
      if(is.null(x)) "" else as.character(x)
    }
    result_group_names <- {
      x <- result[[instance]]$output_par_name
      if(is.null(x)) "" else as.character(x)
    }

    ## instance != NULL = caller wants a specific list element (_exp case).
    ## Strip the "wrapped:" marker to treat the element as scalar instead of
    ## preserving the list. The plain case (output_instance=NULL) goes through
    ## the instance=NULL branch which keeps "wrapped:X".
    result_type <- sub("^wrapped:", "", result_type)
    if(identical(result_type, "wrapped")) result_type <- "R_object"

  }

  return(
    list(
      result = results,
      result_type = result_type,
      result_is_arrow = result_is_arrow,
      result_elem_names = result_elem_names,
      ## Which declared output each result belongs to. Carried so a consumer can
      ## ask for ONE output of a multi-output producer; nothing reads it yet,
      ## the selector that will is the next stage.
      result_group_names = result_group_names
    )
  )

}
