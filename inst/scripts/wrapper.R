args_trailing <- commandArgs(trailingOnly = TRUE)
task_id <- args_trailing[1]
socket_path <- args_trailing[2]
chunk_size <- as.numeric(args_trailing[3])

suppressPackageStartupMessages(library(flumen))
## Internals are reached with flumen::: instead of attach(asNamespace()): a
## namespace on the search path makes R refuse to byte-compile every closure
## defined here (frameTypes: "namespace found within global environments"),
## which the JIT then retries for each new closure.

s <- flumen:::wrapper_connect(socket_path, task_id)

tryCatch({

## Receive the dispatch message and decode its fixed fields. Both live in
## R/wrapper_lib.R so this wrapper and wrapper_shell.R share one copy.
msg <- flumen:::wrapper_receive_task_message(s)

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

## local() isolates parse+eval in a temporary environment so top-level
## assignments in user code (e.g., helper functions defined before the
## main function) do not collide with wrapper-internal variables
## (par_list, task_id, msg, s, ...). The returned closure retains access
## to loaded packages and globalenv() via lexical scoping.
## tryCatch: parse() and eval() can both fail (syntax errors, undefined
## symbols). Without this catch the wrapper would die without
## notifying the supervisor, leaving the workflow hung.
func <- tryCatch(
  local(eval(parse(text = func_code))),
  error = function(e){
    err_msg <- paste0("Failed to parse/evaluate user code: ",
                      conditionMessage(e))
    to_log(paste0("[WR_ERR] ", err_msg), task_id)
    flumen:::send_error_message(task_id, s, chunk_size, err_msg)
    close(s)
    quit(status = 1)
  }
)

## The body's VALUE is the function flumen calls, so a trailing statement
## silently becomes that value and the function is lost. Writing one is the
## natural thing to do in R -- define g, then use it -- and it produced
## R's bare "attempt to apply non-function", which names no task, no
## parameter and no cause. Say what happened instead.
##
## Python does not need this: _find_main_func in wrapper.py picks the last
## top-level def, so trailing statements there are harmless. The asymmetry
## is in the languages, not in flumen, which is why the message spells the
## R rule out rather than pointing at a section.
if(!is.function(func)){
  err_msg <- paste0(
    ## No id here: `task_id` in this process is the exe_id, which names
    ## nothing a user wrote, and the error envelope
    ## around this message already carries the real task name.
    "the task's exec body must END with the function to call, ",
    "but its last expression produced ",
    if(is.null(func)) "NULL" else paste0("an object of class '", paste(class(func), collapse = "/"), "'"),
    ".\n  A statement after the definition becomes the body's value, e.g.\n",
    "      g <- function(xs) paste(unlist(xs))\n",
    "      saveRDS(g, 'out.Rds')      # <- this is what flumen received\n",
    "  Move it INSIDE the function:\n",
    "      g <- function(xs) { res <- paste(unlist(xs)); saveRDS(res, 'out.Rds') }")
  to_log(paste0("[WR_ERR] ", err_msg), task_id)
  flumen:::send_error_message(task_id, s, chunk_size, err_msg)
  close(s)
  quit(status = 1)
}

if(length(msg) > flumen:::N_FIXED_DISPATCH_FIELDS){
  par_list <- flumen:::aggregate_params(msg[-seq_len(flumen:::N_FIXED_DISPATCH_FIELDS)],
                                        forms = declared$params_form)
}else{
  par_list <- list()
}

par_list_names <- names(par_list)

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

par_list <- lapply(seq_along(par_list), function(i){

  par <- par_list[[i]]
  new_par <- c()

  if("file_path" %in% class(par)){

    dyn_wild <- flumen:::find_dynamic_wildcards(par)
    suffix <- flumen:::extract_suffix_contents(par)
    dyn_wild <- setdiff(dyn_wild, suffix)

    ## input param
    if(!is_output_par[i]){

      if(!is.null(dyn_wild)){
        dir_names <- dirname(par[dyn_wild])
        file_matched <- flumen:::extract_multiple_wildcard_values(basename(par[dyn_wild]), list.files(dirname(par[dyn_wild])))$matches
        new_par <- c(new_par, file.path(dir_names, file_matched))
      }

    ## output param
    }else{

      if(!is.null(suffix$indices)){
        new_par <- c(new_par, suffix$values)
      }
    }

    other <- setdiff(seq_along(par), c(dyn_wild, suffix$indices))

    if(length(other)){
      new_par <- c(new_par, par[other])
    }

    ## An `_aggr` is a gather, and a gather always arrives as a list -- the
    ## same contract object gathers already honour. A STANDALONE
    ## new_file_aggr over a static wildcard reaches here as ONE atomic
    ## character vector of N already-concrete paths, and the c() above
    ## flattens it, so the body received a character vector where the
    ## documented (and dep-wrapped) form gives a list. Only the shape
    ## differed: the paths are identical. Restricted to INPUT parameters
    ## carrying the "aggregation" marker, so a plain new_file() or a
    ## new_file_exp() single path stays the bare character it must be.
    if(!is_output_par[i] && "aggregation" %in% class(par)){
      new_par <- as.list(new_par)
    }

    new_par

  }else{
    par
  }

})

names(par_list) <- par_list_names

## e.g., when only dynamic wildcard is present without prefix defined
par_list <- par_list[!sapply(par_list, is.null)]


param_to_remove <- which(is_output_par)

if(length(param_to_remove)){
  par_list <- par_list[-param_to_remove]
}

## Strip flumen's marker classes (FLUMEN_INTERNAL_CLASSES below) before handing
## parameters to the user function: they sit in FRONT of the declared type --
## new_object_exp("{s}") is expansion/flumen_object/character -- so class(x)[1]
## and S3 dispatch see a flumen internal. User classes (Seurat, data.frame) stay.
##
## Recursion catches a list-of-paths (multi-file _aggr+dep) whose outer list is
## unclassed but whose character elements carry "file_path". CRITICAL: recurse
## only into BARE lists -- is.list(data.frame) is TRUE, so a naive recursion
## would walk the columns and destroy the data.frame class.
##
## "path" is stripped beside "flumen_path" because grid -- base R, attached by
## Matrix, ggplot2, lattice and therefore Seurat -- defines a class of that name:
## a flumen path carrying it dispatches into print.path or as.character.path and
## dies on "$ operator is invalid for atomic vectors".
FLUMEN_INTERNAL_CLASSES <- c("expansion", "aggregation", "partial_aggregation",
                              "flumen_object", "object", "file_object", "file_path",
                              "flumen_path", "path")
strip_flumen_classes <- function(x){
  cls <- class(x)
  keep <- setdiff(cls, FLUMEN_INTERNAL_CLASSES)
  if(!identical(keep, cls)){
    if(length(keep) == 0L) x <- unclass(x)
    else class(x) <- keep
  }
  if(is.list(x) && identical(class(x), "list")){
    return(lapply(x, strip_flumen_classes))
  }
  x
}
par_list <- lapply(par_list, strip_flumen_classes)

##############################################################################


## Run user code under tryCatch: on error, notify supervisor, close socket, exit.
## The supervisor's "error" handler propagates it to the scheduler, which broadcasts
## terminate and makes run_workflow() stop().
result <- tryCatch({
  ## flumen guard: an input still carrying a {{...}} template means a
  ## dependency did not resolve (e.g. a new_*_aggr gather over a
  ## dynamic-wildcard output that couldn't be matched to a producer).
  ## Fail loud instead of passing the literal template to the user function.
  .flumen_unresolved <- function(x){
    if(is.character(x)) return(any(grepl("\\{\\{[^}]+\\}\\}", x)))
    if(is.list(x) && identical(class(x), "list"))
      return(any(vapply(x, .flumen_unresolved, logical(1))))
    FALSE
  }
  for(.pn in names(par_list)){
    if(.flumen_unresolved(par_list[[.pn]]))
      stop(sprintf(paste0("input '%s' still contains an unresolved template ({{...}}). ",
                          "Fix: align the dep pattern with the producer's output."), .pn))
  }
  ## Invoke the user function WITHOUT do.call(): do.call() splices each
  ## argument into the constructed call BY VALUE, which raises every
  ## (potentially large) argument's reference count to NAMED=2. The first
  ## in-place modification inside the user code -- e.g. Seurat's
  ## ScaleData()/IntegrateData() writing into a 180MB object -- then
  ## triggers a FULL DEEP COPY of that object, repeated across the
  ## computation. The cost scales with object size: ~3x on small inputs,
  ## ~8x+ (minutes) on real single-cell objects. Binding the arguments as
  ## symbols in a dedicated environment and eval()-ing a hand-built call
  ## preserves normal refcount semantics, so in-place updates stay
  ## copy-free.
  local({
    arg_env  <- new.env(parent = emptyenv())
    arg_syms <- vector("list", length(par_list))
    for(.i in seq_along(par_list)){
      .nm <- paste0(".flarg_", .i)
      assign(.nm, par_list[[.i]], envir = arg_env)
      arg_syms[[.i]] <- as.name(.nm)
    }
    names(arg_syms) <- names(par_list)
    eval(as.call(c(list(func), arg_syms)), envir = arg_env)
  })
},
  error = function(e){
    to_log(paste0("[WR_ERR] user function error: ", conditionMessage(e)), task_id)
    flumen:::send_error_message(task_id, s, chunk_size, conditionMessage(e))
    close(s)
    quit(status = 1)
  }
)

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
}

## An output parameter with dynamic wildcard
n_outputs <- flumen:::wrapper_n_outputs(out_dyn_par, result,
                               if(length(out_files)) data_type else NULL,
                               n_object_outputs = length(out_object_par_names))

## Dynamic OBJECT output produced nothing: fail loud at the producer with a
## clear message, mirroring the dynamic-FILE guard (bug#2). Without this an
## empty list falls into the message-assembly block below and dies with a
## cryptic "argument is not interpretable as logical" (a zero-length to_arrow
## in an if()); a consumer that declared a dependency on this output cannot
## run meaningfully without it either.
if(!is.null(out_dyn_par) && "list" %in% class(result) && length(result) == 0L){
  err_msg <- sprintf(
    "instance '%s' declared a dynamic object output ({{...}}) but produced no output objects (empty result). Verify the task returns at least one element.",
    task_id)
  to_log(paste0("[WR_ERR] ", err_msg), task_id)
  flumen:::send_error_message(task_id, s, chunk_size, err_msg)
  close(s)
  quit(status = 1)
}

## Several STATIC object outputs: the return must be a list named by them, one
## element per declared output, because the consumers select their output by
## that name. Refused here, in the task's own name, rather than at a consumer's
## delivery under a supervisor error.
if(!length(out_files) && is.null(out_dyn_par) && length(out_object_par_names) > 1L &&
   !("list" %in% class(result) && !is.null(names(result)) &&
     setequal(names(result), out_object_par_names) &&
     length(result) == length(out_object_par_names))){
  err_msg <- sprintf(paste0(
    "instance '%s' declares %d object outputs (%s), so it must return a list named ",
    "by them, one element each, e.g. list(%s); it returned %s"),
    task_id, length(out_object_par_names), paste(out_object_par_names, collapse = ", "),
    paste0(out_object_par_names, " = ...", collapse = ", "),
    if(!("list" %in% class(result))) paste0("a ", class(result)[1])
    else if(is.null(names(result))) "an unnamed list"
    else paste0("a list named ", paste(names(result), collapse = ", ")))
  to_log(paste0("[WR_ERR] ", err_msg), task_id)
  flumen:::send_error_message(task_id, s, chunk_size, err_msg)
  close(s)
  quit(status = 1)
}

if(!length(out_files)){
  if("list" %in% class(result)){
    data <- result
    ## A raw element travels VERBATIM, exactly as a bare raw result does.
    ## qs2-serialising it here would expect the consumer to deserialise it
    ## back, and the consumer does not: `raw` routes to the pass-through in
    ## R/messaging.R (and to bytes() in objects.py), by contract, because raw
    ## means "the user's bytes". Nobody undoes the wrapping, so the envelope
    ## arrives as the payload: 5 bytes in, 73 out, silently, to R consumers as
    ## much as to Python ones. Bare bytes cannot be mistaken for a qs2 payload
    ## either, because per-element type restoration in supervisor_lib.R delivers
    ## the type as `raw`, never as `wrapped`.
    types       <- sapply(data, flumen:::get_R_type)
    use_arrow_v <- sapply(types, flumen:::is_arrow_compatible_type)
    ## "wrapped:X" preserves the "was a list" marker + the element's type.
    ## The type is stated even when Arrow cannot carry it and the element
    ## travels as qs2. An element whose type is UNNAMED is one nobody
    ## downstream can forward: the consumer's supervisor would have to decode
    ## it just to learn what it was, and encode it again. Naming the type costs
    ## one string and lets the bytes travel untouched: an R consumer reads
    ## is_arrow=FALSE and qs2-decodes them, and a Python consumer is still
    ## refused by the guard in supervisor_lib.R, which has a type to judge only
    ## because it was named here.
    ##
    ## The bare "wrapped" sentinel is never emitted. It sits one character from
    ## "wrapped:raw", which means the opposite -- the user's OWN bytes, handed
    ## over undecoded -- and collapsing the two delivers a qs2 envelope as if it
    ## were the payload, silently. A `raw` element still
    ## yields exactly "wrapped:raw" here, which is the type serialize_data
    ## states for the sentinel, so the two agree.
    data_type   <- paste0("wrapped:", types)
  }else{
    data <- list(result)
    type_single  <- flumen:::get_R_type(result)
    arrow_single <- flumen:::is_arrow_compatible_type(type_single)
    data_type    <- if(arrow_single) type_single else class(result)
  }
}else{
  data <- result
}

## Deliberately no debug dump of `result` here: it is never read back by the
## data plane, and one write per R task would cost disk and litter the cwd.

## Arrow IPC for Arrow-compatible types, qs2 for the rest. The names of a
## user-returned named list are preserved.
to_arrow <- if(!length(out_files)){ if("list" %in% class(result)) use_arrow_v else arrow_single } else rep(FALSE, length(data))
result_data_names <- if(!length(out_files) && "list" %in% class(result) && !is.null(names(result))) names(result) else NULL
## data_names is the GROUPING slot: which declared output each result belongs
## to. For file outputs it is one element of owner_new per collected file, so it
## is as long as `data` by construction. For OBJECT outputs the attribution is
## fixed by the declaration, which msg[[8]] carries, and elem_name does NOT
## answer the same question: under a dynamic output the element names are the
## wildcard values, and object results carry no group name of their own.
if(!length(out_files)){
  result_group_names <- flumen:::wrapper_object_output_labels(out_object_par_names,
                                                              length(data),
                                                              result_data_names)
}
flag <- flumen:::wrapper_send_result(task_id, s, chunk_size, data, data_type, dyn_wildcard, n_outputs, to_arrow, data_names = result_group_names, elem_name = result_data_names)

}, error = function(e) flumen:::wrapper_fatal(e, task_id, s, chunk_size))

## Keepalive: wrapper waits for SIGTERM from the supervisor. Sys.sleep(Inf)
## blocks at 0% CPU (vs busy loop while(TRUE){} that saturates a core). SIGTERM
## interrupts the process in either case.
Sys.sleep(Inf)
