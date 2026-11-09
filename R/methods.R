## Flumen S3 generics and methods.
##
## All generics dispatched via UseMethod() and their methods, plus the
## print/plot methods inherited from base. The constructors live in R/task.R,
## R/par.R and R/workflow.R. This file only contains the dispatch surface.
##
## print dispatches via base::print (already a generic). Flumen does not
## redefine the print/debug/as generics, to avoid masking base and methods.


## -- get_object_id ------------------------------------------------------------

get_object_id <- function(x){
  UseMethod("get_object_id")
}

#' @export
#' @noRd
get_object_id.task_object <- function(x){
  return(x$task_id)
}

#' @export
#' @noRd
get_object_id.workflow_object <- function(x){
  return(x$workflow_id)
}


## -- get_name_from_object -----------------------------------------------------

get_name_from_object <- function(x){
  UseMethod("get_name_from_object")
}

#' @export
#' @noRd
get_name_from_object.par_object <- function(x){
  return(x$name)
}


## -- get_type_from_object -----------------------------------------------------

get_type_from_object <- function(x){
  UseMethod("get_type_from_object")
}

#' @export
#' @noRd
get_type_from_object.task_object <- function(x){
  return(x$type)
}

#' @export
#' @noRd
get_type_from_object.exe_object <- function(x){
  return(x$type)
}


## -- get_cmd_from_object ------------------------------------------------------

get_cmd_from_object <- function(x){
  UseMethod("get_cmd_from_object")
}

#' @export
#' @noRd
get_cmd_from_object.task_object <- function(x){
  return(x$exec)
}

#' @export
#' @noRd
get_cmd_from_object.exe_object <- function(x){
  return(x$cmd)
}


## -- get_child_objects --------------------------------------------------------

get_child_objects <- function(x){
  UseMethod("get_child_objects")
}

#' @export
#' @noRd
get_child_objects.task_object <- function(x){
  return(x$par_list)
}

#' @export
#' @noRd
get_child_objects.workflow_object <- function(x){
  return(x$task_list)
}


## -- get_value_from_object ----------------------------------------------------

get_value_from_object <- function(x){
  UseMethod("get_value_from_object")
}

#' @export
#' @noRd
get_value_from_object.par_object <- function(x){
  return(x$value)
}


## -- print methods (base::print is already a generic) -------------------------

#' @export
#' @noRd
print.par_object <- function(x, ...){
  to_print <- "parameter object"
  cat(to_print)
}

#' @export
#' @noRd
print.task_object <- function(x, ...){

  param_names <- sapply(x$par_list, function(par){
    par$name
  })

  n_params <- length(unique(param_names))

  to_print <- paste0("operation with ", n_params, " parameters")
  cat(to_print)
}

#' @export
#' @noRd
print.task_list_object <- function(x, ...){

  param_names <- sapply(x, function(par){
    par$name
  })

  n_params <- length(unique(param_names))

  to_print <- paste0("list with ", n_params, " operations")
  cat(to_print)
}

#' @export
#' @noRd
print.workflow_object <- function(x, ...){

  task_list <- x$task_list

  task_ids <- lapply(task_list, function(op){
    get_object_id(op)
  })

  n_op <- length(unique(task_ids))

  n_op_inst <- length(task_list)
  to_print <- paste0("workflow with ", n_op, " operations and ", n_op_inst, " instances")
  cat(to_print, "\n", sep = "")
  invisible(x)
}

#' @export
#' @noRd
print.exec_plan_object <- function(x, ...){

  workers <- sapply(x$worker_list, function(worker){
    return(worker$name)
  })

  to_print <- paste0("execution plan with ", length(unique(workers)), " workers")
  cat(to_print)
}


## -- plot methods (graphics::plot is already a generic) -----------------------

#' @export
#' @noRd
plot.workflow_object <- function(x, ...){
  plot(x$DAG)
}


## -- inspection --------------------------------------------------------------

#' Print the resolved commands of a workflow
#'
#' Pretty-prints, for each task in a workflow, the command (R script,
#' Python script, or shell command) together with the bound parameter values.
#' Useful for inspecting how parameters and wildcards have been resolved
#' before execution.
#'
#' @param workflow_obj A `workflow_object` created with [new_workflow()].
#'
#' @return Invisibly returns a list of character vectors (one per task),
#'   each containing the formatted command lines. Called for its side effect
#'   of printing to the console.
#'
#' @seealso [new_workflow()], [run_workflow()]
#'
#' @examples
#' \dontrun{
#' print_workflow_commands(my_workflow)
#' }
#' @export
print_workflow_commands <- function(workflow_obj) {

  if (!inherits(workflow_obj, "workflow_object") || !("task_list" %in% names(workflow_obj))) {
    stop(flumen_reject(
      call  = "print_workflow_commands()",
      error = "the object is not a valid workflow_object, or has no task_list",
      fix   = "build it with new_workflow()"), call. = FALSE)
  }

  ## Wildcard expansion leaves task_list in the order instances were created,
  ## which interleaves tasks and makes a many-instance workflow hard to read.
  ## Group by task_id, and inside a task order by the numeric suffix of exe_id
  ## ("..._2" before "..._10"), falling back to the original order when there
  ## is no suffix. order() is stable, so equal keys keep their relative order.
  task_list <- workflow_obj$task_list
  if (length(task_list) > 1L) {
    ids  <- vapply(task_list, function(op) as.character(op$task_id)[1], character(1))
    tail_num <- suppressWarnings(as.numeric(
      sub(".*_([0-9]+)$", "\\1", vapply(task_list, function(op)
        as.character(op$exe_id)[1], character(1)))))
    tail_num[is.na(tail_num)] <- 0
    task_list <- task_list[order(ids, tail_num)]
  }

  output_strings <- lapply(task_list, function(op_obj) {

    task_id <- op_obj$task_id
    exe_id <- op_obj$exe_id
    cmd_base <- op_obj$exec

    output_lines <- c(paste0("task_id: '", task_id, "'"),
                      paste0("exe_id: '", exe_id, "'"),
                      "")

    output_lines <- c(output_lines, cmd_base)

    if ("par_list" %in% names(op_obj)) {

      par_list <- op_obj$par_list

      params <- sapply(par_list, function(par_obj) {

        par_name <- par_obj$name
        par_value <- par_obj$value

        if (is.list(par_value) && inherits(par_value, "file_path")) {
          par_value <- par_value[[1]]
        } else if (is.list(par_value) && length(par_value) > 0 && is.character(par_value[[1]])) {
          par_value <- par_value[[1]]
        } else if (is.null(par_value)) {
          par_value <- ""
        } else if (!is.character(par_value)) {
          par_value <- as.character(par_value)
        }

        if(!is.null(par_name)){
          return(paste0("\t", par_name, " ", par_value))
        } else{
          return(paste0("\t", par_value))
        }
      })

      params <- params[!is.na(params)]

      output_lines <- c(output_lines, params)
    }

    return(paste(output_lines, collapse = "\n"))
  })

  ## stdout, not message(): this is requested output, not a diagnostic, and on
  ## stderr it could not be captured with capture.output() or redirected.
  cat(paste(unlist(output_strings), collapse = "\n\n###\n\n"), "\n", sep = "")

  invisible(output_strings)
}
