## Workflow construction and execution-plan helpers.
##
## Public new_workflow() entry point, the internal new_exe_object() factory
## used at dispatch time, the DAG topological-level helper, the exec_plan
## container, plus the has_output_par inspection utility.


#' Create a workflow
#'
#' Builds a workflow from a list of tasks, validates the dependency graph
#' (must be a connected DAG), and expands wildcards into concrete execution
#' instances.
#'
#' @param workflow_id Character. Unique identifier for the workflow.
#' @param desc Character. Human-readable description. Required, with no default.
#' @param task_list List of task objects created with [new_R_task()],
#'   [new_python_task()], or [new_shell_task()]. Dependencies defined via
#'   [new_dep()] must form a connected acyclic graph.
#' @param wildcard_list Optional object from [new_wildcard_list()]. Wildcards
#'   are substituted in task inputs/outputs to produce multiple execution
#'   instances. Defaults to `NULL` (no expansion).
#'
#' @return An object of class `workflow_object` with the resolved task list,
#'   dependency DAG, and wildcard list.
#'
#' @seealso [run_workflow()], [new_R_task()], [new_wildcard_list()]
#'
#' @examples
#' \dontrun{
#' w_list <- new_wildcard_list(sample = c("a", "b", "c"))
#'
#' task_A <- new_R_task(
#'   task_id  = "A",
#'   input  = new_input(x = new_object_exp("{sample}")),
#'   output = new_output(out = new_object("{{out}}")),
#'   exec   = new_script('run <- function(x) data.frame(value = x)')
#' )
#'
#' workflow <- new_workflow(
#'   workflow_id   = "example",
#'   desc          = "Minimal example",
#'   task_list       = list(task_A),
#'   wildcard_list = w_list
#' )
#' }
#'
#' @export
new_workflow <- function(workflow_id,
                         desc,
                         task_list,
                         wildcard_list = NULL){

  ## Early syntax validation on the user-provided task_list templates.
  ## Must run BEFORE prepare_workflow_execution() below: expansion keeps the
  ## task_id of every instance and tells them apart by exe_id, so afterwards a
  ## task expanded into N instances reads as N duplicate task_ids and the check
  ## would refuse every workflow that uses a wildcard.
  validate_workflow(task_list, wildcard_list)

  DAG <- create_DAG(task_list)

  if(!is.null(DAG)){
    if(!igraph::is_connected(DAG)){
      stop(flumen_reject(
        call  = "new_workflow()",
        error = "the DAG is disconnected",
        fix   = "link the tasks with new_dep(), or run the parts as separate workflows"),
        call. = FALSE)
    }
    if(!igraph::is_dag(DAG)){
      stop(flumen_reject(
        call  = "new_workflow()",
        error = sprintf("dependency cycle in task_list, involving task_ids: %s",
                        paste(shQuote(igraph::V(DAG)$name), collapse = ", ")),
        fix   = "the graph must be acyclic, check the new_dep() references"), call. = FALSE)
    }
  }else{
    if(length(unique(sapply(task_list, get_object_id))) > 1){
      stop(flumen_reject(
        call  = "new_workflow()",
        error = "the DAG is disconnected",
        fix   = "link the tasks with new_dep(), or run the parts as separate workflows"),
        call. = FALSE)
    }
  }

  ## Aggregate per-param pre-staged flags from raw task_list (pre-expansion).
  ## Indexed by par_uuid: clones produced by wildcard expansion inherit the
  ## template's par_uuid, so a single lookup serves all instances.
  ## Only pre-staged entries are stored (value FALSE); absence implies the
  ## default behaviour (master reads file bytes at dispatch).
  ## The internal "to_transfer" attribute is set by pre_staged() on
  ## the file value; must be read here BEFORE prepare_workflow_execution(),
  ## because the expansion engine in object_func.R replaces par$value with
  ## the resolved character vector and the original attribute is lost.
  par_locations <- list()
  static_file_paths <- character(0)
  for (task in task_list) {
    for (par in task$par_list) {
      ## Internal pre-staged marker (set by pre_staged()): FALSE
      ## means "already on the worker, read in place -- the master must not
      ## ship it". Indexed by par_uuid (inherited by expansion clones).
      to_transfer <- attr(par$value, "to_transfer")
      if (!is.null(to_transfer) && !isTRUE(to_transfer)) {
        par_locations[[par$par_uuid]] <- FALSE
      }
      ## Collect static (wildcard-free) file paths so run_workflow() can report
      ## the Model 2 vs Model 3 classification under shared_path before the run.
      ## Read here (pre-expansion) while the file_path class is still intact.
      if (any(c("file_object", "file_path") %in% class(par$value))) {
        v <- as.character(par$value)
        v <- v[nzchar(v) & !grepl("\\{", v)]
        if (length(v) > 0L) static_file_paths <- c(static_file_paths, v)
      }
    }
  }
  static_file_paths <- unique(static_file_paths)

  work_exe <- prepare_workflow_execution(task_list, DAG, seq_along(task_list), wildcard_list)

  task_list <- work_exe$task_list
  required_shapes <- work_exe$required_shapes

  ## Now that the wildcards are resolved, check the CONCRETE output paths: two
  ## instances landing on one file overwrite each other. The pattern-level
  ## checks in validate_workflow() cannot see this -- the paths only differ
  ## once substituted (R/validation.R).
  validate_no_colliding_resolved_outputs(task_list)
  ## Same directory + overlapping names + free to run together = a task can
  ## claim another's files (R/validation.R). Needs the DAG, so it runs here.
  validate_no_overlapping_output_globs(work_exe$task_list, DAG)

  workflow_object <- list(
    workflow_id = workflow_id,
    desc = desc,
    task_list = task_list,
    DAG = DAG,
    required_shapes = required_shapes,
    wildcard_list = wildcard_list,
    par_locations = par_locations,
    static_file_paths = static_file_paths
  )

  class(workflow_object) <- "workflow_object"
  return(workflow_object)
}


new_exe_object <- function(task_id,
                           exe_id,
                           cmd,
                           shell_interpreter,
                           type,
                           user_code,
                           func_call,
                           par_list,
                           par_names,
                           par_uuids,
                           params_type,
                           params_is_aggr = NULL,
                           output_files,
                           output_auto_mkdir = TRUE,
                           output_par_names = "",
                           output_object_par_names = "",
                           is_output_par,
                           dyn_wildcard){
  ## params_is_aggr: per-param logical (uuid-named, parallel to params_type)
  ## marking declared aggregation (gather) params. Used at delivery-prep to
  ## keep a 1-element gather a list while a single non-aggr object is bare.
  if(is.null(params_is_aggr) || length(params_is_aggr) != length(params_type)){
    params_is_aggr <- setNames(rep(FALSE, length(params_type)), names(params_type))
  }
  exe_object <- list(
    task_id = task_id,
    exe_id = exe_id,
    cmd = cmd,
    shell_interpreter = shell_interpreter,
    type = type,
    user_code = user_code,
    func_call = func_call,
    par_list = par_list,
    par_names = par_names,
    par_uuids = par_uuids,
    params_type = params_type,
    params_is_aggr = params_is_aggr,
    params_is_arrow = setNames(rep(FALSE, length(params_type)), names(params_type)),
    output_files = output_files,
    output_auto_mkdir = output_auto_mkdir,
    output_par_names = output_par_names,
    output_object_par_names = output_object_par_names,
    is_output_par = is_output_par,
    dyn_wildcard = dyn_wildcard,
    file_transfer = list()
  )

  switch(type,
         shell={
           class <- "shell_exe_object"
         },
         R={
           class <- "R_exe_object"
         },
         python={
           class <- "python_exe_object"
         }
  )

  class(exe_object) <- c(class, "exe_object")
  return(exe_object)
}

get_parallel_levels_from_DAG <- function(DAG,
                                         sequential = FALSE) {

  par_levels <- list("")
  topo_order <- igraph::topo_sort(DAG)
  cur_level <- 1

  for(i in seq_along(topo_order)){

    v <- topo_order[i]
    neig <- igraph::neighbors(DAG, v, mode = "in")$name

    if(!length(neig) | !any(neig %in% par_levels[[cur_level]])){
      if(length(par_levels[par_levels != ""])){
        par_levels[[cur_level]] <- c(par_levels[[cur_level]], v$name)
      }else{
        par_levels <- list(v$name)
      }
    }else{
      cur_level <- cur_level + 1
      par_levels <- c(par_levels, list(v$name))
    }
  }

  levels <- lapply(seq_along(par_levels), function(i){
    rep(i, length(par_levels[[i]]))
  })

  terminal_nodes <- par_levels[[length(par_levels)]]

  if(sequential){
    par_levels <- as.list(unlist(par_levels))
    levels <- as.list(unlist(levels))
  }else{
    levels <- lapply(levels, "[[", 1)
  }

  parallel_levels <- list(
    par_levels = par_levels,
    levels = levels,
    terminal_nodes = terminal_nodes
  )

  return(parallel_levels)
}

new_exec_plan <- function(DAG,
                          worker_list = NULL){

  if(is.null(worker_list)){
    par_levels_ob <- get_parallel_levels_from_DAG(DAG, TRUE)
    w <- new_worker(name = "worker1")
    worker_list <- list(w)
  }else{
    par_levels_ob <- get_parallel_levels_from_DAG(DAG, FALSE)
  }

  par_levels <- par_levels_ob$par_levels
  levels <- par_levels_ob$levels

  if(!is.null(worker_list)){
    names_workers <- sapply(worker_list, function(worker){
      return(worker$name)
    })
    names(worker_list) <- names_workers
  }

  exec_plan <- list(
    parallel_levels_exec = par_levels,
    levels = levels,
    worker_list = worker_list
  )

  class(exec_plan) <- "exec_plan_object"
  return(exec_plan)
}


## -- coercion / inspection helpers --------------------------------------------

has_output_par <- function(object){

  child <- get_child_objects(object)
  last_child <- child[[length(child)]]

  if(class(last_child)[1] == "pipe_object"){
    return(has_output_par(last_child))
  }else{
    par_list_obejct <- get_child_objects(last_child)
    par_object <- par_list_obejct[[length(par_list_obejct)]]
    return(par_object$is_output)
  }
}
