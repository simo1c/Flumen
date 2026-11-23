## Scheduler-state simulators. Used by unit tests in groups C/D/E to exercise
## the scheduler logic (update_scheduler_state, on_task_finished, try_schedule)
## WITHOUT spawning real workers.

#' Build a synthetic sched_state with given dep_map.
#' @param dep_map named list (child_exe_id -> character vector of parent_exe_ids)
#' @param finished_on character map (worker_name -> character vector of finished exe_ids)
make_sched_state <- function(dep_map = list(), finished_on = list()) {
  ## dependents_of derived from dep_map
  dependents_of <- list()
  for (child in names(dep_map)) {
    for (parent in dep_map[[child]]) {
      dependents_of[[parent]] <- c(dependents_of[[parent]], child)
    }
  }
  ## dep_remaining computed from finished_on
  finished_ids <- unlist(finished_on, use.names = FALSE)
  dep_remaining <- setNames(
    lapply(names(dep_map), function(child) {
      parents <- dep_map[[child]]
      length(parents) - sum(parents %in% finished_ids)
    }),
    names(dep_map)
  )
  ## ready_queue = children with dep_remaining == 0 and not yet finished
  ready_queue <- character(0)
  for (child in names(dep_remaining)) {
    if (dep_remaining[[child]] == 0L && !(child %in% finished_ids)) {
      ready_queue <- c(ready_queue, child)
    }
  }

  ## data_to_workers reverse index for finished tasks
  data_to_workers <- new.env(hash = TRUE, parent = emptyenv())
  for (worker in names(finished_on)) {
    for (exe_id in finished_on[[worker]]) {
      existing <- data_to_workers[[exe_id]]
      data_to_workers[[exe_id]] <- unique(c(existing, worker))
    }
  }

  list(
    dep_map         = dep_map,
    dep_remaining   = dep_remaining,
    dependents_of   = dependents_of,
    ready_queue     = ready_queue,
    worker_data     = finished_on,
    data_to_workers = data_to_workers,
    worker_files    = list()
  )
}

#' Quick assertion helper: does sched_state mark `exe_id` as runnable?
is_in_ready_queue <- function(sched_state, exe_id) {
  exe_id %in% sched_state$deps$ready_queue
}

#' Quick lookup of dep_remaining for an exe_id, or NA if absent.
get_dep_remaining <- function(sched_state, exe_id) {
  v <- sched_state$deps$dep_remaining[[exe_id]]
  if (is.null(v)) NA_integer_ else as.integer(v)
}
