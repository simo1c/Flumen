## Regression: a pre_staged() input shared across many tasks must NOT enter the
## in_transit machinery.
##
## A remote worker's first dispatch marks the task's input files in_transit
## (scheduler_dispatch.R), but a pre_staged file is NEVER transferred, so no
## file_received would clear it and the ghost is permanent. On a path shared by
## many tasks (a pre_staged reference genome used by every per-sample `count`)
## select_worker would then exclude that worker from EVERY later task needing it
## -> the worker starves while ready tasks exist.
##
## Root: flumen:::get_task_input_file_paths() feeds BOTH the in_transit marking and the
## select_worker exclusion filter; it must skip pre_staged params (those with
## par_locations[[par_uuid]] == FALSE), exactly as it already skips outputs.

## Minimal exe-object task: one file input (class "file_path"), no deps.
mk_file_input_op <- function(exe_id, path, uuid) {
  list(
    exe_id   = exe_id,
    par_list = list(
      list(value = structure(path, class = "file_path"), par_uuid = uuid)
    )
  )
}

test_that("BUG is real: a pre_staged path is tracked and starves a ghosted worker", {
  ## get_task_input_file_paths tracks the input path (this inclusion is exactly
  ## what marks a never-transferred pre_staged file in_transit -> the ghost).
  op <- mk_file_input_op("count_5", "ref/genome", "u_ref")
  expect_equal(flumen:::get_task_input_file_paths(op), "ref/genome")

  ## select_worker excludes a worker that has that path ghosted in in_transit,
  ## even though the worker is free -> NA -> the dispatch loop leaves it idle.
  ss   <- make_sched_state(dep_map = list(), finished_on = list())
  ss$worker_files <- list(remote2 = list(in_transit = "ref/genome",
                                         completed  = character(0)))
  fifo <- data.frame(Supervisor_ID = "remote2", stringsAsFactors = FALSE)
  wl   <- list(list(name = "remote2", launcher = NULL))
  expect_true(is.na(flumen:::select_worker(op, ss, fifo, wl)))
})

test_that("FIX: pre_staged inputs are excluded from in_transit tracking", {
  op  <- mk_file_input_op("count_5", "ref/genome", "u_ref")
  pl  <- list(u_ref = FALSE)   ## par_locations: u_ref is pre_staged

  ## not tracked for transfer -> never marked in_transit, never used to exclude
  expect_equal(flumen:::get_task_input_file_paths(op, pl), character(0))

  ## a normal (non pre_staged) input is still tracked
  expect_equal(flumen:::get_task_input_file_paths(op, list(other = FALSE)), "ref/genome")

  ## with the pre_staged flag, the ghosted-but-free worker is chosen, not starved
  ss   <- make_sched_state(dep_map = list(), finished_on = list())
  ss$worker_files <- list(remote2 = list(in_transit = "ref/genome",
                                         completed  = character(0)))
  fifo <- data.frame(Supervisor_ID = "remote2", stringsAsFactors = FALSE)
  wl   <- list(list(name = "remote2", launcher = NULL))
  idx  <- flumen:::select_worker(op, ss, fifo, wl, pl)
  expect_false(is.na(idx))
  expect_equal(fifo$Supervisor_ID[idx], "remote2")
})
