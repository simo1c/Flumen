## Regression: a run_workflow(shared_path=) input shared across many tasks must
## NOT enter the in_transit machinery -- the exact twin of the pre_staged ghost
## (see test-prestaged-intransit-starvation.R), for the shared-filesystem
## read-in-place mode.
##
## Bug: a file under a declared shared_path root is read in place by the worker
## (execute_single_task downgrades dtype to "file_path" -> no byte transfer),
## yet the dispatch still marked it in_transit because get_task_input_file_paths
## was shared_path-blind. No transfer -> no file_received -> permanent ghost.
## When that path is shared by many tasks (e.g. an NFS/Lustre reference genome
## used by every per-sample task), select_worker then excludes that worker from
## EVERY later task that needs the same path -> the worker starves.
##
## Root: flumen:::get_task_input_file_paths() feeds BOTH the in_transit marking and the
## select_worker exclusion filter; it must receive shared_path and skip params
## whose paths all lie under a shared root, exactly as it skips pre_staged
## params (par_locations[[par_uuid]] == FALSE) and outputs.

## Minimal exe-object task: one file input (class "file_path"), no deps.
mk_file_input_op <- function(exe_id, path, uuid) {
  list(
    exe_id   = exe_id,
    par_list = list(
      list(value = structure(path, class = "file_path"), par_uuid = uuid)
    )
  )
}

test_that("BUG is real: a shared_path input is tracked and starves a ghosted worker", {
  op <- mk_file_input_op("count_5", "/nfs/ref/genome", "u_ref")

  ## shared_path-blind: the path is tracked (this inclusion is exactly what
  ## marks a never-transferred shared file in_transit -> the ghost).
  expect_equal(flumen:::get_task_input_file_paths(op), "/nfs/ref/genome")

  ## select_worker excludes a worker that has that path ghosted in in_transit,
  ## even though the worker is free -> NA -> the dispatch loop leaves it idle.
  ss   <- make_sched_state(dep_map = list(), finished_on = list())
  ss$worker_files <- list(remote2 = list(in_transit = "/nfs/ref/genome",
                                         completed  = character(0)))
  fifo <- data.frame(Supervisor_ID = "remote2", stringsAsFactors = FALSE)
  wl   <- list(list(name = "remote2", launcher = NULL))
  expect_true(is.na(flumen:::select_worker(op, ss, fifo, wl)))
})

test_that("FIX: shared_path inputs are excluded from in_transit tracking", {
  op <- mk_file_input_op("count_5", "/nfs/ref/genome", "u_ref")
  sp <- "/nfs"   ## declared shared-filesystem root

  ## a file under the shared root is not tracked for transfer -> never marked
  ## in_transit, never used to exclude a worker
  expect_equal(flumen:::get_task_input_file_paths(op, NULL, sp), character(0))

  ## a file NOT under any shared root is still tracked normally
  expect_equal(flumen:::get_task_input_file_paths(op, NULL, "/other"), "/nfs/ref/genome")

  ## with shared_path, the ghosted-but-free worker is chosen, not starved
  ss   <- make_sched_state(dep_map = list(), finished_on = list())
  ss$worker_files <- list(remote2 = list(in_transit = "/nfs/ref/genome",
                                         completed  = character(0)))
  fifo <- data.frame(Supervisor_ID = "remote2", stringsAsFactors = FALSE)
  wl   <- list(list(name = "remote2", launcher = NULL))
  idx  <- flumen:::select_worker(op, ss, fifo, wl, NULL, sp)
  expect_false(is.na(idx))
  expect_equal(fifo$Supervisor_ID[idx], "remote2")
})
