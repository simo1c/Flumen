## Group J — run_workflow argument validation.
## All tests target validate_exec_args() and the inline checks in
## run_workflow() so they fail BEFORE any subprocess spawns.
## Nothing executes; nothing binds a port.

make_minimal_wf <- function() {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script("f <- function(x) x")
  )
  new_workflow(workflow_id = "J", desc = "minimal", task_list = list(task))
}

test_that("J1 — empty worker_list is rejected", {
  skip_on_os("windows")   ## validate_exec_args() refuses local workers on Windows first
  wf <- make_minimal_wf()
  expect_error(
    run_workflow(wf, worker_list = list(), chunk_size = 1024L),
    regexp = "at least one worker"
  )
})

test_that("J2 — non-positive chunk_size is rejected", {
  skip_on_os("windows")   ## validate_exec_args() refuses local workers on Windows first
  wf <- make_minimal_wf()
  w  <- new_worker(name = "w1", n_concurrent_task = 1L)
  expect_error(
    run_workflow(wf, worker_list = list(w), chunk_size = 0),
    regexp = "chunk_size"
  )
  expect_error(
    run_workflow(wf, worker_list = list(w), chunk_size = -10),
    regexp = "chunk_size"
  )
})

test_that("J3 — non-numeric chunk_size is rejected", {
  skip_on_os("windows")   ## validate_exec_args() refuses local workers on Windows first
  wf <- make_minimal_wf()
  w  <- new_worker(name = "w1")
  expect_error(
    run_workflow(wf, worker_list = list(w), chunk_size = "big"),
    regexp = "chunk_size"
  )
})

test_that("J4 — op with unsatisfiable requires errors before scheduler start", {
  skip_on_os("windows")   ## validate_exec_args() refuses local workers on Windows first
  ## Worker advertises no launcher (=> no declared resources). Op requires
  ## 999 GPUs. validate_exec_args must reject before any port is opened.
  task <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script("f <- function(x) x"),
    requires = list(gpu = 999L)
  )
  wf <- new_workflow(workflow_id = "J4", desc = "needs-gpu",
                     task_list = list(task))
  w  <- new_worker(name = "w1")
  expect_error(
    run_workflow(wf, worker_list = list(w), chunk_size = 1024L),
    regexp = "requires"
  )
})
