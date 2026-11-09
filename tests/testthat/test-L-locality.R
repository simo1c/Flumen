## Group L — Resource declarations and locality validation.
## All tests are pure-R unit checks: no scheduler, no worker spawn.
## They cover the path requires -> validate_resources (in new_*_task)
## and the early rejection in validate_exec_args() before run_workflow
## reaches the network setup phase.

test_that("L1 — new_R_task accepts a valid `requires` list", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script("f <- function(x) x"),
    requires = list(cpu = 4L, mem = "32G")
  )
  expect_equal(task$requires$cpu, 4L)
  expect_equal(task$requires$mem, "32G")
})

test_that("L2 — `requires` with an unknown field is rejected at task build", {
  expect_error(
    new_R_task(
      task_id = "A",
      input   = new_input(x = new_object("v")),
      output  = new_output(out = new_object("{{out}}")),
      exec    = new_script("f <- function(x) x"),
      requires = list(disk = "1T")
    ),
    regexp = "disk"
  )
})

test_that("L3 — `requires` with a malformed memory string is rejected", {
  expect_error(
    new_R_task(
      task_id = "A",
      input   = new_input(x = new_object("v")),
      output  = new_output(out = new_object("{{out}}")),
      exec    = new_script("f <- function(x) x"),
      requires = list(mem = "lots-of-RAM")
    ),
    regexp = "mem"
  )
})
