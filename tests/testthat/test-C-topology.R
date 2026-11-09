## Group C — DAG topology (build-time, no execution).
## Validates that flumen builds the right number of nodes and the right
## adjacency structure for canonical DAG shapes, plus rejects malformed
## graphs (cycles, disconnection).

test_that("C1 — single task with no wildcards yields one instance", {
  A <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(y = new_object("{{out}}")),
    exec    = new_script("f <- function(x) x")
  )
  wf <- new_workflow(workflow_id = "C1", desc = "single", task_list = list(A))
  expect_equal(length(wf$task_list), 1L)
  expect_equal(wf$task_list[[1]]$task_id, "A")
})

test_that("C2 — linear A->B preserves edge after expansion (2 samples)", {
  w <- new_wildcard_list(sample = make_toy_counts_list(c("s1", "s2")))
  A <- new_R_task(
    task_id = "A",
    input   = new_input(df = new_object_exp("{sample}")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(df) df")
  )
  B <- new_R_task(
    task_id = "B",
    input   = new_input(x = new_dep(task_id = "A", value = new_object("{{out_A}}"))),
    output  = new_output(out = new_object("{{out_B}}")),
    exec    = new_script("g <- function(x) x")
  )
  wf <- new_workflow(workflow_id = "C2", desc = "linear",
                     task_list = list(A, B), wildcard_list = w)
  ids <- sapply(wf$task_list, function(op) op$task_id)
  expect_equal(sum(ids == "A"), 2L)
  expect_equal(sum(ids == "B"), 2L)
})

test_that("C3 — fan-in via aggregation collapses N producers to 1 consumer", {
  wf <- make_fan_in_wf(c("s1", "s2", "s3"))
  ids <- sapply(wf$task_list, function(op) op$task_id)
  expect_equal(sum(ids == "A"), 3L)
  expect_equal(sum(ids == "C"), 1L)
})

test_that("C4 — cycle in dependency graph is rejected at build time", {
  ## A depends on B, B depends on A => cycle.
  A <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_dep(task_id = "B", value = new_object("{{out_B}}"))),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(x) x")
  )
  B <- new_R_task(
    task_id = "B",
    input   = new_input(x = new_dep(task_id = "A", value = new_object("{{out_A}}"))),
    output  = new_output(out = new_object("{{out_B}}")),
    exec    = new_script("g <- function(x) x")
  )
  expect_error(
    new_workflow(workflow_id = "C4", desc = "cycle", task_list = list(A, B)),
    regexp = "cycle"
  )
})

test_that("C5 — disconnected task graph is rejected at build time", {
  A <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("a")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(x) x")
  )
  B <- new_R_task(
    task_id = "B",
    input   = new_input(x = new_object("b")),
    output  = new_output(out = new_object("{{out_B}}")),
    exec    = new_script("g <- function(x) x")
  )
  expect_error(
    new_workflow(workflow_id = "C5", desc = "disconnected", task_list = list(A, B)),
    regexp = "disconnected"
  )
})

test_that("C6 — diamond A->B,A->C,B->D,C->D builds without error", {
  A <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(x) x")
  )
  B <- new_R_task(
    task_id = "B",
    input   = new_input(x = new_dep(task_id = "A", value = new_object("{{out_A}}"))),
    output  = new_output(out = new_object("{{out_B}}")),
    exec    = new_script("g <- function(x) x")
  )
  C <- new_R_task(
    task_id = "C",
    input   = new_input(x = new_dep(task_id = "A", value = new_object("{{out_A}}"))),
    output  = new_output(out = new_object("{{out_C}}")),
    exec    = new_script("h <- function(x) x")
  )
  D <- new_R_task(
    task_id = "D",
    input   = new_input(
      b = new_dep(task_id = "B", value = new_object("{{out_B}}")),
      c = new_dep(task_id = "C", value = new_object("{{out_C}}"))
    ),
    output  = new_output(out = new_object("{{out_D}}")),
    exec    = new_script("k <- function(b, c) list(b, c)")
  )
  wf <- new_workflow(workflow_id = "C6", desc = "diamond",
                     task_list = list(A, B, C, D))
  ids <- sapply(wf$task_list, function(op) op$task_id)
  expect_setequal(ids, c("A", "B", "C", "D"))
  expect_true(igraph::is_dag(wf$DAG))
})
