## Workflow factories used across the testthat suite.
## Each factory returns a workflow_object ready for run_workflow() calls,
## or builder primitives that individual tests can compose.
##
## Convention: tasks are named A, B, C, ... and outputs are placeholders
## with names matching the task (e.g., out_A) so the resulting graphs are
## easy to read in test failures.

#' Single-task R workflow with one wildcard-driven input.
#' Useful as the simplest happy path for build-time and runtime checks.
make_single_task_wf <- function(sample_names = c("s1")) {
  w <- new_wildcard_list(
    sample = setNames(
      lapply(sample_names, function(s) make_toy_counts(20L, s)),
      sample_names
    )
  )
  A <- new_R_task(
    task_id = "A",
    input   = new_input(df = new_object_exp("{sample}")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("identity_op <- function(df) df")
  )
  new_workflow(
    workflow_id   = "single_task",
    desc          = "single R task",
    task_list     = list(A),
    wildcard_list = w
  )
}

#' Two-task linear DAG (A -> B), one wildcard, R-only.
make_linear_wf <- function(sample_names = c("s1")) {
  w <- new_wildcard_list(
    sample = setNames(
      lapply(sample_names, function(s) make_toy_counts(20L, s)),
      sample_names
    )
  )
  A <- new_R_task(
    task_id = "A",
    input   = new_input(df = new_object_exp("{sample}")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(df) df")
  )
  B <- new_R_task(
    task_id = "B",
    input   = new_input(
      x = new_dep(task_id = "A", value = new_object("{{out_A}}"))
    ),
    output  = new_output(out = new_object("{{out_B}}")),
    exec    = new_script("g <- function(x) nrow(x)")
  )
  new_workflow(
    workflow_id   = "linear",
    desc          = "A -> B",
    task_list     = list(A, B),
    wildcard_list = w
  )
}

#' Fan-out workflow: A spawns N instances, B consumes each instance separately.
make_fan_out_wf <- function(sample_names = c("s1", "s2", "s3")) {
  w <- new_wildcard_list(
    sample = setNames(
      lapply(sample_names, function(s) make_toy_counts(20L, s)),
      sample_names
    )
  )
  A <- new_R_task(
    task_id = "A",
    input   = new_input(df = new_object_exp("{sample}")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(df) df")
  )
  B <- new_R_task(
    task_id = "B",
    input   = new_input(
      x = new_dep(task_id = "A", value = new_object("{{out_A}}"))
    ),
    output  = new_output(out = new_object("{{out_B}}")),
    exec    = new_script("g <- function(x) sum(x$counts)")
  )
  new_workflow(
    workflow_id   = "fan_out",
    desc          = "A_1..A_N -> B_1..B_N",
    task_list     = list(A, B),
    wildcard_list = w
  )
}

#' Fan-in workflow: A spawns N instances, single C aggregates all outputs.
make_fan_in_wf <- function(sample_names = c("s1", "s2", "s3")) {
  w <- new_wildcard_list(
    sample = setNames(
      lapply(sample_names, function(s) make_toy_counts(20L, s)),
      sample_names
    )
  )
  A <- new_R_task(
    task_id = "A",
    input   = new_input(df = new_object_exp("{sample}")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(df) df")
  )
  C <- new_R_task(
    task_id = "C",
    input   = new_input(
      xs = new_dep(task_id = "A", value = new_object_aggr("{{out_A}}"))
    ),
    output  = new_output(out = new_object("{{out_C}}")),
    exec    = new_script("g <- function(xs) length(xs)")
  )
  new_workflow(
    workflow_id   = "fan_in",
    desc          = "A_1..A_N -> C",
    task_list     = list(A, C),
    wildcard_list = w
  )
}

#' Trivial single-worker local config used by integration tests.
make_local_worker <- function(name = "w1", n_concurrent_task = 2L) {
  new_worker(name = name, n_concurrent_task = n_concurrent_task)
}
