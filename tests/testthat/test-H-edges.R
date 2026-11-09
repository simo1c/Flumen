## Group H — Edge cases.
## Boundary inputs that exercise the build pipeline at its corners
## (single-element wildcards, no wildcards at all, par_uuid stability
## across expansion).

test_that("H1 — wildcard with a single value still expands cleanly to one task", {
  w <- new_wildcard_list(sample = make_toy_counts_list(c("only_one")))
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
  wf <- new_workflow(workflow_id = "H1", desc = "n=1",
                     task_list = list(A, B), wildcard_list = w)
  ids <- sapply(wf$task_list, function(op) op$task_id)
  expect_equal(sum(ids == "A"), 1L)
  expect_equal(sum(ids == "B"), 1L)
})

test_that("H2 — workflow with no wildcards at all builds correctly", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("static_output")),
    exec    = new_script("f <- function(x) x")
  )
  wf <- new_workflow(workflow_id = "H2", desc = "no-wc",
                     task_list = list(task), wildcard_list = NULL)
  expect_equal(length(wf$task_list), 1L)
  expect_null(wf$wildcard_list)
})

test_that("H3 — par_uuid is preserved across wildcard expansion (clones inherit template)", {
  w <- new_wildcard_list(sample = make_toy_counts_list(c("s1", "s2", "s3")))
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
  ## Capture template par_uuids BEFORE handing the task_list to new_workflow().
  template_uuids_A <- A$par_uuids
  expect_true(length(template_uuids_A) >= 1L)

  wf <- new_workflow(workflow_id = "H3", desc = "uuid-stability",
                     task_list = list(A, B), wildcard_list = w)
  ## All A clones must carry exactly the same par_uuids as the template.
  A_clones <- wf$task_list[sapply(wf$task_list, function(op) op$task_id == "A")]
  for (clone in A_clones) {
    expect_equal(clone$par_uuids, template_uuids_A)
  }
})

test_that("H4 — workflow_id and desc are stored on the workflow_object", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script("f <- function(x) x")
  )
  wf <- new_workflow(workflow_id = "wf-tag",
                     desc        = "human-readable description",
                     task_list   = list(task))
  expect_equal(wf$workflow_id, "wf-tag")
  expect_equal(wf$desc, "human-readable description")
  expect_s3_class(wf, "workflow_object")
})
