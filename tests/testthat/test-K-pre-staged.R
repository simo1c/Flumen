## Group K — pre_staged() wrapper and par_locations bookkeeping. Verifies that
## a pre-staged file input (wrapped with pre_staged()) is honoured both at
## validation time (warning suppression) and at workflow construction time
## (par_locations map indexed by par_uuid), and that the wrapper only accepts
## new_file()/new_file_exp() values.

test_that("K1 — pre_staged() input silences the missing-file warning", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(genome = pre_staged(new_file("does_not_exist.fa"))),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script("f <- function(genome) genome")
  )
  expect_silent(
    new_workflow(workflow_id = "K1", desc = "no-warn",
                 task_list = list(task))
  )
})

test_that("K2 — a transferred (non pre-staged) missing file emits a warning", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(genome = new_file("does_not_exist.fa")),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script("f <- function(genome) genome")
  )
  ## The warning is a labelled block whose long fields wrap, and the break can
  ## fall between "not" and "exist". Matching any whitespace instead of a single
  ## space keeps the assertion on the words, so a change of terminal width or of
  ## the interpolated file name cannot silence it.
  expect_warning(
    new_workflow(workflow_id = "K2", desc = "warn",
                 task_list = list(task)),
    regexp = "does not[[:space:]]+exist"
  )
})

test_that("K3 — pre_staged() input registers an entry in workflow$par_locations", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(genome = pre_staged(new_file("missing.fa"))),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script("f <- function(genome) genome")
  )
  ## par_uuid of the input param is captured BEFORE new_workflow() because
  ## the construction path may rewrite par$value internally.
  input_par_uuid <- task$par_list[[1]]$par_uuid

  wf <- suppressWarnings(
    new_workflow(workflow_id = "K3", desc = "ploc",
                 task_list = list(task))
  )
  expect_true(input_par_uuid %in% names(wf$par_locations))
  expect_false(isTRUE(wf$par_locations[[input_par_uuid]]))
})

test_that("K4 — pre_staged() accepts only new_file()/new_file_exp()", {
  ## accepted
  expect_silent(pre_staged(new_file("g.fa")))
  expect_silent(pre_staged(new_file_exp("{s}.fq")))
  ## rejected: aggregation, object, dependency, plain literal
  expect_error(pre_staged(new_file_aggr("d_{s}.tsv")), regexp = "new_file")
  expect_error(pre_staged(new_object("{{x}}")),        regexp = "new_file")
  expect_error(pre_staged(new_dep(task_id = "t", value = new_object("{{x}}"))),
               regexp = "new_file")
  expect_error(pre_staged("just/a/path"),              regexp = "new_file")
})
