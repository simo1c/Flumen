## Group A — Wildcard placeholder syntax.
## Build-time unit tests: validate that flumen recognises and tags each
## placeholder syntax variant onto the value class chain. No workflow is
## executed; failures here surface as new_workflow() construction errors.

test_that("A1 — single static workflow wildcard {x} produces N instances", {
  ## Two-task DAG so flumen has a connected graph; A is the wildcard-expanded
  ## producer, B consumes each instance via new_dep. Two samples => two A
  ## instances and two B instances after build-time expansion.
  w_list <- new_wildcard_list(sample = make_toy_counts_list(c("s1", "s2")))
  A <- new_R_task(
    task_id = "A",
    input   = new_input(df = new_object_exp("{sample}")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(df) df")
  )
  B <- new_R_task(
    task_id = "B",
    input   = new_input(
      x = new_dep(task_id = "A", value = new_object_exp("{{out_A}}"))
    ),
    output  = new_output(out = new_object("{{out_B}}")),
    exec    = new_script("g <- function(x) x")
  )
  wf <- new_workflow(
    workflow_id   = "A1",
    desc          = "single static wildcard",
    task_list     = list(A, B),
    wildcard_list = w_list
  )
  task_ids <- sapply(wf$task_list, function(op) op$task_id)
  expect_equal(sum(task_ids == "A"), 2L)
  expect_equal(sum(task_ids == "B"), 2L)
})

test_that("A2 — single dynamic wildcard {{x}} is recognised in output", {
  w_list <- new_wildcard_list(sample = c("s1"))
  task <- new_R_task(
    task_id = "A",
    input   = new_input(df = new_object_exp("{sample}")),
    output  = new_output(out = new_object("{{out_dynamic}}")),
    exec    = new_script("f <- function(df) df")
  )
  ## Find the output param post-construction; its value must contain {{...}}
  out_par <- task$par_list[[length(task$par_list)]]
  expect_match(as.character(out_par$value), "\\{\\{out_dynamic\\}\\}")
})

test_that("A3 — backtick prefix isolates the static portion of a path", {
  w_list <- new_wildcard_list(sample = c("s1"))
  task <- new_shell_task(
    task_id = "A",
    input   = new_input(genome = pre_staged(new_file("genome.fa"))),
    output  = new_output(idx = new_file("`index/`{{N}}")),
    exec    = new_cmd("bowtie2-build")
  )
  out_par <- task$par_list[[length(task$par_list)]]
  expect_match(as.character(out_par$value), "`index/`")
  expect_match(as.character(out_par$value), "\\{\\{N\\}\\}")
})

test_that("A4 — aggregate brackets [{x}] yield class 'aggregation'", {
  obj <- new_file_aggr("counts/{sample}.tsv")
  expect_true("aggregation" %in% class(obj))
  expect_true("file_object" %in% class(obj))
})

test_that("A5 — multiple wildcards in the same path coexist", {
  w_list <- new_wildcard_list(
    ref    = c("mmu"),
    sample = c("s1", "s2")
  )
  task <- new_shell_task(
    task_id = "A",
    input   = new_input(reads = new_file_exp("{ref}/{sample}.fq")),
    output  = new_output(sam   = new_file("aligned/{ref}_{sample}.sam")),
    exec    = new_cmd("aligner")
  )
  wf <- new_workflow(
    workflow_id   = "A5",
    desc          = "two wildcards in path",
    task_list     = list(task),
    wildcard_list = w_list
  )
  ## Two samples × one ref => two task instances
  expect_equal(length(wf$task_list), 2L)
})
