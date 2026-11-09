## Group D — Multi-wildcard alignment.
## Validates the cardinality of expansion across DAGs that mix several
## wildcards. Confirms (a) cross-product expansion when wildcards are
## independent, (b) zip alignment when the same wildcard recurs across
## tasks, and (c) the interaction between expansion and aggregation.

test_that("D1 — same wildcard across A and B aligns by name (zip, not cross)", {
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
  wf <- new_workflow(workflow_id = "D1", desc = "zip",
                     task_list = list(A, B), wildcard_list = w)
  ## 2 samples => 2 + 2 = 4 instances total (NOT 2*2 = 4 with cross, but the
  ## structural distinction is that B has exactly one parent each, not two).
  ids <- sapply(wf$task_list, function(op) op$task_id)
  expect_equal(sum(ids == "A"), 2L)
  expect_equal(sum(ids == "B"), 2L)
  ## Each B has exactly one A parent (zip semantics).
  Bs <- wf$task_list[ids == "B"]
  for (b in Bs) {
    parents <- igraph::neighbors(wf$DAG, b$task_id, mode = "in")
    expect_equal(length(parents), 1L)
  }
})

test_that("D2 — independent wildcards in same path produce cross-product", {
  w <- new_wildcard_list(
    ref    = c("mmu", "hsa"),
    sample = c("s1", "s2", "s3")
  )
  task <- new_shell_task(
    task_id = "A",
    input   = new_input(reads = pre_staged(new_file_exp("{ref}/{sample}.fq"))),
    output  = new_output(sam   = new_file("aligned/{ref}_{sample}.sam")),
    exec    = new_cmd("aligner")
  )
  wf <- new_workflow(workflow_id = "D2", desc = "cross",
                     task_list = list(task), wildcard_list = w)
  ## 2 refs * 3 samples = 6 instances.
  expect_equal(length(wf$task_list), 6L)
})

test_that("D3 — shared wildcard propagates through dependency chain", {
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
  C <- new_R_task(
    task_id = "C",
    input   = new_input(x = new_dep(task_id = "B", value = new_object("{{out_B}}"))),
    output  = new_output(out = new_object("{{out_C}}")),
    exec    = new_script("h <- function(x) x")
  )
  wf <- new_workflow(workflow_id = "D3", desc = "chain",
                     task_list = list(A, B, C), wildcard_list = w)
  ids <- sapply(wf$task_list, function(op) op$task_id)
  ## All three tasks must expand 1:1 with the sample wildcard.
  expect_equal(sum(ids == "A"), 2L)
  expect_equal(sum(ids == "B"), 2L)
  expect_equal(sum(ids == "C"), 2L)
})

test_that("D4 — partial aggregation collapses on `by` axis only", {
  ## Two ref values * 2 samples = 4 producers; aggregating by 'sample' keeps
  ## one C per ref => 2 consumers, each summing over its sample dimension.
  w <- new_wildcard_list(
    ref    = c("mmu", "hsa"),
    sample = c("s1", "s2")
  )
  A <- new_R_task(
    task_id = "A",
    input   = new_input(reads = pre_staged(new_file_exp("{ref}/{sample}.fq"))),
    output  = new_output(out   = new_object("{{out_A}}")),
    exec    = new_script("f <- function(reads) reads")
  )
  C <- new_R_task(
    task_id = "C",
    input   = new_input(
      xs = new_dep(task_id = "A", value = new_object_aggr("{{out_A}}", by = "sample"))
    ),
    output  = new_output(out = new_object("{{out_C}}")),
    exec    = new_script("g <- function(xs) length(xs)")
  )
  wf <- new_workflow(workflow_id = "D4", desc = "partial-aggr",
                     task_list = list(A, C), wildcard_list = w)
  ids <- sapply(wf$task_list, function(op) op$task_id)
  expect_equal(sum(ids == "A"), 4L)  ## 2 ref * 2 sample
  expect_equal(sum(ids == "C"), 2L)  ## one per ref after collapsing on sample
})
