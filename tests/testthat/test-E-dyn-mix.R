## Group E — Dynamic and static wildcard interaction at build time.
## Static wildcards {x} are resolved during new_workflow(); dynamic
## wildcards {{x}} stay as placeholders to be substituted at runtime.
## This group verifies how the two coexist.

test_that("E1 — pure dynamic wildcard yields a single template task instance", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(x) x")
  )
  wf <- new_workflow(workflow_id = "E1", desc = "dyn only", task_list = list(task))
  expect_equal(length(wf$task_list), 1L)
  ## The dynamic placeholder is preserved verbatim post-construction.
  out_par <- wf$task_list[[1]]$par_list[[length(wf$task_list[[1]]$par_list)]]
  expect_match(as.character(out_par$value), "\\{\\{out_A\\}\\}")
})

test_that("E2 — static expands while dynamic survives in the same task", {
  w <- new_wildcard_list(sample = c("s1", "s2"))
  task <- new_shell_task(
    task_id = "A",
    input   = new_input(reads = new_file_exp("{sample}.fq")),
    output  = new_output(idx = new_file("aligned/{sample}/`run/`{{N}}")),
    exec    = new_cmd("aligner")
  )
  wf <- new_workflow(workflow_id = "E2", desc = "mix",
                     task_list = list(task), wildcard_list = w)
  expect_equal(length(wf$task_list), 2L)
  ## Each instance keeps the {{N}} placeholder for runtime expansion.
  for (op in wf$task_list) {
    out_par <- op$par_list[[length(op$par_list)]]
    expect_match(as.character(out_par$value), "\\{\\{N\\}\\}")
  }
})

test_that("E3 — dynamic placeholder is preserved through downstream dep wiring", {
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
  wf <- new_workflow(workflow_id = "E3", desc = "dyn-thru-dep",
                     task_list = list(A, B), wildcard_list = w)
  ids <- sapply(wf$task_list, function(op) op$task_id)
  expect_equal(sum(ids == "A"), 2L)
  expect_equal(sum(ids == "B"), 2L)
})

test_that("E4 — backtick prefix carves the static prefix out of a {{x}} path", {
  task <- new_shell_task(
    task_id = "A",
    input   = new_input(genome = pre_staged(new_file("genome.fa"))),
    output  = new_output(idx = new_file("`bowtie2_idx/`{{N}}")),
    exec    = new_cmd("bowtie2-build")
  )
  wf <- new_workflow(workflow_id = "E4", desc = "bt-quote",
                     task_list = list(task))
  expect_equal(length(wf$task_list), 1L)
  out_par <- wf$task_list[[1]]$par_list[[length(wf$task_list[[1]]$par_list)]]
  ## Backtick block must remain syntactically intact post-build.
  expect_match(as.character(out_par$value), "`bowtie2_idx/`")
  expect_match(as.character(out_par$value), "\\{\\{N\\}\\}")
})
