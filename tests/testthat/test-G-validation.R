## Group G — Build-time validation errors.
## Verifies that new_workflow() refuses malformed task_lists with a
## descriptive message. Each test pins one validation rule from
## R/validation.R::validate_workflow().

test_that("G1 — empty task_list is rejected", {
  expect_error(
    new_workflow(workflow_id = "G1", desc = "empty", task_list = list()),
    regexp = "at least one operation"
  )
})

test_that("G2 — duplicate task_id is rejected", {
  A1 <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("a")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(x) x")
  )
  A2 <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("b")),
    output  = new_output(out = new_object("{{out_B}}")),
    exec    = new_script("g <- function(x) x")
  )
  expect_error(
    new_workflow(workflow_id = "G2", desc = "dup", task_list = list(A1, A2)),
    regexp = "duplicate task_id"
  )
})

test_that("G3 — using a static wildcard not declared in wildcard_list errors", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(df = new_object_exp("{undeclared}")),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(df) df")
  )
  ## No wildcard_list passed => 'undeclared' is undeclared.
  expect_error(
    new_workflow(workflow_id = "G3", desc = "missing-wc",
                 task_list = list(task)),
    regexp = "not declared"
  )
})

test_that("G4 — new_dep referencing an unknown task_id is rejected", {
  A <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_dep(task_id = "Z", value = new_object("{{out_Z}}"))),
    output  = new_output(out = new_object("{{out_A}}")),
    exec    = new_script("f <- function(x) x")
  )
  expect_error(
    new_workflow(workflow_id = "G4", desc = "bad-dep", task_list = list(A)),
    regexp = "not in task_list"
  )
})

test_that("G5 — more than one dynamic wildcard in a single parameter errors", {
  task <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("a")),
    output  = new_output(out = new_object("{{X}}_{{Y}}")),
    exec    = new_script("f <- function(x) x")
  )
  expect_error(
    new_workflow(workflow_id = "G5", desc = "two-dyn", task_list = list(task)),
    regexp = "dynamic"
  )
})

test_that("G6 — bare-folder output path is rejected", {
  ## Single-segment, no extension, no wildcard, no backtick — i.e. the
  ## pattern that pre-removal would have used new_folder("output_dir").
  task <- new_shell_task(
    task_id = "copy",
    new_input(reads = new_file("../data/{sample}.fastq")),
    new_output(new_file("output_dir")),
    exec = new_cmd("mv")
  )
  expect_error(
    new_workflow(
      workflow_id   = "G6",
      desc          = "bare-folder",
      task_list     = list(task),
      wildcard_list = new_wildcard_list(sample = "x")
    ),
    regexp = "ambiguous output path"
  )
})

test_that("G6b — trailing-slash output path is rejected", {
  task <- new_shell_task(
    task_id = "copy",
    new_input(reads = new_file("../data/{sample}.fastq")),
    new_output(new_file("output_dir/")),
    exec = new_cmd("mv")
  )
  expect_error(
    new_workflow(
      workflow_id   = "G6b",
      desc          = "trailing-slash",
      task_list     = list(task),
      wildcard_list = new_wildcard_list(sample = "x")
    ),
    regexp = "ambiguous output path"
  )
})

test_that("G7 — two+ pure-dynamic outputs (no static discriminator) are rejected", {
  ## A dynamic {{name}} is a runtime fan-out of ONE output slot, not a
  ## build-time output selector; a consumer addresses an output only by STATIC
  ## content. Several outputs distinguished only by a dynamic wildcard are
  ## mutually unselectable -- the whole named collection is delivered instead,
  ## crashing a typed consumer far from the cause (e.g. a Python task receiving
  ## a dict of all outputs). Reject at build time. (Rule 4c.)
  gen <- new_R_task(
    task_id = "gen",
    input   = new_input(x = 1L),
    output  = new_output(d = new_object("{{d}}"),
                         m = new_object("{{m}}"),
                         v = new_object("{{v}}")),
    exec    = new_script("f <- function(x) list(d = x, m = x, v = x)")
  )
  expect_error(
    new_workflow(workflow_id = "G7", desc = "multi-pure-dynamic-out",
                 task_list = list(gen)),
    regexp = "pure dynamic wildcards"
  )
})

test_that("G7b — a SINGLE pure-dynamic output is accepted (canonical pattern)", {
  ## The most common output declaration: one new_object("{{out}}") slot whose
  ## count/keys are decided at runtime. Must NOT be caught by rule 4c.
  gen <- new_R_task(
    task_id = "gen",
    input   = new_input(x = 1L),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script("f <- function(x) list(a = x, b = x)")
  )
  expect_no_error(
    suppressWarnings(new_workflow(workflow_id = "G7b", desc = "single-dynamic-out",
                                  task_list = list(gen)))
  )
})

test_that("G7c — dynamic output WITH a static path discriminator is accepted", {
  ## The real multi-output shape (scRNA count, RSEM, ...): each output carries
  ## static path context, so a consumer can select it. Not caught by rule 4c.
  gen <- new_shell_task(
    task_id = "gen",
    new_input(x = new_object_exp("{s}")),
    new_output(matrix = new_file("c_{s}/outs/filtered/{{m}}"),
               bam    = new_file("c_{s}/outs/possorted.bam")),
    exec = new_cmd("true")
  )
  expect_no_error(
    suppressWarnings(new_workflow(workflow_id = "G7c", desc = "dynamic-with-static-path",
                                  task_list = list(gen),
                                  wildcard_list = new_wildcard_list(s = c("a", "b"))))
  )
})

test_that("G6c — backtick + dynamic folder pattern is accepted", {
  task <- new_shell_task(
    task_id = "copy",
    new_input(reads = new_file("../data/{sample}.fastq")),
    new_output(new_file("`output_dir`/{{out}}")),
    exec = new_cmd("mv")
  )
  expect_silent(
    new_workflow(
      workflow_id   = "G6c",
      desc          = "ok-folder",
      task_list     = list(task),
      wildcard_list = new_wildcard_list(sample = "x")
    )
  )
})

test_that("G6d — file with extension or wildcard is accepted", {
  ## Several legitimate output patterns must NOT trip the bare-folder check.
  ok_files <- c(
    "output.txt",
    "data/{sample}.tsv",
    "data/{{out}}.csv",
    "`prefix`.{{N}}.bt2"
  )
  for(p in ok_files){
    task <- new_shell_task(
      task_id = "ok",
      new_input(reads = new_file("../data/{sample}.fastq")),
      new_output(new_file(p)),
      exec = new_cmd("touch")
    )
    expect_silent(
      new_workflow(
        workflow_id   = paste0("G6d_", gsub("[^A-Za-z0-9]", "_", p)),
        desc          = "ok-pattern",
        task_list     = list(task),
        wildcard_list = new_wildcard_list(sample = "x")
      )
    )
  }
})

test_that("G8 - input/output must come from new_input()/new_output()", {
  S <- new_script('f <- function(x) x')

  ## Unrejected, a bare new_object()/new_file() as `output` builds and runs
  ## while registering no output, so a downstream new_dep() fails much later
  ## with an unrelated "input was not resolved" error.
  expect_error(
    new_R_task(task_id = "G8", input = new_input(x = 1),
               output = new_object("{{o}}"), exec = S),
    regexp = "'output' must be created with new_output"
  )
  expect_error(
    new_R_task(task_id = "G8", input = new_object_exp("{sample}"),
               output = new_output(o = new_object("{{o}}")), exec = S),
    regexp = "'input' must be created with new_input"
  )
  expect_error(
    new_shell_task(task_id = "G8", output = new_file("out.txt"),
                   exec = new_script('#!/bin/bash\ntrue')),
    regexp = "'output' must be created with new_output"
  )
  expect_error(
    new_python_task(task_id = "G8", output = new_object("{{o}}"),
                    exec = new_script('def f():\n    return 1')),
    regexp = "'output' must be created with new_output"
  )

  ## Legitimate shapes stay valid: both wrapped, and NULL input or output.
  expect_s3_class(
    new_R_task(task_id = "G8", input = new_input(x = 1),
               output = new_output(o = new_object("{{o}}")), exec = S),
    "task_object"
  )
  expect_s3_class(
    new_R_task(task_id = "G8", output = new_output(o = new_object("{{o}}")), exec = S),
    "task_object"
  )
  expect_s3_class(
    new_R_task(task_id = "G8", input = new_input(x = 1), exec = S),
    "task_object"
  )
})

test_that("G9 - a forgotten list() is caught at the entry point", {
  skip_on_os("windows")   ## validate_exec_args() refuses local workers on Windows first
  S <- new_script('f <- function(x) x')
  task <- new_R_task(task_id = "G9", input = new_input(x = 1),
                     output = new_output(o = new_object("{{o}}")), exec = S)
  worker <- new_worker(name = "G9w", n_concurrent_task = 1)
  wf <- new_workflow(workflow_id = "G9", desc = "d",
                     task_list = list(task), wildcard_list = list())

  ## task_object and worker_object are themselves lists, so these used to pass
  ## the length check and die deep inside with "$ operator is invalid for
  ## atomic vectors".
  expect_error(
    new_workflow(workflow_id = "G9", desc = "d",
                 task_list = task, wildcard_list = list()),
    regexp = "must be a list of tasks, not a single task"
  )
  expect_error(run_workflow(wf, worker_list = worker),
               regexp = "must be a list of workers, not a single worker")

  ## Wrong element types are named with their position and class.
  expect_error(
    new_workflow(workflow_id = "G9", desc = "d",
                 task_list = list(task, "oops"), wildcard_list = list()),
    regexp = "must contain only tasks built with"
  )
  expect_error(run_workflow(wf, worker_list = list(worker, 42)),
               regexp = "must contain only workers built with")
})

test_that("G10 - duplicate worker names fail fast instead of timing out", {
  skip_on_os("windows")   ## validate_exec_args() refuses local workers on Windows first
  wf <- new_workflow(
    workflow_id = "G10", desc = "d", wildcard_list = list(),
    task_list = list(new_R_task(task_id = "G10", input = new_input(x = 1),
                                output = new_output(o = new_object("{{o}}")),
                                exec = new_script('f <- function(x) x')))
  )
  mkw <- function(n) new_worker(name = n, n_concurrent_task = 1)

  ## The scheduler keys workers by name, so duplicates used to collide silently
  ## and surface only as a startup timeout blaming firewalls and ports.
  expect_error(run_workflow(wf, worker_list = list(mkw("w1"), mkw("w1"))),
               regexp = "duplicate worker name")
  expect_error(run_workflow(wf, worker_list = list(mkw("a"), mkw("b"), mkw("a"))),
               regexp = "duplicate worker name")
})

test_that("G11 - a fanned-out task cannot write all instances to one file", {
  S <- new_script('#!/bin/bash\ntrue')
  wl <- new_wildcard_list(s = c("s1", "s2"))
  mk <- function(out) new_shell_task(task_id = "G11",
                                     input  = new_input(s = new_object_exp("{s}")),
                                     output = new_output(o = out), exec = S)

  ## N instances resolving to the same path overwrite each other on disk while
  ## the run still reports success: silent data loss.
  expect_error(
    new_workflow(workflow_id = "G11", desc = "d",
                 task_list = list(mk(new_file("fixed.txt"))), wildcard_list = wl),
    regexp = "contains no wildcard"
  )

  ## A static wildcard gives each instance its own path.
  expect_s3_class(
    new_workflow(workflow_id = "G11", desc = "d",
                 task_list = list(mk(new_file("out_{s}.txt"))), wildcard_list = wl),
    "workflow_object"
  )
  ## A dynamic wildcard alone does NOT save a fanned-out task: the names are
  ## known only at run time, so every instance globs the same directory and
  ## claims whatever a sibling wrote while it ran -- samples merge in silence.
  ## This assertion used to expect the workflow to build; the shared-namespace
  ## guard refuses the shape instead, which is the behaviour we want.
  expect_error(
    new_workflow(workflow_id = "G11", desc = "d",
                 task_list = list(mk(new_file("`d/`{{any}}"))), wildcard_list = wl),
    regexp = "carries no static wildcard"
  )
  ## Putting the static wildcard in the path gives each instance its own
  ## namespace, and the dynamic part is then free to be bound at run time.
  expect_s3_class(
    new_workflow(workflow_id = "G11", desc = "d",
                 task_list = list(mk(new_file("`d/{s}/`{{any}}"))), wildcard_list = wl),
    "workflow_object"
  )
  ## A task that does not fan out has a single instance, so no collision:
  ## new_file_exp() with no wildcard yields one instance (verified behaviour).
  expect_s3_class(
    new_workflow(workflow_id = "G11", desc = "d", wildcard_list = list(),
                 task_list = list(new_shell_task(task_id = "G11b",
                   input  = new_input(x = 1),
                   output = new_output(o = new_file("fixed.txt")), exec = S))),
    "workflow_object"
  )
})

test_that("G12 — duplicate parameter names are rejected, unnamed ones are not", {
  ## Two parameters sharing a REAL name collide in the wrapper's named par_list
  ## and its per-parameter length bookkeeping -> "invalid 'times' argument".
  expect_error(
    new_workflow(workflow_id = "G12", desc = "d", wildcard_list = list(),
                 task_list = list(new_shell_task(task_id = "G12a",
                   input  = new_input(x = 1, x = 2),
                   output = new_output(o = new_file("o.txt")),
                   exec   = new_cmd("true")))),
    regexp = "duplicate parameter name"
  )

  ## Unnamed parameters are the POSITIONAL arguments of a shell task, so several
  ## of them is the normal case -- `mv src dst` cannot be written any other way,
  ## since naming a parameter pushes the name itself into argv.
  expect_s3_class(
    new_workflow(workflow_id = "G12", desc = "d", wildcard_list = list(),
                 task_list = list(new_shell_task(task_id = "G12b",
                   input  = new_input(pre_staged(new_file("src.txt"))),
                   output = new_output(new_file("dst.txt")),
                   exec   = new_cmd("mv")))),
    "workflow_object"
  )
})
