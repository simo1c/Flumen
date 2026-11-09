## Group X — preview_workflow(), the static "dry run".
##
## It is an exported function, 182 lines, and covr measured it at 0%: nothing
## had ever executed it. It is also what a user runs BEFORE launching, to see
## what the pipeline will do -- so a break here lands on someone's first
## contact with flumen.
##
## Nothing is executed: preview_workflow() only reads the already-expanded
## template and prints. Each test captures the output and asserts on what it
## must contain.

cap <- function(...) paste(utils::capture.output(preview_workflow(...)), collapse = "\n")

mk_producer <- function(id = "gen")
  new_R_task(task_id = id,
             input  = new_input(s = new_object_exp("{sample}")),
             output = new_output(out = new_object("{{m}}")),
             exec   = new_script('f <- function(s) s'))

test_that("X1 — the header names the workflow and lists the wildcards", {
  wf <- new_workflow("demo_wf", "d", list(mk_producer()),
                     wildcard_list = new_wildcard_list(sample = c("s1", "s2", "s3")))
  out <- cap(wf)
  expect_true(grepl("flumen workflow preview", out, fixed = TRUE))
  expect_true(grepl("demo_wf", out, fixed = TRUE))
  expect_true(grepl("sample=", out, fixed = TRUE))
  expect_true(grepl("s1,s2,s3", out, fixed = TRUE))
})

test_that("X2 — the instance count reflects the static expansion", {
  wf <- new_workflow("counts", "d", list(mk_producer()),
                     wildcard_list = new_wildcard_list(sample = c("a", "b", "c", "d")))
  out <- cap(wf)
  expect_true(grepl("gen", out, fixed = TRUE))
  expect_true(grepl("4", out, fixed = TRUE))          # four instances
})

test_that("X3 — an aggregating consumer is reported as such, not expanded", {
  prod <- mk_producer()
  cons <- new_R_task(task_id = "gather",
                     input  = new_input(ms = new_dep("gen", new_object_aggr("{{m}}"))),
                     output = new_output(out = new_object("{{g}}")),
                     exec   = new_script('f <- function(ms) ms'))
  out <- cap(new_workflow("aggr_wf", "d", list(prod, cons),
                          wildcard_list = new_wildcard_list(sample = c("s1", "s2"))))
  expect_true(grepl("gather", out, fixed = TRUE))
  ## Full aggregation: a single instance, so no "per <wildcard>" qualifier.
  expect_true(grepl("[aggr]", out, fixed = TRUE))
  expect_false(grepl("aggr per", out, fixed = TRUE))
})

test_that("X4 — partial aggregation shows the by-group", {
  prod <- new_R_task(task_id = "gen",
                     input  = new_input(s = new_object_exp("{sample}"),
                                        r = new_object_exp("{ref}")),
                     output = new_output(out = new_object("{{m}}")),
                     exec   = new_script('f <- function(s, r) s'))
  cons <- new_R_task(task_id = "grp",
                     input  = new_input(ms = new_dep("gen", new_object_aggr("{{m}}", by = "sample"))),
                     output = new_output(out = new_object("{{g}}")),
                     exec   = new_script('f <- function(ms) ms'))
  out <- cap(new_workflow("by_wf", "d", list(prod, cons),
                          wildcard_list = new_wildcard_list(sample = c("s1", "s2"),
                                                            ref    = c("r1", "r2"))))
  ## A gather inside a task that is cloned over {sample} is a partial
  ## aggregation: one gather per sample, not one for the whole workflow. The
  ## preview reports it from the post-expansion facts (the task has clones,
  ## each pinned to one sample) -- the "partial_aggregation" class is stripped
  ## during expansion, on purpose, once the by-wildcard is resolved.
  expect_true(grepl("aggr per sample", out, fixed = TRUE))
  expect_true(grepl("2 instances", out, fixed = TRUE))
})

test_that("X5 — the runtime foot-gun is flagged: _exp over a dynamic wildcard", {
  ## The documented hazard: the consumer runs once per FILE the producer emits,
  ## not once per sample. The preview exists largely to surface this.
  ## The producer's dynamic output carries {sample} so each instance owns its
  ## own namespace -- without it the shared-namespace guard refuses the workflow
  ## and the foot-gun this test is about never gets as far as the preview.
  prod <- new_R_task(task_id = "mk",
                     input  = new_input(s = new_object_exp("{sample}")),
                     output = new_output(out = new_file("outs/{sample}/{{any}}.txt")),
                     exec   = new_script('f <- function(s) s'))
  cons <- new_R_task(task_id = "use",
                     input  = new_input(f = new_dep("mk", new_file_exp("outs/{sample}/{{any}}.txt"))),
                     output = new_output(out = new_object("{{u}}")),
                     exec   = new_script('f <- function(f) f'))
  out <- cap(new_workflow("dyn_wf", "d", list(prod, cons),
                          wildcard_list = new_wildcard_list(sample = c("s1", "s2"))))
  expect_true(grepl("runtime", out))
  expect_true(grepl("each file produced by", out, fixed = TRUE))
})

test_that("X6 — with workers, eligibility is reported and an impossible pin is flagged", {
  pinned <- new_R_task(task_id = "needs_gpu",
                       input  = new_input(x = 1L),
                       output = new_output(o = new_object("{{o}}")),
                       exec   = new_script('f <- function(x) x'),
                       requires = list(tags = "gpu"))
  wf <- new_workflow("pin_wf", "d", list(pinned))

  ## a worker that satisfies the tag
  ok <- cap(wf, worker_list = list(new_worker("w1", n_concurrent_task = 1,
                                              resources = list(tags = "gpu"))))
  expect_true(grepl("w1", ok, fixed = TRUE))

  ## no worker satisfies it -> the preview must say so rather than stay silent
  bad <- cap(wf, worker_list = list(new_worker("w1", n_concurrent_task = 1)))
  expect_true(grepl("NO eligible worker", bad, fixed = TRUE))
})

test_that("X7 — preview works with no workers and no wildcards", {
  ## The degenerate call: a user exploring a single task before declaring
  ## anything else. Must print, not fail.
  wf <- new_workflow("bare", "d",
                     list(new_R_task(task_id = "solo", input = new_input(x = 1L),
                                     output = new_output(o = new_object("{{o}}")),
                                     exec = new_script('f <- function(x) x'))))
  out <- cap(wf)
  expect_true(grepl("solo", out, fixed = TRUE))
  expect_true(nchar(out) > 0L)
})
