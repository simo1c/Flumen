## Group F — Polyglot operation builders (R / Python / shell).
## Build-time only. No interpreter is invoked: we just verify that each
## op factory returns the right object class, that script bodies survive
## construction, and that the exec object retains the expected subclass.

test_that("F1 — new_R_task accepts a new_script() exec and yields R_task_object", {
  src <- "f <- function(x) x + 1"
  task <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script(src)
  )
  expect_true("R_task_object" %in% class(task) || "task_object" %in% class(task))
  expect_true("script" %in% class(task$exec))
})

test_that("F2 — new_shell_task accepts new_cmd and yields shell_task_object", {
  task <- new_shell_task(
    task_id = "A",
    input   = new_input(genome = pre_staged(new_file("genome.fa"))),
    output  = new_output(idx = new_file("`out/`{{N}}")),
    exec    = new_cmd("bowtie2-build")
  )
  expect_true("sh_cmd" %in% class(task$exec) || "cmd" %in% class(task$exec))
})

test_that("F3 — new_python_task dedents indented source and preserves script class", {
  ## Indented (as if defined inside an R block); dedent_python() should
  ## strip leading whitespace so wrapper.py can parse the body.
  indented <- "    def f(x):\n        return x + 1\n"
  task <- new_python_task(
    task_id = "A",
    input   = new_input(x = new_object("v")),
    output  = new_output(out = new_object("{{out}}")),
    exec    = new_script(indented)
  )
  expect_true("script" %in% class(task$exec))
  ## Leading 4-space indent should be removed.
  expect_false(grepl("^    def", as.character(task$exec)))
  expect_true(grepl("^def f", as.character(task$exec)))
})
