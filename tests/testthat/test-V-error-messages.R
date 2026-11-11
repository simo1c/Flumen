## Group V — quality of the build-time error messages.
##
## An error is only useful if the reader can act on it. Every case below asserts
## three things, not just that something was raised:
##   (1) it errors at BUILD time, before any process or socket exists;
##   (2) the message NAMES the offending element -- the task, parameter,
##       wildcard or worker -- so the reader knows where to look;
##   (3) the message carries an ACTIONABLE cue, not just a statement of fact.
##
## This is a regression guard: the messages are currently good, and the point is
## to keep them that way. Nothing here executes a workflow.

## A message is actionable if it tells the reader what to do differently --
## either in words (an imperative) or by naming the construct that is the remedy.
## The second branch matters: several messages guide by pointing at the right
## API rather than by phrasing an instruction, and a purely verb-based rule
## would reject them wrongly. The leading "fn(...):" prefix is stripped first,
## so the function being complained about cannot satisfy the rule by itself.
IMPERATIVE <- "use |Use |Add |add |must |Hint|Allowed|align|Rename|rename|instead|Give |Set |Remove|Match "
REMEDY_CALL <- "[A-Za-z_][A-Za-z0-9_.*]*\\("

is_actionable <- function(msg) {
  body <- sub("^[^:\n]*:[[:space:]]*", "", msg)
  grepl(IMPERATIVE, body) || grepl(REMEDY_CALL, body)
}

## A message is a labelled block whose long fields wrap to the terminal width,
## so where a line breaks depends on the width and on how long the interpolated
## values are. Collapsing the whitespace first makes an expectation match the
## WORDS of a message rather than its layout: `task : T` holds whether the
## renderer pads the label to ten columns or breaks the line after it.
expect_good_error <- function(expr, names_it, label) {
  msg <- tryCatch({ force(expr); NA_character_ },
                  error = function(e) conditionMessage(e))
  expect_false(is.na(msg), label = paste0(label, " raises an error"))
  flat <- gsub("[[:space:]]+", " ", msg)
  for (n in names_it)
    expect_true(grepl(n, flat, fixed = TRUE),
                label = paste0(label, " names '", n, "' -- got: ", substr(flat, 1, 160)))
  expect_true(is_actionable(msg),
              label = paste0(label, " is actionable -- got: ", substr(msg, 1, 120)))
}

test_that("V1 — exec given a bare string points at new_script()", {
  expect_good_error(
    new_R_task(task_id = "T", output = new_output(o = new_object("{{o}}")),
               exec = "f <- function() 1"),
    c("exec", "new_script"), "V1")
})

test_that("V2 — a task with neither input nor output names the task", {
  expect_good_error(
    new_R_task(task_id = "T", exec = new_script('f <- function() 1')),
    c("task : T", "input", "output"), "V2")
})

test_that("V3 — duplicate parameter names name the task and the parameter", {
  expect_good_error(
    new_workflow("W", "d", list(new_shell_task(
      task_id = "T", input = new_input(x = 1, x = 2),
      output = new_output(o = new_file("o.txt")), exec = new_cmd("true"))),
      wildcard_list = list()),
    c("task : T", "'x'"), "V3")
})

test_that("V4 — an undeclared wildcard names it and says how to declare it", {
  expect_good_error(
    new_workflow("W", "d", list(new_R_task(
      task_id = "T", input = new_input(x = new_object_exp("{nope}")),
      output = new_output(o = new_object("{{o}}")),
      exec = new_script('f <- function(x) x'))),
      wildcard_list = new_wildcard_list(other = c("a"))),
    c("nope", "new_wildcard_list"), "V4")
})

test_that("V5 — a dep that matches no producer output lists the real outputs", {
  p <- new_R_task(task_id = "P", output = new_output(o = new_file("a/{{x}}.txt")),
                  exec = new_script('f <- function() 1'))
  cons <- new_R_task(task_id = "C",
                     input  = new_input(f = new_dep("P", new_file("b/{{x}}.txt"))),
                     output = new_output(o = new_object("{{o}}")),
                     exec   = new_script('f <- function(f) f'))
  expect_good_error(
    new_workflow("W", "d", list(p, cons), wildcard_list = list()),
    c("task : C", "'P'", "b/{{x}}.txt", "a/{{x}}.txt"), "V5")
})

test_that("V6 — backticks in an R task explain that they are shell-only", {
  expect_good_error(
    new_workflow("W", "d", list(new_R_task(
      task_id = "T", output = new_output(o = new_file("`d/`{{x}}.txt")),
      exec = new_script('f <- function() 1'))), wildcard_list = list()),
    c("task : T", "shell-only"), "V6")
})

test_that("V7 — self_host on a local worker names the worker and the alternative", {
  expect_good_error(
    new_worker("w1", self_host = "1.2.3.4", n_concurrent_task = 1),
    c("'w1'", "self_host", "master_host"), "V7")
})

test_that("V8 — an unknown 'requires' field lists the allowed ones", {
  expect_good_error(
    new_R_task(task_id = "T", input = new_input(x = 1),
               output = new_output(o = new_object("{{o}}")),
               exec = new_script('f <- function(x) x'), requires = list(foo = 1)),
    c("'T'", "foo", "cpu"), "V8")
})

test_that("V9 — a static object aggregation points at the dynamic form", {
  p <- new_R_task(task_id = "P", input = new_input(s = new_object_exp("{s}")),
                  output = new_output(o = new_object("{{o}}")),
                  exec = new_script('f <- function(s) s'))
  cons <- new_R_task(task_id = "C",
                     input  = new_input(v = new_dep("P", new_object_aggr("{s}"))),
                     output = new_output(o = new_object("{{o2}}")),
                     exec   = new_script('f <- function(v) v'))
  expect_good_error(
    new_workflow("W", "d", list(p, cons), wildcard_list = new_wildcard_list(s = c("a", "b"))),
    ## new_file_aggr is asserted on purpose: the message must say where a static
    ## {wc} pattern IS valid, not only that it is invalid here.
    c("task : C", "new_object_aggr", "new_file_aggr"), "V9")
})

test_that("V10 — overlapping worker port ranges name both workers", {
  skip_on_os("windows")   ## validate_exec_args() refuses local workers on Windows first
  task <- new_R_task(task_id = "T", input = new_input(x = 1),
                     output = new_output(o = new_object("{{o}}")),
                     exec = new_script('f <- function(x) x'))
  wf <- new_workflow("W", "d", list(task))
  ml <- structure(list(resources = NULL), class = "manual_launcher")
  expect_good_error(
    run_workflow(wf, worker_list = list(
        new_worker("wa", n_concurrent_task = 1, ports = 61100:61149),
        new_worker("wb", self_host = "127.0.0.1", n_concurrent_task = 1,
                   ports = 61100:61149, launcher = ml)),
      master_host = "127.0.0.1", ports = 61000:61099, chunk_size = 1024L),
    c("'wa'", "'wb'", "61100"), "V10")
})
