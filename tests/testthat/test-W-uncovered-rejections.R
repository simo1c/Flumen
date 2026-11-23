## Group W — build-time rejections that no test reached.
##
## R/validation.R refuses far more shapes than the rest of the suite reaches.
## The ones below had no test anywhere. They are guarantees flumen makes to the
## user, so each is pinned here: the rejection must fire, and the message must
## name what is wrong.
##
## All of these fail during new_workflow() / new_wildcard_list(), before any
## process or socket exists, so the group is fast and deterministic.

ok_task <- function(id = "T", input = new_input(x = 1L),
                    output = new_output(o = new_object("{{o}}"))) {
  new_R_task(task_id = id, input = input, output = output,
             exec = new_script('f <- function(x) x'))
}

test_that("W1 — a task depending on itself is rejected", {
  self <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_dep("A", new_object("{{o}}"))),
    output  = new_output(o = new_object("{{o}}")),
    exec    = new_script('f <- function(x) x')
  )
  ## [[:space:]]+ rather than a literal space: the message is a labelled block
  ## whose fields wrap, so a break may fall between any two words.
  expect_error(new_workflow("W1", "self dep", list(self)),
               regexp = "task[[:space:]]+itself")
})

test_that("W2 — two tasks declaring the same file output are rejected", {
  ## Silent data loss otherwise: both write the same path, one overwrites the
  ## other, and the run still reports success.
  a <- new_R_task(task_id = "A", input = new_input(x = 1L),
                  output = new_output(o = new_file("shared/out.txt")),
                  exec = new_script('f <- function(x) x'))
  b <- new_R_task(task_id = "B", input = new_input(x = 1L),
                  output = new_output(o = new_file("shared/out.txt")),
                  exec = new_script('f <- function(x) x'))
  expect_error(new_workflow("W2", "colliding outputs", list(a, b)),
               regexp = "both declare")
})

test_that("W3 — a wildcard declared with no values is rejected", {
  task <- new_R_task(task_id = "A",
                     input  = new_input(s = new_object_exp("{s}")),
                     output = new_output(o = new_object("{{o}}")),
                     exec   = new_script('f <- function(s) s'))
  expect_error(
    new_workflow("W3", "empty wildcard", list(task),
                 wildcard_list = new_wildcard_list(s = character(0))),
    regexp = "no values")
})

test_that("W4 — a wildcard value that is empty or NA is rejected", {
  task <- new_R_task(task_id = "A",
                     input  = new_input(s = new_object_exp("{s}")),
                     output = new_output(o = new_object("{{o}}")),
                     exec   = new_script('f <- function(s) s'))
  ## The two are refused by different guards, and each is asserted on what its
  ## own guard says. An empty string is caught in new_wildcard_list(), because
  ## an atomic value is its own key and an empty key is no key. NA survives that
  ## check and is caught by new_workflow(), which screens the resolved values.
  expect_error(
    new_workflow("W4", "empty value", list(task),
                 wildcard_list = new_wildcard_list(s = c("a", ""))),
    regexp = "empty[[:space:]]+value")
  expect_error(
    new_workflow("W4", "NA value", list(task),
                 wildcard_list = new_wildcard_list(s = c("a", NA))),
    regexp = "empty[[:space:]]+or[[:space:]]+NA")
})

test_that("W5 — a wildcard value containing a backslash is rejected", {
  task <- new_R_task(task_id = "A",
                     input  = new_input(s = new_object_exp("{s}")),
                     output = new_output(o = new_object("{{o}}")),
                     exec   = new_script('f <- function(s) s'))
  expect_error(
    new_workflow("W5", "backslash value", list(task),
                 wildcard_list = new_wildcard_list(s = c("a", "b\\c"))),
    regexp = "backslash")
})

test_that("W6 — new_dep() in an output is rejected", {
  ## A dependency is a consumer-side construct; on an output it has no meaning.
  bad <- new_R_task(
    task_id = "B",
    input   = new_input(x = 1L),
    output  = new_output(o = new_dep("A", new_object("{{o}}"))),
    exec    = new_script('f <- function(x) x')
  )
  producer <- ok_task("A")
  expect_error(new_workflow("W6", "dep in output", list(producer, bad)),
               regexp = "new_dep")
})

test_that("W7 — an _exp / _aggr constructor in an output is rejected", {
  exp_out <- new_R_task(
    task_id = "A", input = new_input(x = 1L),
    output  = new_output(o = new_object_exp("{s}")),
    exec    = new_script('f <- function(x) x')
  )
  expect_error(
    new_workflow("W7", "exp in output", list(exp_out),
                 wildcard_list = new_wildcard_list(s = c("a", "b"))),
    regexp = "not allowed")

  aggr_out <- new_R_task(
    task_id = "A", input = new_input(x = 1L),
    output  = new_output(o = new_object_aggr("{{o}}")),
    exec    = new_script('f <- function(x) x')
  )
  expect_error(new_workflow("W7", "aggr in output", list(aggr_out)),
               regexp = "not allowed")
})

test_that("W8 — mixing _exp and non-_exp on the same wildcard is rejected", {
  ## The two intents are incompatible: one fans the task out, the other binds
  ## the wildcard as a vector in a single instance.
  mixed <- new_R_task(
    task_id = "A",
    input   = new_input(a = new_object_exp("{s}"), b = new_object("{s}")),
    output  = new_output(o = new_object("{{o}}")),
    exec    = new_script('f <- function(a, b) a')
  )
  expect_error(
    new_workflow("W8", "conflicting drivers", list(mixed),
                 wildcard_list = new_wildcard_list(s = c("a", "b"))),
    regexp = "conflicting[[:space:]]+expansion")
})

test_that("W9 — an empty-string value in a constructor is rejected", {
  bad <- new_R_task(
    task_id = "A", input = new_input(f = new_file("")),
    output  = new_output(o = new_object("{{o}}")),
    exec    = new_script('f <- function(f) f')
  )
  expect_error(new_workflow("W9", "empty value", list(bad)),
               regexp = "empty-string value")
})
