## print_workflow_commands() prints one block per task instance: the task_id,
## the exe_id, the exec body, and one tab-indented line per bound parameter,
## with blocks separated by "###". Instances of the same task stay contiguous,
## whatever order the expansion left them in.
##
## It writes to stdout, not to a message stream, so the output is capturable and
## redirectable, and it returns the blocks invisibly for programmatic use.
## Nothing is executed.

cap <- function(wf) utils::capture.output(print_workflow_commands(wf))

mk_wf <- function(values = c("s1", "s2", "s3")) {
  A <- new_R_task("aaa",
                  input  = new_input(s = new_object_exp("{sample}"), k = 7),
                  output = new_output(o1 = new_object("{{m}}")),
                  exec   = new_script('f <- function(s, k) paste(s, k)'))
  B <- new_R_task("bbb",
                  input  = new_input(x = new_dep(task_id = "aaa",
                                                 value = new_object_exp("{{m}}"))),
                  output = new_output(o2 = new_object("{{n}}")),
                  exec   = new_script('g <- function(x) x'))
  new_workflow("two_tasks", "d", list(A, B),
               wildcard_list = new_wildcard_list(sample = values))
}

test_that("a non-workflow is refused, naming what to do instead", {
  expect_error(print_workflow_commands(list(task_list = list())),
               "not a valid workflow_object")
  expect_error(print_workflow_commands(list(task_list = list())),
               "new_workflow")
})

test_that("one block per instance, each naming its task_id and exe_id", {
  wf  <- mk_wf()
  out <- cap(wf)
  n   <- length(wf$task_list)

  expect_equal(sum(grepl("^task_id: '", out)), n)
  expect_equal(sum(grepl("^exe_id: '", out)), n)
  expect_equal(sum(out == "###"), n - 1L)

  ## every exe_id of the workflow appears, so no instance is dropped
  for (op in wf$task_list)
    expect_true(any(grepl(as.character(op$exe_id)[1], out, fixed = TRUE)),
                info = as.character(op$exe_id)[1])
})

test_that("the exec body and the bound parameters are shown", {
  out <- cap(mk_wf())

  expect_true(any(grepl("f <- function(s, k) paste(s, k)", out, fixed = TRUE)))
  expect_true(any(grepl("g <- function(x) x", out, fixed = TRUE)))

  ## parameters are tab-indented, "<name> <value>", one per line
  expect_true(any(out == "\ts s1"))
  expect_true(any(out == "\tk 7"))          # a literal, not a wildcard value
  expect_true(any(out == "\to1 {{m}}"))     # the declared output pattern
})

test_that("instances of the same task stay contiguous", {
  out <- cap(mk_wf())
  ids <- sub("^task_id: '(.*)'$", "\\1", grep("^task_id: '", out, value = TRUE))
  ## contiguous <=> the run-length encoding has one run per distinct task
  expect_equal(length(rle(ids)$values), length(unique(ids)))
})

test_that("the blocks are returned invisibly, one per instance", {
  wf <- mk_wf()
  utils::capture.output(r <- withVisible(print_workflow_commands(wf)))

  expect_false(r$visible)
  expect_type(r$value, "list")
  expect_length(r$value, length(wf$task_list))
  expect_true(all(vapply(r$value, is.character, logical(1))))
  expect_true(grepl("task_id: 'aaa'", r$value[[1]], fixed = TRUE))
})

test_that("the output goes to stdout, so it is capturable", {
  wf <- mk_wf()
  expect_gt(length(utils::capture.output(print_workflow_commands(wf))), 0L)
  ## and nothing is written to the message stream
  expect_silent(utils::capture.output(print_workflow_commands(wf)))
})
