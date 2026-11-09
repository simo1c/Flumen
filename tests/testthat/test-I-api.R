## Group I — API ergonomics.
## Surface tests on the public DSL: new_input/new_output naming,
## new_dep variants, new_script(file=) loading, new_worker config.

test_that("I1 — new_input / new_output preserve named arguments", {
  inp <- new_input(a = new_object("x"), b = new_object("y"))
  expect_equal(names(inp), c("a", "b"))

  out <- new_output(z = new_object("{{out}}"))
  expect_equal(names(out), c("z"))
})

test_that("I2 — new_wildcard_list rejects unnamed non-wildcard_object args", {
  ## Path tested in R/wildcard_dsl.R: a positional vector without a name
  ## (and not produced by new_wildcard()) is rejected, since the wildcard
  ## key is mandatory for downstream {name} substitution.
  expect_error(
    new_wildcard_list(c("s1", "s2")),
    regexp = "unnamed arguments"
  )
})

test_that("I3 — new_dep accepts task_id with a value pattern", {
  d <- new_dep(task_id = "A", value = new_object("{{out_A}}"))
  expect_true("dep_object" %in% class(d) ||
              "dep_list"   %in% class(d))
})

test_that("I4 — new_script(file = path) loads source from disk", {
  tmp <- tempfile(fileext = ".R")
  on.exit(unlink(tmp), add = TRUE)
  writeLines("f <- function(x) x * 2", tmp)
  s <- new_script(file = tmp)
  expect_true("script" %in% class(s))
  expect_match(as.character(s), "f <- function")
})

test_that("I5 — new_worker stores the supplied configuration", {
  w <- new_worker(name = "w1", n_concurrent_task = 4L)
  expect_equal(w$name, "w1")
  expect_equal(w$n_concurrent_task, 4L)
})
