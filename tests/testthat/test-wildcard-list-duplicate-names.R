## new_wildcard_list() assigned each wildcard with `wildcards[[name]] <- ...`,
## which REPLACES an earlier wildcard of the same name, so its check for
## duplicated names, made after the loop, never fired:
## new_wildcard_list(s = c("a", "b"), s = "c") kept s = "c" alone, silently,
## and the workflow ran on one sample instead of three (R/wildcard_dsl.R).

test_that("a wildcard declared twice is refused, naming it, in every form", {
  expect_error(new_wildcard_list(s = c("a", "b"), s = "c"),
               regexp = "wildcard 's' is declared more than once")
  expect_error(new_wildcard_list(s = c("a", "b"), new_wildcard("s", "c")),
               regexp = "wildcard 's' is declared more than once")
  expect_error(new_wildcard_list(new_wildcard("s", "a"), new_wildcard("s", "b")),
               regexp = "wildcard 's' is declared more than once")
})

test_that("distinct names are accepted and keep all their values", {
  w <- new_wildcard_list(s = c("a", "b"), r = "c", S = "d")
  expect_identical(names(w), c("s", "r", "S"))
  expect_identical(names(w$s), c("a", "b"))
})
