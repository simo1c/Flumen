## A shell task receives its parameters as text. The wrapper built that text
## with unlist() / c(), which strip the class and keep the storage: a Date
## reached the tool as 19797, a factor as its codes, an integer64 as a
## meaningless double, silently. shell_arg_text() (R/wrapper_lib.R) renders a
## classed value with as.character(), and leaves everything else untouched.

test_that("classed values become their own text", {
  expect_identical(flumen:::shell_arg_text(as.Date("2024-03-15")), "2024-03-15")
  expect_identical(flumen:::shell_arg_text(factor(c("lo", "hi"))), c("lo", "hi"))
  expect_identical(flumen:::shell_arg_text(as.POSIXct("2024-03-15 10:20:30", tz = "UTC")),
                   "2024-03-15 10:20:30")
  expect_identical(flumen:::shell_arg_text(bit64::as.integer64(2)^40), "1099511627776")
})

test_that("plain values are returned exactly as they were", {
  for (v in list(100000, 3.14, -1, 5L, "a b", TRUE, NA, c(1, 2, 3), c("x", "y")))
    expect_identical(flumen:::shell_arg_text(v), v)
})

test_that("a list (an aggregated value) is converted element by element", {
  expect_identical(flumen:::shell_arg_text(list(as.Date("2024-03-15"), "p.txt")),
                   list("2024-03-15", "p.txt"))
  expect_identical(flumen:::shell_arg_text(list(1, 2)), list(1, 2))
})
