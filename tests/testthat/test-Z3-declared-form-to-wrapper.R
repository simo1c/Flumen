## The container a parameter is delivered in is DECLARED to the wrapper
## (fixed dispatch field params_form) and read there, rather than inferred
## from how many same-named slots the message carries.

enc  <- function(v) qs2::qs_serialize(v)
slot <- function(name, v, type) list(data_names = name, data_type = type, data = enc(v),
                                     is_arrow = FALSE, elem_name = "")

test_that("the wrapper builds the container from the declared form, not from the slot count", {
  msg <- list(slot("xs", 1, "numeric"), slot("k", "a", "character"))
  r <- flumen:::aggregate_params(msg, forms = c("aggregate", "bare"))
  expect_true(is.list(r$xs)); expect_length(r$xs, 1L); expect_identical(r$xs[[1]], 1)
  expect_identical(r$k, "a")
  ## the same message with the other declaration gives the other container
  r2 <- flumen:::aggregate_params(msg, forms = c("bare", "list"))
  expect_identical(r2$xs, 1)
  expect_true(is.list(r2$k)); expect_length(r2$k, 1L)
})

test_that("a disagreement between the declaration and the message is refused, never guessed", {
  two <- list(slot("xs", 1, "numeric"), slot("xs", 2, "numeric"))
  expect_error(flumen:::aggregate_params(two, forms = "bare"), "message carries 2")
  one <- list(slot("xs", 1, "numeric"), slot("k", "a", "character"))
  expect_error(flumen:::aggregate_params(one, forms = "bare"), "1 parameter form")
})

## One contract only. There is no "without a
## declaration" case: a message that does not carry it comes from another build
## of flumen, and whoever reads it stops instead of falling back to the rule
## that counted the slots.
test_that("the forms are required: there is no un-declared path left", {
  two <- list(slot("xs", 1, "numeric"), slot("xs", 2, "numeric"))
  expect_error(flumen:::aggregate_params(two), "forms")
  expect_false("forms" %in% names(formals(flumen:::aggregate_params))[
    vapply(formals(flumen:::aggregate_params), function(d) !identical(d, quote(expr = )), logical(1))])
})

test_that("the fixed dispatch fields name params_form as the ninth", {
  expect_identical(flumen:::FIXED_DISPATCH_FIELD_NAMES[9], "params_form")
  expect_identical(flumen:::N_FIXED_DISPATCH_FIELDS, 9L)
})
