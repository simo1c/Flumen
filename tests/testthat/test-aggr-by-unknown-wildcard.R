## Regression: a partial-aggregation `by=` that names a wildcard NOT declared in
## new_wildcard_list() must be rejected at build time.
##
## Bug: an unknown/typo `by=` was silently dropped by intersect() in
## expand_with_partial_aggregation() (object_func.R) and the consumer collapsed
## into a FULL aggregation with NO error and NO warning -- a silent wrong
## grouping (e.g. `by="smaple"` merges all samples instead of grouping per
## sample). validate_aggr_by_wildcards() now fails loudly at new_workflow().

wl <- new_wildcard_list(a = c("a1", "a2"), b = c("b1", "b2"))

build_by <- function(by_val) {
  A <- new_R_task("A",
    input  = new_input(a = new_object_exp("{a}"), b = new_object_exp("{b}")),
    output = new_output(out = new_object("{{out_A}}")),
    exec   = new_script('f <- function(a, b) paste(a, b, sep="_")'))
  B <- new_R_task("B",
    input  = new_input(xs = new_dep("A", value = new_object_aggr("{{out_A}}", by = by_val))),
    output = new_output(out = new_object("{{out_B}}")),
    exec   = new_script('f <- function(xs) length(xs)'))
  tryCatch(
    suppressWarnings(new_workflow("p", "m1", task_list = list(A, B), wildcard_list = wl)),
    error = function(e) e)
}

test_that("a declared by= builds and yields the partial (per-group) instances", {
  wf <- build_by("b")
  expect_false(inherits(wf, "condition"))
  ids <- vapply(wf$task_list, function(op) op$task_id, character(1))
  ## partial aggregation by="b" -> one B instance per {a} value (2), not 1 (full)
  expect_equal(sum(ids == "B"), 2L)
})

test_that("an unknown (typo) by= is rejected with a clear error", {
  wf <- build_by("bb")
  expect_true(inherits(wf, "condition"))
  msg <- conditionMessage(wf)
  expect_match(msg, "not a declared wildcard")
  expect_match(msg, "'bb'")           ## names the offending wildcard
  expect_match(msg, "a, b")           ## lists the known wildcards
})

test_that("a full aggregation (by = NULL) is untouched by the check", {
  A <- new_R_task("A",
    input  = new_input(a = new_object_exp("{a}"), b = new_object_exp("{b}")),
    output = new_output(out = new_object("{{out_A}}")),
    exec   = new_script('f <- function(a, b) paste(a, b, sep="_")'))
  B <- new_R_task("B",
    input  = new_input(xs = new_dep("A", value = new_object_aggr("{{out_A}}"))),
    output = new_output(out = new_object("{{out_B}}")),
    exec   = new_script('f <- function(xs) length(xs)'))
  wf <- tryCatch(
    suppressWarnings(new_workflow("p", "full", task_list = list(A, B), wildcard_list = wl)),
    error = function(e) e)
  expect_false(inherits(wf, "condition"))
  ids <- vapply(wf$task_list, function(op) op$task_id, character(1))
  expect_equal(sum(ids == "B"), 1L)   ## full aggregation -> single B instance
})
