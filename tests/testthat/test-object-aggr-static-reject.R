## Regression: object aggregation with a STATIC {wc} pattern must be rejected at
## build time. Objects aggregate by a DYNAMIC name (new_object_aggr("{{name}}"))
## matching the producer's new_object("{{name}}"); a static {wc} pattern is a
## file-only construct (new_file_aggr). A static OBJECT aggregation is silently
## unmatchable: the producer's static object output is registered, but the
## aggregation resolves to no producer at runtime ("could not be resolved to a
## producer") or hangs. validate_new_workflow() now fails loudly with a pointer
## to the correct pattern.

wl <- new_wildcard_list(a = c("a1", "a2"), b = c("b1", "b2"))

build_obj_aggr <- function(value, prod_out = new_object("{{out_A}}")) {
  A <- new_R_task("A",
    input  = new_input(a = new_object_exp("{a}"), b = new_object_exp("{b}")),
    output = new_output(out = prod_out),
    exec   = new_script('f <- function(a, b) 1L'))
  B <- new_R_task("B",
    input  = new_input(xs = new_dep("A", value = value)),
    output = new_output(out = new_object("{a}_s")),
    exec   = new_script('f <- function(xs) sum(as.integer(unlist(xs)))'))
  tryCatch(
    suppressWarnings(new_workflow("p", "m", task_list = list(A, B), wildcard_list = wl)),
    error = function(e) e)
}

test_that("static-pattern object aggregation is rejected with a clear error", {
  wf <- build_obj_aggr(new_object_aggr("{a}_{b}_v", by = "b"),
                       prod_out = new_object("{a}_{b}_v"))
  expect_true(inherits(wf, "condition"))
  ## Collapse the whitespace first: the message is a labelled block whose long
  ## fields wrap, so a break can fall between any two words and would otherwise
  ## silence an assertion that the words are still there.
  msg <- gsub("[[:space:]]+", " ", conditionMessage(wf))
  expect_match(msg, "static wildcard pattern")
  expect_match(msg, "\\{\\{name\\}\\}")     ## points at the dynamic form
  expect_match(msg, "new_file_aggr")          ## says where a static {wc} IS valid
  expect_match(msg, "\\{a\\}_\\{b\\}_v")      ## echoes the offending pattern
})

test_that("full (by=NULL) static-pattern object aggregation is also rejected", {
  wf <- build_obj_aggr(new_object_aggr("{a}_{b}_v"),
                       prod_out = new_object("{a}_{b}_v"))
  expect_true(inherits(wf, "condition"))
  expect_match(conditionMessage(wf), "static wildcard pattern")
})

test_that("dynamic-name object aggregation is accepted (no false positive)", {
  wf <- build_obj_aggr(new_object_aggr("{{out_A}}", by = "b"))
  expect_false(inherits(wf, "condition"))
})

test_that("static-pattern FILE aggregation stays exempt", {
  P <- new_shell_task("P",
    input  = new_input(a = new_object_exp("{a}"), b = new_object_exp("{b}")),
    output = new_output(o = new_file("{a}_{b}.txt", auto_mkdir = FALSE)),
    exec   = new_script('echo hi > {a}_{b}.txt'))
  C <- new_R_task("C",
    input  = new_input(fs = new_dep("P", value = new_file_aggr("{a}_{b}.txt", by = "b"))),
    output = new_output(o = new_object("{a}_s")),
    exec   = new_script('f <- function(fs) length(fs)'))
  wf <- tryCatch(
    suppressWarnings(new_workflow("pf", "file", task_list = list(P, C), wildcard_list = wl)),
    error = function(e) e)
  expect_false(inherits(wf, "condition"))
})
