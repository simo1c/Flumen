## Stage 11: the supervisor deduces nothing about where a dependency's data
## is, how it is obtained, which constituent a row refers to, what template it
## carries, or in what form the parameter is delivered. The scheduler declares
## every one of those. This test keeps the removed deductions from returning:
## each entry is a code shape that implemented one of them, searched in the
## package sources with comment lines stripped. It runs only where the sources
## are reachable (the suite is run from the source tree, see
## tests/tools/RIENTRO.md).

r_dir <- testthat::test_path("..", "..", "R")

strip_comments <- function(lines) sub("#.*$", "", lines)

sources <- function() {
  files <- list.files(r_dir, pattern = "\\.R$", full.names = TRUE)
  setNames(lapply(files, function(f) strip_comments(readLines(f, warn = FALSE))), basename(files))
}

deductions <- list(
  "1  co-location inferred from the supervisor's own registry" =
    "exe_id_to_req %in% names(state$reg_tasks)",
  "2  same machine inferred from is_local and is_remote" =
    "producer_is_local && !state$is_remote",
  "4  constituent chosen by position (fallback to the first remaining)" =
    "matching_req <- item",
  "5  template read from the first output_req still pending" =
    "if(!is.null(.rem)) .rem$dyn_wildcard",
  "7  container form inferred from the cache result count" =
    "(is.null(instance) && length(results) > 1)",
  "9b per-parameter locality vector (a second record of the constituents)" =
    "par_list_locality",
  "10 per-name type collapse (one R_object slot speaks for the group)" =
    'if (any(slots == "R_object")) "R_object"',
  "11 rows released at producer finish without a delivery" =
    'state$wrapper_status[w, "Status"]           <- "ready"',
  "14 delivered container decided by the gather flag and slot counts" =
    "slot_is_aggr      <- rep(params_is_aggr_orig",
  "is_local, the flag that meant two things" =
    "is_local = is_local"
)

test_that("none of the removed deductions is back in the sources", {
  skip_if_not(dir.exists(r_dir), "package sources not reachable from here")
  src <- sources()
  for (nm in names(deductions)) {
    hits <- vapply(src, function(lines) any(grepl(deductions[[nm]], lines, fixed = TRUE)), logical(1))
    expect_false(any(hits), info = paste0(nm, " found in: ", paste(names(src)[hits], collapse = ", ")))
  }
})

test_that("the one way to read a constituent is by producer identity", {
  skip_if_not(dir.exists(r_dir), "package sources not reachable from here")
  src <- sources()
  ## the accessor exists and is used by every reader of a constituent's fields
  expect_true(any(grepl("output_req_for <- function", src[["p2p_dispatch.R"]], fixed = TRUE)))
  for (f in c("supervisor_deps.R", "supervisor_handlers.R"))
    expect_true(any(grepl("output_req_for(", src[[f]], fixed = TRUE)), info = f)
  ## no positional walk over a container's constituents outside the accessor
  walkers <- vapply(src[setdiff(names(src), "p2p_dispatch.R")], function(lines)
    any(grepl("for(.it in actual_param", lines, fixed = TRUE) |
        grepl("for(item in par_val_req)", lines, fixed = TRUE)), logical(1))
  expect_false(any(walkers), info = paste(names(walkers)[walkers], collapse = ", "))
})
