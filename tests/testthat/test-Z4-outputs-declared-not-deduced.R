## Stage 14, piece 3: what a task produced is not deduced from a snapshot of
## its output directories any more. A declared output that exists before the
## task runs is refused (wrapper_refuse_preexisting_outputs, exercised end to
## end by tests/regression/test_rerun_dirty_dir.R and
## test_dynamic_ignores_pre_existing.R); after the run, everything the pattern
## matches is the task's own, and a static output is produced iff it exists.
## Two dynamic patterns of different instances that can match the same names
## in one directory are refused at validation.

test_that("collect_declared_output takes every file the pattern matches, and existence for a static output", {
  d <- file.path(tempdir(), paste0("z4_", basename(tempfile()))); dir.create(d)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  writeLines("a", file.path(d, "s1_a.txt")); writeLines("b", file.path(d, "s1_b.txt"))
  writeLines("x", file.path(d, "other.log"))
  r <- flumen:::collect_declared_output(file.path(d, "s1_{{n}}.txt"), "T")
  expect_null(r$error)
  expect_setequal(basename(r$files), c("s1_a.txt", "s1_b.txt"))
  r2 <- flumen:::collect_declared_output(file.path(d, "s1_{{n}}.csv"), "T")
  expect_match(r2$error, "produced no matching files")
  r3 <- flumen:::collect_declared_output(file.path(d, "other.log"), "T")
  expect_null(r3$error); expect_identical(basename(r3$files), "other.log")
  r4 <- flumen:::collect_declared_output(file.path(d, "missing.log"), "T")
  expect_match(r4$error, "no such file exists after the task ran")
  ## the snapshot-based predicates are gone
  expect_false(exists("static_output_produced", envir = asNamespace("flumen"), inherits = FALSE))
  expect_false(exists("output_changed", envir = asNamespace("flumen"), inherits = FALSE))
})

test_that("produced_files names only what appeared since the start, for the by-product warning", {
  d <- file.path(tempdir(), paste0("z4b_", basename(tempfile()))); dir.create(d)
  on.exit(unlink(d, recursive = TRUE), add = TRUE)
  writeLines("old", file.path(d, "old.txt"))
  snap <- list(); snap[[paste0(d, "\tFALSE")]] <- list(files = "old.txt")
  writeLines("new", file.path(d, "new.txt"))
  expect_identical(flumen:::produced_files(d, FALSE, snap), "new.txt")
})

## The second half of the rule -- no other task may declare a pattern that
## would accept my files -- was already enforced before this stage, by
## patterns_overlap() / validate_no_overlapping_output_globs() and covered by
## tests/regression/test_runtime_expanded_shared_dynamic_output.R (the
## "A_{{n}}.txt swallows A_1's file" arm). Nothing is added here: a second
## rule would only fire first with a worse message.
test_that("overlapping output globs of two tasks are already refused, by witness substitution", {
  expect_true(flumen:::patterns_overlap("A_*", "A_1_*"))
  expect_false(flumen:::patterns_overlap("A_*", "B_*"))
})
