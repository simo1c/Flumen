## Group B — Value-class hierarchy.
## Build-time unit tests on the constructors in R/fs.R. The DSL chains
## classes (e.g. expansion -> object) so the scheduler can dispatch on
## class membership; these tests guard the exact class composition.

test_that("B1 — base constructors set base classes", {
  obj <- new_object("foo")
  expect_true("flumen_object" %in% class(obj))

  fil <- new_file("a/b.txt")
  expect_true(all(c("file_object", "file_path", "flumen_path") %in% class(fil)))
})

test_that("B2 — _exp variants add 'expansion' to the class chain", {
  obj <- new_object_exp("{x}")
  expect_true("expansion" %in% class(obj))
  expect_true("flumen_object" %in% class(obj))

  fil <- new_file_exp("{x}.fa")
  expect_true(all(c("expansion", "file_object") %in% class(fil)))
})

test_that("B3 — _aggr variants add 'aggregation' to the class chain", {
  obj <- new_object_aggr("{x}")
  expect_true("aggregation" %in% class(obj))
  expect_false("partial_aggregation" %in% class(obj))

  fil <- new_file_aggr("{x}.tsv")
  expect_true("aggregation" %in% class(fil))
  expect_false("partial_aggregation" %in% class(fil))
})

test_that("B4 — _aggr with by= becomes partial_aggregation (subclass of aggregation)", {
  obj <- new_object_aggr("{x}_{y}", by = "y")
  expect_true("partial_aggregation" %in% class(obj))
  ## partial_aggregation must remain an aggregation for downstream dispatch.
  expect_true("aggregation" %in% class(obj))

  fil <- new_file_aggr("{x}_{y}.tsv", by = "y")
  expect_true("partial_aggregation" %in% class(fil))
  expect_true("aggregation" %in% class(fil))
})
