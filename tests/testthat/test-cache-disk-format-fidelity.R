## Regression: the result-cache disk-spill format must round-trip the SEMANTIC
## `is_arrow` flag and the per-element `elem_name` faithfully.
##
## `is_raw` and `is_arrow` are distinct: a non-Arrow R object (S4 / list / Seurat)
## is qs2-serialized to RAW bytes (is_raw=TRUE) while its semantic is_arrow is
## FALSE. Conflating them makes a consumer decode qs2 bytes as Arrow and crash
## ("Invalid IPC stream"). Both flags and `elem_name` are persisted as their own
## fields, so a named multi-output keeps its names across a spill.

rt <- function(el) {
  tf <- tempfile(fileext = ".bin")
  on.exit(unlink(tf))
  flumen:::write_result_to_disk(structure(list(el), class = "result_object"), tf)
  flumen:::read_result_from_disk(tf)[[1]]
}

test_that("non-Arrow R object keeps is_arrow=FALSE and its elem_name after spill", {
  orig <- list(a = 1:5, meta = "sampleA")            # generic non-Arrow object
  el   <- list(result    = qs2::qs_serialize(orig, compress_level = 1),  # raw qs2 bytes
               data_type = "R_object",
               is_arrow  = FALSE,
               elem_name = "myout")
  back <- rt(el)

  expect_false(back$is_arrow)
  expect_identical(back$elem_name, "myout")
  expect_true(is.raw(back$result))

  ## and it decodes back to the ORIGINAL object via the correct (qs2) codec
  expect_identical(flumen:::decode_message_data(back$result, back$data_type, back$is_arrow), orig)
})

test_that("Arrow result keeps is_arrow=TRUE after spill", {
  arrow_bytes <- flumen:::serialize_data(list(1:3), "vector", TRUE)$data[[1]]
  el <- list(result = arrow_bytes, data_type = "vector", is_arrow = TRUE, elem_name = NULL)
  back <- rt(el)
  expect_true(back$is_arrow)
  expect_true(is.raw(back$result))
})

test_that("file-path result round-trips (is_arrow=FALSE, char payload, NULL name)", {
  el <- list(result = c("/data/out.txt"), data_type = "file_path",
             is_arrow = FALSE, elem_name = NULL)
  back <- rt(el)
  expect_false(back$is_arrow)
  expect_identical(back$result, "/data/out.txt")
  expect_null(back$elem_name)
})

test_that("elem_name distinguishes NULL from empty string across spill", {
  e_null <- rt(list(result = charToRaw("x"), data_type = "raw", is_arrow = FALSE, elem_name = NULL))
  e_empty <- rt(list(result = charToRaw("x"), data_type = "raw", is_arrow = FALSE, elem_name = ""))
  expect_null(e_null$elem_name)
  expect_identical(e_empty$elem_name, "")
})
