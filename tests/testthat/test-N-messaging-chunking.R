## Group N — Messaging: chunked transport.
## new_message() must divide chunk_size across a message's elements
## (eff_chunk_size), so a multi-element message stays within one chunk_size on
## the wire and keeps the K=2 in-flight RAM bound.

## Build every wire chunk of a multi-element message, then decode + reassemble.
## Returns list(sizes = <wire message byte sizes>, merged = <reassembled elements>).
build_and_reassemble <- function(data, chunk_size){
  n_el  <- length(data)
  dtype <- rep("R_object", n_el)
  dname <- paste0("e", seq_len(n_el))
  mk <- function(i) flumen:::new_message(
    sender = "s", msg_id = "mid", msg_type = "result",
    data_type = dtype, data_names = dname, data = data,
    is_arrow = FALSE, file_name = "", chunk = i, chunk_size = chunk_size)
  n_chunks <- mk(1L)$n_chunks
  wire  <- lapply(seq_len(n_chunks), function(i) mk(i)$message)
  sizes <- vapply(wire, length, numeric(1))
  chunked <- list()
  for(i in seq_len(n_chunks))
    chunked <- flumen:::push_chunked_message(
      chunked, flumen:::decode_message_metadata(wire[[i]]), "mid", i)
  list(sizes = sizes, merged = flumen:::merge_all_chunks(chunked[["mid"]]))
}

## Header allowance: the atomic Arrow-IPC header is prepended whole to every
## wire message (~2-4 KB even for dozens of elements). 50 KB is a generous
## ceiling for it.
HEADER_SLACK <- 50000

test_that("N1 — a multi-element message stays bounded by chunk_size on the wire", {
  set.seed(1)
  chunk_size <- 1e5
  ## 3 elements x 80 KB: counted per element each has n_ch = 1 (80 KB < 100 KB),
  ## and the whole message goes out as a single ~240 KB wire message >> chunk_size.
  data <- lapply(1:3, function(i) as.raw(sample(0:255, 80000, replace = TRUE)))
  rt <- build_and_reassemble(data, chunk_size)

  expect_lte(max(rt$sizes), chunk_size + HEADER_SLACK)
  ## round-trip: every element reconstructed byte-identical
  for(i in seq_along(data))
    expect_identical(flumen:::raw_payload(rt$merged[[i]]$data), data[[i]])
})

test_that("N2 — a many-element message (fan-in shape) stays bounded too", {
  set.seed(2)
  chunk_size <- 1e5
  data <- lapply(1:25, function(i) as.raw(sample(0:255, 80000, replace = TRUE)))
  rt <- build_and_reassemble(data, chunk_size)

  expect_lte(max(rt$sizes), chunk_size + HEADER_SLACK)
  for(i in seq_along(data))
    expect_identical(flumen:::raw_payload(rt$merged[[i]]$data), data[[i]])
})

test_that("N3 — a single-element message is chunked normally and round-trips", {
  set.seed(3)
  chunk_size <- 1e5
  ## 250 KB element, no per-element division (n_elements == 1) -> 3 chunks.
  data <- list(as.raw(sample(0:255, 250000, replace = TRUE)))
  rt <- build_and_reassemble(data, chunk_size)

  expect_gt(length(rt$sizes), 1L)
  expect_lte(max(rt$sizes), chunk_size + HEADER_SLACK)
  expect_identical(flumen:::raw_payload(rt$merged[[1]]$data), data[[1]])
})
