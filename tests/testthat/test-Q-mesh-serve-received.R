## Group Q — Mesh Stage 1: serve-from-received.
## A worker that RECEIVED a P2P output (but did not produce it) re-caches it under a
## composite "exe_id#out_instance" key (complete_p2p_transfer) so it can later serve
## it to a peer; sup_handle_parameter_request falls back to that key when the producer
## key is absent. These tests lock the reconstruction (byte-faithful, all modalities)
## and the serve fallback. The reconstruction mirrors get_result_from_msg; Q1 proves
## the FULL real-function data path (serve -> chunked transfer -> receive -> reconstruct)
## reproduces the producer's cache entry exactly. Pure in-process; the multi-worker
## e2e is exercised only once Stage 3 routes a request to a holder.

## Mirror of the Stage-1 reconstruction (complete_p2p_transfer) for one received entry.
recon_received <- function(data, data_type, is_arrow, elem_name) {
  if (data_type %in% c("file", "file_path")) {
    list(result = qs2::qs_deserialize(flumen:::raw_payload(data)),
         data_type = "file", is_arrow = FALSE, elem_name = elem_name)
  } else {
    list(result = flumen:::raw_payload(data),
         data_type = data_type, is_arrow = isTRUE(is_arrow), elem_name = elem_name)
  }
}

test_that("Q1 — full data path (serve->chunked transfer->receive->reconstruct) == producer cache", {
  b1 <- qs2::qs_serialize(data.frame(x = 1:5))
  b2 <- qs2::qs_serialize(list(10, 20))
  producer <- flumen:::new_result(list(
    list(result = b1, data_type = "R_object",       is_arrow = FALSE, elem_name = "elemA"),
    list(result = b2, data_type = "wrapped:double", is_arrow = FALSE, elem_name = "elemB")))

  inst <- flumen:::get_result_instance(producer, NULL)
  pr <- inst$result; dt <- inst$result_type; ia <- inst$result_is_arrow; dn <- inst$result_elem_names
  n  <- length(pr)
  data <- flumen:::serialize_data(c(pr, "par", "EXE", "WRAP"),
                                  c(dt, rep("R_object", 3)),
                                  c(as.logical(ia), FALSE, FALSE, FALSE))
  message <- list(
    sender = rep("w", n + 3), msg_id = rep("par_WRAP_EXE", n + 3),
    msg_type = c(rep("parameter_requested", n), "parameter_requested_name",
                 "parameter_requested_exe_id", "wrapper_id"),
    data_type = unlist(data$data_type$data_struct), data_names = rep("", n + 3),
    elem_name = c(as.character(dn), "", "", ""), data = data$data,
    is_arrow = data$data_type$is_arrow, file_name = data$file_name)

  addr <- "tcp://127.0.0.1:31961"
  L <- nanonext::socket("pair", listen = addr); Sys.sleep(0.3)
  D <- nanonext::socket("pair", dial = addr);   Sys.sleep(0.6)
  on.exit({ close(D); close(L) }, add = TRUE)
  flumen:::send_message_by_chunks("w", message, L, chunk_size = 10000)
  Sys.sleep(0.6)
  chunked <- list()
  for (k in 1:50) {
    raw <- nanonext::recv(D, mode = "raw", block = 400)
    if (inherits(raw, "errorValue") || is.null(raw)) break
    dec <- flumen:::decode_message_metadata(raw)
    chunked <- flumen:::push_chunked_message(chunked, dec, "par_WRAP_EXE", dec[[1]]$chunk)
  }
  msg <- flumen:::merge_all_chunks(chunked[["par_WRAP_EXE"]])

  recv <- list()
  for (i in seq_along(msg)) if (isTRUE(msg[[i]]$msg_type == "parameter_requested"))
    recv <- c(recv, list(recon_received(msg[[i]]$data, msg[[i]]$data_type,
                                        msg[[i]]$is_arrow, msg[[i]]$elem_name)))
  ri <- flumen:::get_result_instance(flumen:::new_result(recv), NULL)

  expect_identical(ri$result[[1]], b1)
  expect_identical(ri$result[[2]], b2)
  ## "R_object" is a message tag, not a type: announcing it as the wire type
  ## made the receiver pick the wrong decoder, so serialize_data() now emits
  ## "raw" for bytes that are already encoded. The payload is unchanged, which
  ## the qs_deserialize() check below still proves.
  expect_identical(as.character(ri$result_type), c("raw", "wrapped:double"))
  expect_identical(as.character(ri$result_elem_names), c("elemA", "elemB"))
  expect_identical(qs2::qs_deserialize(ri$result[[1]]), data.frame(x = 1:5))
})

test_that("Q2 — reconstruction is byte/type-faithful across all modalities", {
  obj <- qs2::qs_serialize(data.frame(a = 1:3))
  arr <- as.raw(rep(9L, 200))
  f   <- tempfile(fileext = ".dat"); writeBin(as.raw(1:10), f)
  els <- list(
    recon_received(qs2::qs_serialize(f), "file",          FALSE, "f"),    # file: path deserialized
    recon_received(obj,                   "R_object",      FALSE, "o"),
    recon_received(arr,                   "arrow:double",  TRUE,  "a"),
    recon_received(qs2::qs_serialize(1),  "wrapped:double",FALSE, "w"))
  ri <- flumen:::get_result_instance(flumen:::new_result(els), NULL)
  expect_identical(ri$result[[1]], f)                 # file path, re-readable
  expect_true(file.exists(ri$result[[1]]))
  expect_identical(ri$result[[2]], obj)               # object blob identical
  expect_identical(ri$result[[3]], arr)               # arrow blob identical
  expect_identical(as.character(ri$result_type), c("file", "R_object", "arrow:double", "wrapped:double"))
  expect_identical(as.logical(ri$result_is_arrow), c(FALSE, FALSE, TRUE, FALSE))
})

test_that("Q3 — serve fallback resolves a holder composite-key entry (whole payload)", {
  cache <- flumen:::new_memory_cache(dir = tempfile("q3_"))
  blob  <- qs2::qs_serialize(list(7, 8, 9))
  holder_entry <- flumen:::new_result(list(
    list(result = blob, data_type = "R_object", is_arrow = FALSE, elem_name = "")))
  exe <- "EXE123"; out_instance <- 4L
  flumen:::cache_set(cache, paste0(exe, "#", out_instance), holder_entry)  # received, not produced

  ## replicate the serve fallback: producer key absent -> try composite key, serve whole
  results  <- flumen:::cache_get(cache, exe)               # NULL (not produced here)
  inst_arg <- out_instance
  expect_null(results)
  if (is.null(results)) {
    results  <- flumen:::cache_get(cache, paste0(exe, "#", out_instance))
    if (!is.null(results)) inst_arg <- NULL
  }
  ri <- flumen:::get_result_instance(results, inst_arg)
  expect_identical(ri$result[[1]], blob)
  expect_identical(as.character(ri$result_type), "R_object")
})
