## check_stalled_transfers() fails a peer-to-peer fetch when no NEW chunk has
## arrived for FLUMEN_STALLED_TRANSFER_SECS (600 s by default), measured from the
## last chunk that arrived. Measuring from the START of the fetch would fail a
## healthy transfer running for 600 s in total after about 10 s without a new
## chunk, with "no data arrived ... for 600s" (R/supervisor_handlers.R).

## A supervisor state holding one fetch of parameter x from producer PROD.
stalled_state <- function(started_ago, chunks = 0L, last_progress_ago = NULL) {
  st <- new.env()
  st$worker_name <- "w"
  st$thresholds  <- NULL
  st$pending_resolutions_env <- flumen:::p2p_make_env()
  st$par_req_chunked_message <- list()
  st$reg_tasks <- list()
  key <- flumen:::p2p_make_key("PROD", NULL, FALSE)
  flumen:::p2p_register_or_check(st$pending_resolutions_env, key,
                                 list(wrapper_id = "W1", par_name = "x"))
  e <- get(key, envir = st$pending_resolutions_env)
  e$started <- Sys.time() - started_ago
  if (!is.null(last_progress_ago)) {
    e$seen <- chunks
    e$last_progress <- Sys.time() - last_progress_ago
  }
  assign(key, e, envir = st$pending_resolutions_env)
  if (chunks > 0L) st$par_req_chunked_message[["x_W1_PROD"]] <- as.list(seq_len(chunks))
  st
}

## Runs n checks and returns the error messages they reported.
run_checks <- function(st, n) {
  sent <- character(0)
  local_mocked_bindings(
    send_error_notify_to_scheduler = function(state, task_id, exe_id, message, ...)
      sent <<- c(sent, message),
    to_log = function(...) invisible(NULL),
    .package = "flumen")
  for (i in seq_len(n)) flumen:::check_stalled_transfers(st)
  sent
}

test_that("a long healthy transfer is not failed after a short quiet spell", {
  st <- stalled_state(started_ago = 1200)
  st$par_req_chunked_message[["x_W1_PROD"]] <- list(1)   ## a chunk arrives now
  expect_length(run_checks(st, 1), 0)                     ## progress observed
  expect_length(run_checks(st, 5), 0)                     ## ~10 s quiet: still fine
})

test_that("a transfer whose last chunk is older than the threshold is failed", {
  st <- stalled_state(started_ago = 1200, chunks = 3L, last_progress_ago = 700)
  sent <- run_checks(st, 5)
  expect_length(sent, 1)
  expect_match(sent, "no data arrived")
})

test_that("a fetch that never received a chunk is failed once the threshold passes", {
  st <- stalled_state(started_ago = 700)
  expect_length(run_checks(st, 5), 1)
})

test_that("a fetch that just started is left alone", {
  st <- stalled_state(started_ago = 10)
  expect_length(run_checks(st, 5), 0)
})
