## Group P — PAIR control channels (sock_ctrl / sock_term) + worker-side heartbeat.
## Both channels are PAIR, so the WORKER -- the dialer on every channel -- can
## also SEND on them (its keepalive heartbeat). That keeps the "dialer = sender"
## invariant NNG needs to detect a broken pipe and auto-reconnect. These tests
## lock bidirectional integrity, the chunked-dispatch path over PAIR, the dialer's
## auto-reconnect, and send_aio buffering of the handshake before connect.
## Pure in-process (nanonext sockets + flumen internals); no subprocesses.

drain_serial <- function(s, max = 1000L) {
  out <- vector("list", 0)
  for (i in seq_len(max)) {
    m <- nanonext::recv(s, mode = "serial", block = 300)
    if (inherits(m, "errorValue") || is.null(m)) break
    out[[length(out) + 1L]] <- m
  }
  unlist(out)
}

test_that("P1 — PAIR is full-duplex: dispatch down + heartbeat up, lossless and ordered", {
  addr <- "tcp://127.0.0.1:31931"
  L <- nanonext::socket("pair", listen = addr); Sys.sleep(0.3)   # scheduler
  D <- nanonext::socket("pair", dial   = addr); Sys.sleep(0.6)   # worker
  on.exit({ close(D); close(L) }, add = TRUE)
  N <- 200
  for (i in seq_len(N)) {
    nanonext::send_aio(L,  i, mode = "serial")   # dispatch chunk (down)
    nanonext::send_aio(D, -i, mode = "serial")   # heartbeat      (up)
  }
  Sys.sleep(0.6)
  gotD <- drain_serial(D)   # worker received the dispatch stream
  gotL <- drain_serial(L)   # scheduler received the heartbeat stream
  expect_identical(gotD, seq_len(N))             # all, in order
  expect_identical(gotL, -seq_len(N))            # all, in order, concurrent direction
})

test_that("P2 — send_message_by_chunks reassembles correctly over a PAIR socket", {
  addr <- "tcp://127.0.0.1:31932"
  L <- nanonext::socket("pair", listen = addr); Sys.sleep(0.3)
  D <- nanonext::socket("pair", dial   = addr); Sys.sleep(0.6)
  on.exit({ close(D); close(L) }, add = TRUE)
  big <- as.raw(rep(42L, 50000))
  msg <- list(sender = rep("s", 3), msg_id = "mid", msg_type = rep("result", 3),
              data_type = rep("R_object", 3), data_names = rep("", 3),
              data = list(big, big, big), is_arrow = rep(FALSE, 3), file_name = rep("", 3))
  flumen:::send_message_by_chunks("s", msg, L, chunk_size = 20000)  # forces multiple chunks
  Sys.sleep(0.8)
  chunked <- list(); nrecv <- 0L
  for (k in 1:50) {
    raw <- nanonext::recv(D, mode = "raw", block = 500)
    if (inherits(raw, "errorValue") || is.null(raw)) break
    nrecv <- nrecv + 1L
    dec <- flumen:::decode_message_metadata(raw)
    chunked <- flumen:::push_chunked_message(chunked, dec, "mid", dec[[1]]$chunk)
  }
  merged <- flumen:::merge_all_chunks(chunked[["mid"]])
  expect_gt(nrecv, 1L)                                            # actually chunked
  expect_length(merged, 3)
  expect_identical(flumen:::raw_payload(merged[[1]]$data), big)
  expect_identical(flumen:::raw_payload(merged[[3]]$data), big)
})

test_that("P3 — PAIR dialer auto-reconnects after the listener restarts (detected drop)", {
  addr <- "tcp://127.0.0.1:31933"
  L <- nanonext::socket("pair", listen = addr); Sys.sleep(0.3)
  D <- nanonext::socket("pair", dial   = addr); Sys.sleep(0.6)
  nanonext::send_aio(L, "m1", mode = "serial")
  expect_identical(nanonext::recv(D, mode = "serial", block = 2000), "m1")
  close(L); Sys.sleep(0.8)
  L2 <- nanonext::socket("pair", listen = addr); Sys.sleep(2.5)   # dialer re-dials (reconnect-time ~1s)
  on.exit({ close(D); close(L2) }, add = TRUE)
  nanonext::send_aio(L2, "m2", mode = "serial")
  expect_identical(nanonext::recv(D, mode = "serial", block = 3000), "m2")
})

test_that("P4 — send_aio buffers a message sent before the peer dials (handshake path)", {
  addr <- "tcp://127.0.0.1:31934"
  L <- nanonext::socket("pair", listen = addr); Sys.sleep(0.2)
  nanonext::send_aio(L, as.raw(c(1, 2, 3, 4)), mode = "raw")      # sent with NO peer yet
  Sys.sleep(0.4)
  D <- nanonext::socket("pair", dial = addr); Sys.sleep(0.7)
  on.exit({ close(D); close(L) }, add = TRUE)
  got <- nanonext::recv(D, mode = "raw", block = 2500)
  expect_false(inherits(got, "errorValue"))
  expect_identical(as.integer(got), c(1L, 2L, 3L, 4L))
})
