## Group R — Mesh Stage 2: holder visibility (data_received signal + holder index).
## When a worker receives a COMPLETE output via P2P it re-caches it (Stage 1) and emits
## a `data_received(exe_id, out_instance)` notification; the scheduler records the worker
## in its holder index (holders[["exe_id#out_instance"]]). These tests lock the wire
## round-trip of the signal and the index update / crash-prune logic. The index is inert
## until Stage 3 routing reads it. Pure in-process.

minimal_state <- function() list(holders = new.env(hash = TRUE, parent = emptyenv()))

test_that("R1 — data_received signal round-trips (exe_id, out_instance) over the wire", {
  addr <- "tcp://127.0.0.1:31971"
  L <- nanonext::socket("push", listen = addr); Sys.sleep(0.3)
  D <- nanonext::socket("pull", dial = addr);   Sys.sleep(0.5)
  on.exit({ close(D); close(L) }, add = TRUE)

  flumen:::send_data_received_to_central_scheduler(L, "worker7", "EXE9", 3L)
  raw <- nanonext::recv(D, mode = "raw", block = 2000)
  dec <- flumen:::decode_message_metadata(raw)
  expect_identical(dec[[1]]$msg_type, "data_received")
  expect_identical(dec[[1]]$sender, "worker7")
  exe <- flumen:::decode_message_data(dec[[1]]$data, dec[[1]]$data_type, dec[[1]]$is_arrow)
  oi  <- flumen:::decode_message_data(dec[[2]]$data, dec[[2]]$data_type, dec[[2]]$is_arrow)
  expect_identical(exe, "EXE9")
  expect_identical(oi, "3")
})

test_that("R2 — NULL out_instance (aggregated/whole output) travels as empty string", {
  addr <- "tcp://127.0.0.1:31972"
  L <- nanonext::socket("push", listen = addr); Sys.sleep(0.3)
  D <- nanonext::socket("pull", dial = addr);   Sys.sleep(0.5)
  on.exit({ close(D); close(L) }, add = TRUE)

  flumen:::send_data_received_to_central_scheduler(L, "w", "EXEa", NULL)
  raw <- nanonext::recv(D, mode = "raw", block = 2000)
  dec <- flumen:::decode_message_metadata(raw)
  oi  <- flumen:::decode_message_data(dec[[2]]$data, dec[[2]]$data_type, dec[[2]]$is_arrow)
  expect_identical(oi, "")   # NULL out_instance -> "" -> key "EXEa#"
})

test_that("R3 — mark_holder records workers per (exe_id, out_instance); dedups", {
  s <- minimal_state()
  s <- flumen:::mark_holder(s, "EXE9", "3", "w1")
  s <- flumen:::mark_holder(s, "EXE9", "3", "w2")
  s <- flumen:::mark_holder(s, "EXE9", "3", "w1")   # duplicate -> no-op
  s <- flumen:::mark_holder(s, "EXEa", "",  "w3")   # different output (aggregated)
  expect_setequal(s$holders[["EXE9#3"]], c("w1", "w2"))
  expect_identical(s$holders[["EXEa#"]], "w3")
})

test_that("R4 — drop_holder_worker prunes a crashed worker from every entry", {
  s <- minimal_state()
  s <- flumen:::mark_holder(s, "EXE9", "3", "w1")
  s <- flumen:::mark_holder(s, "EXE9", "3", "w2")
  s <- flumen:::mark_holder(s, "EXEb", "",  "w2")
  s <- flumen:::drop_holder_worker(s, "w2")          # w2 crashes
  expect_identical(s$holders[["EXE9#3"]], "w1")      # w2 removed, w1 stays
  expect_false("EXEb#" %in% ls(s$holders))           # entry emptied -> removed
})

test_that("R5 — holder key matches the supervisor's composite cache key format", {
  ## supervisor caches under paste0(exe_id, "#", out_instance_str); the scheduler keys
  ## holders the same way, so a future Stage-3 lookup lines up with what was cached.
  exe <- "EXE9"; out_str <- "3"
  s <- flumen:::mark_holder(minimal_state(), exe, out_str, "w1")
  expect_true(paste0(exe, "#", out_str) %in% ls(s$holders))
})
