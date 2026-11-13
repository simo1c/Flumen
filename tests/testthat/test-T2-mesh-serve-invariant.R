## Group T2 — serving_count is an authoritative-set mirror that cannot
## leak. Locks the three historical leak paths (per-sample/select, coalescing overwrite,
## no-transfer resolve) via the two release triggers, plus crash cleanup and the
## no-double-decrement guarantee. Pure in-process (mirrors test-S setup).

mk_state <- function() list(
  holders         = new.env(hash = TRUE, parent = emptyenv()),
  data_to_workers = new.env(hash = TRUE, parent = emptyenv()),
  serving_count   = new.env(hash = TRUE, parent = emptyenv()),
  serving_pending = new.env(hash = TRUE, parent = emptyenv()))

sup_info <- data.frame(
  Supervisor_ID   = c("prod", "h1", "h2", "consumer"),
  Socket_data_req = c("tcp://prod:31000", "tcp://h1:31000", "tcp://h2:31000", "tcp://cons:31000"),
  Host            = c("prodH", "h1H", "h2H", "consH"),
  Uses_tcp        = c(TRUE, TRUE, TRUE, TRUE),
  stringsAsFactors = FALSE)

## A req whose producer output is EXE9#2, held by h1 (so reroute picks h1).
mk_req <- function() flumen:::new_output_req("dw", "EXE9", 2L, "tcp://prod:31000", uses_tcp = TRUE)
route  <- function(s, consumer, exe) flumen:::mesh_reroute_output_req(mk_req(), consumer, s, sup_info,
                                                                     consuming_exe_id = exe)
cnt    <- function(s, w) { v <- s$serving_count[[w]]; if (is.null(v)) 0L else as.integer(v) }

## h1 is the SOLE holder and the producer is left out of data_to_workers, so every route
## deterministically picks h1 (routing choice is covered in test-S; here we isolate the
## serving_count invariant).
setup_h1 <- function() { s <- mk_state(); s$holders[["EXE9#2"]] <- "h1"; s }

test_that("T2.1 — coalescing: N fetches (same worker+key, distinct tasks) each counted, not overwritten", {
  s <- setup_h1()
  route(s, "consumer", "cA"); route(s, "consumer", "cB"); route(s, "consumer", "cC")
  expect_equal(cnt(s, "h1"), 3L)                       # was 1 under the overwrite bug
  expect_equal(length(ls(s$serving_pending)), 3L)      # three distinct pending entries
})

test_that("T2.2 — early release frees ALL coalesced serves on the shared key at once", {
  s <- setup_h1()
  route(s, "consumer", "cA"); route(s, "consumer", "cB"); route(s, "consumer", "cC")
  s <- flumen:::mesh_release_serve(s, "consumer", "EXE9#2")   # one transfer satisfies all waiters
  expect_equal(cnt(s, "h1"), 0L)
  expect_equal(length(ls(s$serving_pending)), 0L)
})

test_that("T2.3 — backstop releases by exe_id when NO early signal ever arrives (select path)", {
  s <- setup_h1()
  route(s, "consumer", "cA"); route(s, "consumer", "cB")
  expect_equal(cnt(s, "h1"), 2L)
  s <- flumen:::mesh_release_by_exe_id(s, "cA")        # cA finished; cB still running
  expect_equal(cnt(s, "h1"), 1L)                       # only cA's serve released
  s <- flumen:::mesh_release_by_exe_id(s, "cB")
  expect_equal(cnt(s, "h1"), 0L)                       # no permanent leak
})

test_that("T2.4 — backstop is task-scoped: a sibling's RESULT does not free another task's serve", {
  s <- setup_h1()
  route(s, "consumer", "cA")
  s <- flumen:::mesh_release_by_exe_id(s, "cOTHER")    # unrelated task on same worker
  expect_equal(cnt(s, "h1"), 1L)                       # cA's serve untouched
})

test_that("T2.5 — no double-decrement: early release then backstop stays at 0 (no underflow)", {
  s <- setup_h1()
  route(s, "consumer", "cA")
  s <- flumen:::mesh_release_serve(s, "consumer", "EXE9#2")  # early
  expect_equal(cnt(s, "h1"), 0L)
  s <- flumen:::mesh_release_by_exe_id(s, "cA")              # backstop finds nothing
  expect_equal(cnt(s, "h1"), 0L)                            # never goes negative
})

test_that("T2.6 — crash cleanup releases serves the dead worker sourced, and clears its mirror", {
  s <- setup_h1()
  route(s, "consumer", "cA"); route(s, "consumer", "cB")
  expect_equal(cnt(s, "h1"), 2L)
  s <- flumen:::drop_serving_worker(s, "h1")
  expect_equal(cnt(s, "h1"), 0L)
  expect_false("h1" %in% ls(s$serving_count))          # mirror entry removed entirely
  expect_equal(length(ls(s$serving_pending)), 0L)
})

test_that("T2.7 — crash of the CONSUMER also frees the serves routed to feed it", {
  s <- setup_h1()
  route(s, "consumer", "cA")
  s <- flumen:::drop_serving_worker(s, "consumer")
  expect_equal(cnt(s, "h1"), 0L)                       # source h1's serve for the dead consumer freed
})
