## Group S — least-connections source routing.
## At consumer dispatch the scheduler rewrites each output_req's transport triple to a
## least-loaded peer holder (if any holds the data), else leaves it pointing at the
## producer (pre-mesh behaviour). These tests lock the selection policy, triple
## coherence, self-avoidance, and the serving_count book-keeping. Pure in-process.

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

mk_req <- function() flumen:::new_output_req("dw", "EXE9", 2L, "tcp://prod:31000", uses_tcp = TRUE)

test_that("S1 — no peer holder -> request unchanged (producer)", {
  s <- mk_state(); s$data_to_workers[["EXE9"]] <- "prod"
  out <- flumen:::mesh_reroute_output_req(mk_req(), "consumer", s, sup_info)
  expect_identical(out$socket, "tcp://prod:31000")          # untouched
  expect_equal(length(ls(s$serving_count)), 0L)             # nothing routed
})

test_that("S2 — busy producer + idle holder -> rerouted to the holder (coherent triple)", {
  s <- mk_state(); s$data_to_workers[["EXE9"]] <- "prod"
  s$holders[["EXE9#2"]] <- "h1"
  s$serving_count[["prod"]] <- 5L                            # producer busy
  out <- flumen:::mesh_reroute_output_req(mk_req(), "consumer", s, sup_info)
  expect_identical(out$socket, "tcp://h1:31000")            # least-loaded -> holder
  expect_true(out$uses_tcp)                                 # triple from h1's row
  expect_null(out$is_local)                                # the flag is gone: source / fetch say where (stage 11)
})

test_that("S3 — least-connections among multiple holders", {
  s <- mk_state(); s$data_to_workers[["EXE9"]] <- "prod"
  s$holders[["EXE9#2"]] <- c("h1", "h2")
  s$serving_count[["prod"]] <- 5L; s$serving_count[["h1"]] <- 3L; s$serving_count[["h2"]] <- 1L
  out <- flumen:::mesh_reroute_output_req(mk_req(), "consumer", s, sup_info)
  expect_identical(out$socket, "tcp://h2:31000")            # h2 has the fewest serves
})

test_that("S4 — never reroutes to the consumer's own worker", {
  s <- mk_state(); s$data_to_workers[["EXE9"]] <- "prod"
  s$holders[["EXE9#2"]] <- c("h1", "h2")
  s$serving_count[["prod"]] <- 5L; s$serving_count[["h2"]] <- 9L  # h2 busy, h1 idle (0)
  out <- flumen:::mesh_reroute_output_req(mk_req(), "h1", s, sup_info)
  expect_false(identical(out$socket, "tcp://h1:31000"))     # h1 is the consumer -> excluded
  expect_identical(out$socket, "tcp://prod:31000")          # remaining min (prod=5 < h2=9)
})

test_that("S5 — serving_count bumps on route and is released on completion", {
  s <- mk_state(); s$data_to_workers[["EXE9"]] <- "prod"
  s$holders[["EXE9#2"]] <- "h1"; s$serving_count[["prod"]] <- 5L
  flumen:::mesh_reroute_output_req(mk_req(), "consumer", s, sup_info)
  expect_equal(as.integer(s$serving_count[["h1"]]), 1L)     # routed -> +1
  s <- flumen:::mesh_release_serve(s, "consumer", "EXE9#2")
  expect_equal(as.integer(s$serving_count[["h1"]]), 0L)     # completed -> -1
  expect_equal(length(ls(s$serving_pending)), 0L)           # pending cleared
})

test_that("S6 — mesh_release_serve on an unknown fetch is a no-op", {
  s <- mk_state()
  expect_silent(s2 <- flumen:::mesh_release_serve(s, "nobody", "X#0"))
})
