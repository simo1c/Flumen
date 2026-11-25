## Unit tests for the reachability-matrix pure logic (R/reachability.R):
## build_reachability() classifies every directed edge and decides pass/fail.
## Edge kinds: control (worker->scheduler, from registration) and p2p
## (worker<->worker, from each worker's report). The scheduler is NEVER a P2P
## endpoint, so there is no scheduler->worker edge -- a co-located local worker
## probes its own peers like any other worker.

test_that("all reachable -> ok, no failures", {
  r <- flumen:::build_reachability(
    all_workers   = c("remote1", "remote2"),
    registered    = c("remote1", "remote2"),
    worker_reports = list(remote1 = c(remote2 = TRUE),
                          remote2 = c(remote1 = TRUE)))
  expect_true(r$ok)
  expect_equal(nrow(r$failures), 0L)
})

test_that("worker <-> worker unreachable -> p2p failure", {
  r <- flumen:::build_reachability(
    all_workers   = c("remote1", "remote2"),
    registered    = c("remote1", "remote2"),
    worker_reports = list(remote1 = c(remote2 = FALSE),
                          remote2 = c(remote1 = FALSE)))
  expect_false(r$ok)
  expect_equal(nrow(r$failures), 2L)
  expect_true(all(r$failures$kind == "p2p"))
})

test_that("worker never registered -> worker->scheduler control failure", {
  r <- flumen:::build_reachability(
    all_workers   = c("remote1", "remote2"),
    registered    = c("remote1"),                 # remote2 missing
    worker_reports = list(remote1 = c(remote2 = TRUE)))
  expect_false(r$ok)
  expect_true(any(r$failures$from == "remote2" &
                  r$failures$to   == "scheduler" &
                  r$failures$kind == "control"))
})

test_that("the scheduler is never a probe source (no scheduler->worker edges)", {
  ## A local worker 'lw' that cannot reach remote1 shows up as a p2p edge from
  ## 'lw' -- NOT as a 'scheduler -> remote1' edge. The scheduler never probes.
  r <- flumen:::build_reachability(
    all_workers    = c("lw", "remote1"),
    registered     = c("lw", "remote1"),
    worker_reports = list(lw = c(remote1 = FALSE), remote1 = c(lw = TRUE)),
    local_workers  = "lw")
  expect_false(any(r$edges$from == "scheduler"))
  expect_false(any(r$edges$kind == "sched-p2p"))
  expect_false(r$ok)                              # lw -> remote1 (p2p) failed
})

test_that("compute_probe_roster includes local workers (no Host) as probers", {
  ## supervisor_info: a LOCAL worker (mac: empty Host, NA data port) + 2 remotes.
  ## The local worker dials OUT to pull data, so it MUST probe the remotes' data
  ## ports -- even though it exposes no inbound TCP endpoint of its own.
  si <- data.frame(
    Supervisor_ID = c("mac", "remote1", "remote2"),
    Host          = c("",    "10.0.0.1", "10.0.0.2"),
    Data_req_port = c(NA_integer_, 61100L, 61100L),
    stringsAsFactors = FALSE)

  roster <- flumen:::compute_probe_roster(si, ctrl_worker_ids = c("mac", "remote1", "remote2"))

  ## the local worker is a prober, targeting BOTH remotes
  expect_true("mac" %in% names(roster))
  expect_equal(sort(as.character(roster[["mac"]]$Supervisor_ID)), c("remote1", "remote2"))

  ## remotes probe the OTHER remote only -- the local worker exposes no TCP
  ## endpoint, so it is never a probe TARGET
  expect_equal(as.character(roster[["remote1"]]$Supervisor_ID), "remote2")
  expect_false("mac" %in% as.character(roster[["remote1"]]$Supervisor_ID))

  ## a worker without a control channel is never asked to probe
  roster2 <- flumen:::compute_probe_roster(si, ctrl_worker_ids = c("remote1", "remote2"))
  expect_false("mac" %in% names(roster2))
})

test_that("compute_probe_roster: no TCP targets -> empty roster (all-local/IPC)", {
  si <- data.frame(
    Supervisor_ID = c("mac1", "mac2"),
    Host          = c("", ""),
    Data_req_port = c(NA_integer_, NA_integer_),
    stringsAsFactors = FALSE)
  expect_equal(length(flumen:::compute_probe_roster(si, c("mac1", "mac2"))), 0L)
})

test_that("format_reachability renders edges and a FAILURES section on failure", {
  r <- flumen:::build_reachability(
    all_workers = c("a", "b"), registered = c("a", "b"),
    worker_reports = list(a = c(b = FALSE), b = c(a = TRUE)))
  txt <- flumen:::format_reachability(r)
  expect_true(any(grepl("who reaches whom", txt)))
  expect_true(any(grepl("FAILURES", txt)))
  expect_true(any(grepl("a -> b : UNREACHABLE", txt, fixed = TRUE)))
})

test_that("per-kind remediation: control + p2p, with drop-local-worker hint", {
  r <- flumen:::build_reachability(
    all_workers    = c("lw", "remote1", "remote2"),
    registered     = c("lw", "remote1"),             # remote2 missing -> control
    worker_reports = list(lw = c(remote1 = FALSE)),  # lw -> remote1 p2p fail (lw is local)
    local_workers  = "lw")
  txt <- paste(flumen:::format_reachability(r), collapse = "\n")
  expect_match(txt, "check start_worker on")                      # control fix
  expect_match(txt, "new_worker(ports=)", fixed = TRUE)           # p2p fix
  expect_match(txt, "drop the local worker 'lw'", fixed = TRUE)   # local-worker p2p hint
})
