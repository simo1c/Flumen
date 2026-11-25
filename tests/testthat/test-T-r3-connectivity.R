## register_worker_p2p_endpoint ONLY fills the worker's P2P endpoint (Host +
## Socket_data_req). Reachability and the abort-on-unreachable decision belong to
## the connectivity-check phase, whose matrix semantics are characterised in
## test-U-reachability.R.

mk_supinfo <- function(drp){
  data.frame(Supervisor_ID = "w1", PID = NA_integer_,
             Socket_data_req = "", Socket_ctrl = "", Socket_term = "",
             Host = "", Port_base = NA_integer_, Data_req_port = as.integer(drp),
             Uses_tcp = TRUE, stringsAsFactors = FALSE)
}

test_that("register_worker_p2p_endpoint fills Host + Socket_data_req", {
  out <- flumen:::register_worker_p2p_endpoint(mk_supinfo(31000L), "w1", "10.0.0.7")
  expect_equal(out$Host[1], "10.0.0.7")
  expect_equal(out$Socket_data_req[1], "tcp://10.0.0.7:31000")
})

test_that("fills even an unreachable host, with no teardown", {
  ## 192.0.2.1 is non-routable. Filling the endpoint does not probe it: the
  ## probe happens in the reachability phase.
  out <- flumen:::register_worker_p2p_endpoint(mk_supinfo(80L), "w1", "192.0.2.1")
  expect_equal(out$Host[1], "192.0.2.1")
  expect_equal(out$Socket_data_req[1], "tcp://192.0.2.1:80")
})

test_that("skips a worker with no data_req port (local/IPC)", {
  out <- flumen:::register_worker_p2p_endpoint(mk_supinfo(NA), "w1", "10.0.0.7")
  expect_equal(out$Host[1], "")   # NA Data_req_port -> nothing to fill
})
