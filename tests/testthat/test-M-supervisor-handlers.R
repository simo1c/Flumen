## Group M — Supervisor message handlers (R/supervisor_handlers.R).
## Unit tests for the handlers extracted from inst/scripts/supervisor.R: build a
## synthetic supervisor state env with new_supervisor_state(), drive one handler,
## assert the state mutation / return value. These exercise package-internal
## functions (flumen:::...). No subprocesses are spawned.

Sys.setenv(FLUMEN_LOG_DIR = tempdir())   ## keep to_log() writes out of the check dir

mk_state <- function(...) {
  args <- list(
    worker_name = "test_worker", is_remote = FALSE, n_concurrent_task = 1L,
    chunk_size = 1e6, timeout = NULL, sleep_time = 0.1, self_host = NULL,
    root_path = NULL, result_cache = NULL
  )
  do.call(flumen:::new_supervisor_state, utils::modifyList(args, list(...)))
}

test_that("M1 — sup_handle_terminate tears down cleanly and signals quit", {
  st <- mk_state()
  ## fresh state: no wrapper pids, no sockets, empty coalescing env, NULL cache
  ## -> the teardown is a no-op; the handler must still return the "quit" sentinel.
  expect_identical(flumen:::sup_handle_terminate(st), "quit")
})

test_that("M2 — sup_handle_p2p_transfer_done closes the per-transfer socket named in the ACK", {
  done_url <- "p2p-consumer-recv-url-xyz"          ## opaque name under which the producer stored it
  s_req  <- nanonext::socket("pull", listen = "ipc:///tmp/flumen_t_p2p_req.socket")
  s_done <- nanonext::socket("pull", listen = "ipc:///tmp/flumen_t_p2p_done.socket")
  on.exit(suppressWarnings(try(close(s_req),  silent = TRUE)), add = TRUE)
  on.exit(suppressWarnings(try(close(s_done), silent = TRUE)), add = TRUE)

  st <- mk_state()
  st$socket_list  <- list(sock_data_req = s_req)
  st$socket_list[[done_url]] <- s_done
  st$message_list <- list()

  sd  <- flumen:::serialize_data(list(done_url), "R_object", FALSE)
  msg <- list(list(sender    = "consumer",
                   data      = sd$data[[1]],
                   data_type = unlist(sd$data_type$data_struct)[1],
                   is_arrow  = sd$data_type$is_arrow[1]))

  flumen:::sup_handle_p2p_transfer_done(msg, st)

  expect_false(done_url %in% names(st$socket_list))        ## the per-transfer socket: closed + dropped
  expect_true("sock_data_req" %in% names(st$socket_list))  ## the data-request listener: untouched
  expect_true("sock_data_req" %in% names(st$message_list)) ## ... and re-armed for recv
})

test_that("M3 — sup_handle_handshake applies the scheduler config to a remote worker's state", {
  s_res  <- nanonext::socket("push", dial   = "ipc:///tmp/flumen_t_hs_res.socket")
  s_ctrl <- nanonext::socket("pull", listen = "ipc:///tmp/flumen_t_hs_ctrl.socket")
  on.exit(suppressWarnings(try(close(s_res),  silent = TRUE)), add = TRUE)
  on.exit(suppressWarnings(try(close(s_ctrl), silent = TRUE)), add = TRUE)

  st <- mk_state(is_remote = TRUE, worker_name = "placeholder",
                 n_concurrent_task = 1L, chunk_size = 2e9, sleep_time = 0.1)
  st$socket_list <- list(sock_res = s_res, sock_ctrl = s_ctrl)

  config <- list(worker_name = "remote_W", n_concurrent_task = 4L, chunk_size = 5e6,
                 timeout = NULL, sleep_time = 0.2, root_path = "/tmp/wroot",
                 cache_dir = NULL, cache_ram_limit = 64, cache_disk_limit = 256,
                 ports = NULL)                ## NULL ports -> no TCP data-request socket opened here
  sd  <- flumen:::serialize_data(list(config), "R_object", FALSE)
  msg <- list(list(sender = "central_scheduler", msg_type = "handshake"),
              list(data = sd$data[[1]], data_type = unlist(sd$data_type$data_struct)[1],
                   is_arrow = sd$data_type$is_arrow[1]))

  flumen:::sup_handle_handshake(msg, st, "sock_ctrl")

  expect_identical(st$worker_name,       "remote_W")
  expect_identical(st$n_concurrent_task, 4L)
  expect_identical(st$chunk_size,        5e6)
  expect_identical(st$sleep_time,        0.2)
  expect_identical(st$root_path,         "/tmp/wroot")
  expect_false(is.null(st$result_cache))                   ## cache built from the config
  expect_true("sock_ctrl" %in% names(st$message_list))     ## control socket re-armed
})

test_that("M4 - a wrapper that dies before connecting is reported, not waited on", {
  skip_on_os("windows")
  skip_if_not(file.exists("/usr/bin/false"))

  ## /usr/bin/false spawns fine and exits 1 immediately: the same shape as a
  ## Python interpreter without pyarrow/pynng, which used to hang the run
  ## forever because spawn succeeded and no wrapper ever dialed back.
  wf <- new_workflow(
    workflow_id = "M4", desc = "d", wildcard_list = list(),
    task_list = list(new_python_task(
      task_id = "M4", input = new_input(x = 1),
      output = new_output(o = new_object("{{o}}")),
      exec = new_script('def f(x):\n    return x')))
  )

  t0 <- Sys.time()
  msg <- tryCatch(
    run_workflow(wf, chunk_size = 1e8,
                 worker_list = list(new_worker(name = "M4w", n_concurrent_task = 1,
                                               python_cmd = "/usr/bin/false"))),
    error = function(e) conditionMessage(e))
  elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

  expect_match(msg, "before connecting")
  expect_lt(elapsed, 60)   ## it used to never return at all
})
