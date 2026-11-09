## Central-scheduler entry point: builds the scheduling state, launches the
## supervisors, and runs the event loop. Called IN-PROCESS by run_workflow()
##
## Returns list(status, err_info, interrupted):
##   status      = 0 normal completion / 1 abnormal
##   err_info    = structured error_notify when a task / scheduler error
##                 occurred (NULL otherwise)
##   interrupted = TRUE when the run was ended by a user interrupt (ESC/Ctrl-C)

run_central_scheduler <- function(workflow, 
                                  exec_plan, 
                                  chunk_size,
                                  sleep_time, 
                                  startup_timeout){

  to_log(paste0("Execution with PID ", Sys.getpid()), "Central scheduler")

  ## A run starts with no sends pending: anything still registered was left by
  ## an earlier run of this R session (see discard_pending_sends).
  discard_pending_sends()

  ## sock_res (results uplink, ALL supervisors fan-in here):
  ##   - All-local workflow  -> IPC listen (unique path, UUID to avoid orphan collision)
  ##   - Has remote workers  -> TCP listen on ports[1] (the SHARED notify port
  ##     allocated by setup_remote_worker_tcp). Every supervisor PUSH-dials it,
  ##     the local ones of a mixed workflow included.
  ## Per-worker sock_ctrl_<name> dispatch sockets are opened by sched_init_workflow().
  is_remote <- vapply(exec_plan$worker_list,
                       function(w) !is.null(w$launcher), logical(1))
  if(any(is_remote)){
    ## Extract the shared notify port from any remote worker's master_url
    ## (setup_remote_worker_tcp() assigned the same notify_port to all).
    ## Format: "tcp://host:notify_port,ctrl_port".
    first_remote_url <- exec_plan$worker_list[[which(is_remote)[1]]]$master_url
    body  <- sub("^tcp://", "", first_remote_url)
    notify_port <- as.integer(strsplit(sub("^[^:]+:", "", body),
                                        ",", fixed = TRUE)[[1]][1])
    sock_res_path <- paste0("tcp://0.0.0.0:", notify_port)
  } else {
    sock_res_path <- paste0("ipc://", short_ipc_file())
  }

  socket_info <- init_sockets(list(), list(), "Central scheduler", sock_res_path = sock_res_path)

  state <- new_scheduler_state(
    timeout               = NULL,
    sleep_time            = sleep_time,
    startup_timeout       = startup_timeout,
    sock_res_path         = sock_res_path
  )
  state$socket_list  <- socket_info$socket_list
  state$message_list <- socket_info$message_list

  ## Build the scheduling state + launch the workers directly: the workflow object
  ## is passed in by the in-process caller, not carried by a message.
  sched_init_workflow(state, workflow, exec_plan, chunk_size)

  ## Every master<->worker channel is dialed BY THE WORKER, so the worker warms
  ## ALL of them from its side (sock_res, and the now-PAIR sock_ctrl/sock_term;
  ## see inst/scripts/supervisor.R). A listener-side heartbeat could keep a flow
  ## warm but never reconnect it (the listener cannot re-dial), so it is dropped.
  ## The scheduler instead DRAINS the worker heartbeats arriving on ctrl/term
  ## (armed in message_list; handled by the sched_step "heartbeat" case).

  ## The event loop runs until a teardown path calls scheduler_teardown(), which
  ## signals the `flumen_scheduler_exit` condition. Catch it here and return its
  ## payload -- the loop has no other exit.
  tryCatch(
    while(TRUE){
      tryCatch({
        ## Advance the sends this loop queued instead of waiting on, and report
        ## the ones that failed for good. A dispatch failure surfaces here, one
        ## or more turns after the send, carrying the context the send recorded
        ## so the report reads as if it had been waited on in place.
        for(.ctx in advance_pending_sends()){
          if(identical(.ctx$kind, "dispatch")) sched_report_dispatch_failure(state, .ctx)
        }
        sched_check_startup_timeout(state)
        sched_check_worker_silence(state)
        sched_check_probe_phase(state)
        ## [DIAG + recovery] A recv aio that resolved to an NNG errorValue is
        ## otherwise swallowed by the unresolved|is_error_value test below and
        ## never re-armed, wedging that socket. sock_res is the single results
        ## uplink for ALL workers. Detect the error state, log the NNG code once
        ## per episode, and re-arm so delivery self-heals. Mirrors the supervisor
        ## main loop's [DIAG] block (inst/scripts/supervisor.R).
        err_socks <- names(state$message_list)[vapply(state$message_list,
                       function(m) isTRUE(nanonext::is_error_value(m)), logical(1))]
        if(length(err_socks)){
          prev     <- if(is.null(state$logged_err_socks)) character(0) else state$logged_err_socks
          new_errs <- setdiff(err_socks, prev)
          for(es in new_errs){
            code <- tryCatch(as.integer(state$message_list[[es]]$data), error = function(e) NA_integer_)
            to_log(paste0("[DIAG] recv socket '", es, "' in ERROR state (NNG code ",
                          code, ") -- re-arming to recover results-uplink"), "Central scheduler")
          }
          if(length(new_errs)) state$logged_err_socks <- union(prev, new_errs)
          for(es in err_socks) state$message_list <- update_message(state$message_list, state$socket_list, es)
        } else if(!is.null(state$logged_err_socks)){
          state$logged_err_socks <- NULL   ## reset once all sockets recovered
        }
        no_msg <- sapply(state$message_list, function(m) nanonext::unresolved(m) | nanonext::is_error_value(m))
        if(!all(no_msg)){
          for(recv_socket in names(state$message_list)[!no_msg]){
            sched_step(decode_message_metadata(state$message_list[[recv_socket]]$data), recv_socket, state)
          }
        }
        Sys.sleep(state$sleep_time)
      }, error     = function(e) scheduler_fatal(e, state),
         interrupt = function(e) scheduler_interrupt(state))
    },
    flumen_scheduler_exit = function(c) list(status      = c$exit_status,
                                             err_info    = c$err_info,
                                             interrupted = isTRUE(c$interrupted))
  )
}
