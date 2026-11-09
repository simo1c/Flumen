suppressPackageStartupMessages(library(flumen))
## Internals are reached with flumen::: instead of attach(asNamespace())

pid_supervisor <- Sys.getpid()
args <- commandArgs(trailingOnly = TRUE)
worker_name    <- args[1]

## Worker-initiated TCP mode is signalled by a non-NULL value at arg17
## (master_url). Local IPC mode stops at the 16 positional args the central
## scheduler passes when it spawns the subprocess.
master_url  <- if(length(args) >= 17 && args[17] != "NULL") args[17] else NULL
remote_self <- if(length(args) >= 18 && args[18] != "NULL") args[18] else NULL
is_remote   <- !is.null(master_url)

if(is_remote){
  req_data_sock  <- NULL
  sock_ctrl_path <- NULL
  sock_term_path <- NULL
  sock_res_path  <- NULL
} else {
  req_data_sock  <- args[2]
  sock_ctrl_path <- if(length(args) >= 3 && args[3] != "NULL") args[3] else NULL
  ## args[8]  = sock_res dial path (scheduler-unique IPC path for local supervisors)
  ## args[15] = sock_term dial path (dedicated terminate channel)
  sock_res_path  <- if(length(args) >= 8 && args[8] != "NULL") args[8] else NULL
  sock_term_path <- if(length(args) >= 15 && args[15] != "NULL") args[15] else NULL
}

if(!is_remote){
  ## ── LOCAL WORKER: read all config from command-line args ─────────────────
  n_concurrent_task  <- as.numeric(args[4])
  chunk_size       <- as.numeric(args[5])
  timeout          <- if(length(args) >= 6 && !is.na(args[6]) && args[6] != "NULL") as.numeric(args[6]) else NULL
  sleep_time       <- as.numeric(args[7])
  self_host        <- if(length(args) >= 10 && args[10] != "NULL") args[10] else NULL
  ## P2P transfer pool bounds: set only when this local worker is in a mixed
  ## local+remote setup (self_host was injected by run_workflow()). NULL ->
  ## peer transfer goes via IPC (all-local workflow). The two arrive END first
  ## (args[9]) and START after (args[11]), the order the argv was built in --
  ## see the annotated vector in R/scheduler_channels.R.
  p2p_pool_start   <- if(length(args) >= 11 && args[11] != "NULL") as.integer(args[11]) else NULL
  p2p_pool_end     <- if(length(args) >= 9  && args[9]  != "NULL") as.integer(args[9])  else NULL
  cache_ram_limit  <- if(length(args) >= 12 && !is.na(args[12])) as.numeric(args[12]) * 1e6 else 4096L * 1e6
  cache_disk_limit <- if(length(args) >= 13 && !is.na(args[13])) as.numeric(args[13]) * 1e6 else 51200L * 1e6
  cache_dir_arg    <- if(length(args) >= 14 && args[14] != "NULL") args[14] else tempdir()
  ## arg15: sock_term dial path — read above in the socket-path block
  ## arg16: root_path — base directory for received files (NULL = current dir)
  root_path        <- if(length(args) >= 16 && args[16] != "NULL") args[16] else NULL
  result_cache     <- flumen:::new_memory_cache(max_ram=cache_ram_limit, max_disk=cache_disk_limit, dir=cache_dir_arg)
} else {
  ## ── REMOTE WORKER: most config arrives via handshake from scheduler ───────
  n_concurrent_task  <- 1L            ## placeholder until handshake
  chunk_size       <- 2e9           ## placeholder until handshake
  timeout          <- NULL
  sleep_time       <- 0.1           ## placeholder until handshake
  self_host        <- remote_self   ## passed by start_worker (auto-detected or override)
  root_path        <- NULL          ## set on handshake
  result_cache     <- NULL          ## initialized on handshake
  p2p_pool_start   <- NULL          ## set on handshake (from config$ports)
  p2p_pool_end     <- NULL          ## set on handshake
}

to_log(paste0("Supervisor in execution with PID ", pid_supervisor), worker_name)
socket_info <- flumen:::init_sockets(list(), list(), worker_name,
                             data_req_socket = req_data_sock,
                             sock_ctrl_path  = sock_ctrl_path,
                             sock_term_path  = sock_term_path,
                             sock_res_path   = sock_res_path,
                             master_url      = master_url)
state <- flumen:::new_supervisor_state(
  worker_name       = worker_name,
  is_remote         = is_remote,
  n_concurrent_task = n_concurrent_task,
  chunk_size        = chunk_size,
  timeout           = timeout,
  sleep_time        = sleep_time,
  self_host         = self_host,
  root_path         = root_path,
  result_cache      = result_cache,
  p2p_pool_start    = p2p_pool_start,
  p2p_pool_end      = p2p_pool_end
)
state$socket_list  <- socket_info$socket_list
state$message_list <- socket_info$message_list

if(!is_remote){
  ## Local worker: the scheduler IPC socket is already listening, so READY can go out
  ## at once. EXECUTE_TASK arrives on sock_ctrl_<name>, push from the scheduler and
  ## pull dial from this supervisor -- see init_sockets() in R/messaging.R.
  flag <- flumen:::send_ready_to_central_scheduler(
    state$socket_list[["sock_res"]],
    worker_name,
    n_concurrent_task,
    self_host = self_host
  )
}
## Remote workers (is_remote=TRUE): READY deferred until "handshake" arrives on sock_ctrl.
## Avoids NNG PUSH dropping/consuming the message before the new scheduler PULL connects.

## Keepalive. A stateful firewall evicts idle TCP flows, so a long task leaves the
## master<->worker channels silent and a later message is blackholed on a
## dead-but-ESTABLISHED connection. The WORKER sends the beat on all three sockets,
## not the scheduler: only the dialer's own send lets NNG detect a broken pipe and
## reconnect, and the worker is the dialer on every channel. Remote only (local
## supervisors use IPC). Every liveness threshold in the engine must sit above this
## interval, which an environment variable can lower. Read fresh each
## cycle: on a remote node the master's value arrives with the handshake, after
## this point.
heartbeat_interval <- function()
  flumen:::worker_threshold(state, "heartbeat_secs", "FLUMEN_HEARTBEAT_SECS", 20)
state$last_heartbeat <- Sys.time()

## A wrapper that dies before connecting would otherwise leave the run waiting on
## a socket nobody will dial (see check_dead_wrappers). Local and remote alike.
WRAPPER_CHECK_INTERVAL <- 2  ## seconds; polls only the registered wrappers
state$last_wrapper_check <- Sys.time()

## Worker-side dead-man's-switch (remote only). If this supervisor never establishes
## its control channel -- wrong or unreachable master_host -- the master can never
## terminate it, since it kills remote workers over sock_ctrl and their PID sits on
## another host: it would spin forever as an orphan. Having reached the scheduler is
## latched the first time sock_ctrl holds a connected pipe, read level-triggered from
## nanonext::stat(sock_ctrl, "pipes") so a connection made before the query is not
## missed, and never re-tested, so a later firewall eviction cannot re-trigger it.
## Without a connection within ORPHAN_CONNECT_TIMEOUT the worker self-terminates.
##
## Raise this whenever run_workflow(startup_timeout = ) is raised: a worker that gives
## up is not the failure mode, an orphan outliving its master is. The clock starts
## once the supervisor process exists, so time in a batch queue does not count.
##
## Read from the environment, not the handshake: it governs the case where the
## handshake never arrives.
ORPHAN_CONNECT_TIMEOUT <- local({
  v <- suppressWarnings(as.numeric(Sys.getenv("FLUMEN_ORPHAN_CONNECT_SECS", "45")))
  if(!isTRUE(is.finite(v)) || v <= 0) 45 else v
})  ## seconds
if(is_remote){
  state$ever_connected <- FALSE
  orphan_t0 <- Sys.time()
}

## Worker-side dead-man's switch for a scheduler that STOPS answering: the half the
## orphan timeout above leaves uncovered, since that test is never repeated once
## latched. Without it a network partition leaves the master tearing the run down
## while this node keeps computing for nobody.
##
## The signal is a REPLY, the scheduler answering the keepalive on the ctrl channel
## (the "heartbeat" case in R/scheduler_handlers.R). Answering is not initiating, so
## the rule that a listener cannot beat on its own does not apply here. It arms only
## once a reply has been heard (state$sched_beat_seen, set in sup_step), so a
## scheduler too old to answer leaves this node as it was without the check, and a
## cluster is routinely mixed between redistributions.
##
## 600 s, 30 unanswered beats, oversized on purpose: on the master's side a false
## positive stops the run and says so, here it KILLS RUNNING TASKS on a healthy node.
## The value comes from the handshake, which always precedes the first reply and so
## precedes arming.
scheduler_silence <- function()
  flumen:::worker_threshold(state, "scheduler_silence_secs", "FLUMEN_SCHEDULER_SILENCE_SECS", 600)

while(TRUE){
  ## Advance the sends this loop queued instead of waiting on. A result_notify
  ## that has exhausted its retries is reported here, loudly, because the
  ## scheduler will otherwise never learn the task finished.
  for(.ctx in flumen:::advance_pending_sends()){
    if(identical(.ctx$kind, "result_notify"))
      to_log(sprintf(paste0("result_notify for %s could NOT be delivered after %d attempts",
                            " -- the scheduler will not learn this task finished and the",
                            " workflow will stall. Keep the scheduler host awake and",
                            " reachable for the whole run."),
                     .ctx$msg_sender, .ctx$attempts), worker_name)
  }
  if(is_remote && !isTRUE(state$ever_connected) &&
     nanonext::stat(state$socket_list[["sock_ctrl"]], "pipes") > 0)
    state$ever_connected <- TRUE
  if(is_remote &&
     flumen:::supervisor_orphaned(TRUE, isTRUE(state$ever_connected),
                         as.numeric(difftime(Sys.time(), orphan_t0, units = "secs")),
                         ORPHAN_CONNECT_TIMEOUT)){
    to_log(paste0("ORPHANED: no scheduler at ", master_url, " within ",
                  ORPHAN_CONNECT_TIMEOUT, "s; this worker stops. Fix: check master_host, ",
                  "or export FLUMEN_ORPHAN_CONNECT_SECS on this host before the worker starts; ",
                  "the master's shell has no effect on it."),
           worker_name)
    try(flumen:::clean_termination(state$all_pid, state$socket_list, state$result_cache),
        silent = TRUE)
    quit(status = 1)
  }
  if(is_remote && isTRUE(state$sched_beat_seen) &&
     isTRUE(is.finite(scheduler_silence())) && scheduler_silence() > 0 &&
     as.numeric(difftime(Sys.time(), state$last_scheduler_contact,
                         units = "secs")) >= scheduler_silence()){
    to_log(paste0("SCHEDULER SILENT: no answer for ",
                  round(as.numeric(difftime(Sys.time(), state$last_scheduler_contact,
                                            units = "secs"))),
                  "s (threshold ", scheduler_silence(), "s); this worker stops. Fix: raise ",
                  "FLUMEN_SCHEDULER_SILENCE_SECS in the shell that launches the run."),
           worker_name)
    try(flumen:::clean_termination(state$all_pid, state$socket_list, state$result_cache),
        silent = TRUE)
    quit(status = 1)
  }
  tryCatch({
    no_msg <- sapply(state$message_list, function(m) nanonext::unresolved(m) | nanonext::is_error_value(m))
    ## [DIAG] surface recv sockets that resolved to an ERROR value -- normally
    ## these are silently treated as "no message" and never reported. Log once
    ## per socket per error episode (throttled to avoid flooding).
    errs <- names(state$message_list)[vapply(state$message_list,
              function(m) isTRUE(nanonext::is_error_value(m)), logical(1))]
    prev <- if(is.null(state$logged_err_socks)) character(0) else state$logged_err_socks
    new_errs <- setdiff(errs, prev)
    if(length(new_errs)){
      to_log(paste0("[DIAG] recv socket(s) in ERROR state (swallowed by poll loop): ",
                    paste(new_errs, collapse = ", ")), state$worker_name)
      state$logged_err_socks <- union(prev, new_errs)
    }
    if(length(setdiff(prev, errs))) state$logged_err_socks <- intersect(prev, errs)  ## reset when recovered
    if(!all(no_msg)){
      for(recv_socket in names(state$message_list)[!no_msg]){
        msg <- flumen:::decode_message_metadata(state$message_list[[recv_socket]]$data)
        if(identical(flumen:::sup_step(msg, recv_socket, state), "quit")) quit(status = 0)
      }
    }
  }, error = function(e) flumen:::supervisor_fatal(e, state))

  ## Turn a wrapper that died before connecting into the error the run should
  ## have reported, instead of waiting for a READY that will never come. Guarded
  ## so a failure here can never take the supervisor down.
  if(as.numeric(difftime(Sys.time(), state$last_wrapper_check, units = "secs")) >= WRAPPER_CHECK_INTERVAL){
    tryCatch(flumen:::check_dead_wrappers(state),
             error = function(e) to_log(paste0("check_dead_wrappers failed: ",
                                               conditionMessage(e)), state$worker_name))
    ## Same cadence, same shape of question: a fetch from a peer that stopped
    ## answering leaves a task waiting for an input that will never arrive, and
    ## this node is the only one that knows it is waiting. Guarded so a failure
    ## here can never take the supervisor down.
    tryCatch(flumen:::check_stalled_transfers(state),
             error = function(e) to_log(paste0("check_stalled_transfers failed: ",
                                               conditionMessage(e)), state$worker_name))
    state$last_wrapper_check <- Sys.time()
  }

  if(is_remote &&
     as.numeric(difftime(Sys.time(), state$last_heartbeat, units = "secs")) >= heartbeat_interval()){
    ## Worker warms every channel it dialed (dialer=sender). ctrl/term are PAIR,
    ## so the worker can send on them; the heartbeat both keeps the flow warm and
    ## gives NNG the failed-send signal needed to auto-reconnect a dropped pipe.
    try(flumen:::send_heartbeat(state$socket_list[["sock_res"]],  state$worker_name), silent = TRUE)
    try(flumen:::send_heartbeat(state$socket_list[["sock_ctrl"]], state$worker_name), silent = TRUE)
    try(flumen:::send_heartbeat(state$socket_list[["sock_term"]], state$worker_name), silent = TRUE)
    state$last_heartbeat <- Sys.time()
  }
  Sys.sleep(state$sleep_time)
}
