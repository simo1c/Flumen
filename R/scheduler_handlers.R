## Message handlers for the central scheduler. The scheduler runs in-process in
## the run_workflow() session (see run_central_scheduler() in R/scheduler_run.R).
##
## The scheduler's mutable runtime state is bundled into a single environment built by
## new_scheduler_state(); it is passed (by reference) to handlers, which mutate
## `state$<field>` in place so the main loop never re-binds it.

## Bundled mutable runtime state of the central scheduler. One environment, created
## once at start-up; the caller fills `socket_list`/`message_list` right after (from
## init_sockets()), and sched_init_workflow() populates the workflow / plan /
## table / scheduling fields.
new_scheduler_state <- function(timeout, sleep_time,
                                startup_timeout, sock_res_path){
  st <- new.env(parent = emptyenv())

  ## config (from run_workflow)
  st$timeout               <- timeout
  st$sleep_time            <- sleep_time
  st$startup_timeout       <- startup_timeout
  st$sock_res_path         <- sock_res_path           ## scheduler's results-uplink IPC path

  ## NNG socket / message tables -- populated by the caller right after construction
  st$socket_list  <- list()
  st$message_list <- list()

  ## free concurrent-task slots (one row per slot, on Supervisor_ID) and the chunk
  ## reassembly buffer
  st$available_fifo       <- data.frame(Supervisor_ID = NULL)
  st$res_chunked_message  <- list()

  ## worker bring-up tracking
  st$workers_ready      <- character(0)
  st$n_workers_expected <- 0L
  st$supervisor_info    <- NULL   ## the worker-info data.frame (set by sched_init_workflow)
  st$startup_deadline   <- NULL   ## NULL = not armed; set after launch, cleared once all connect
  st$chunk_size         <- NULL   ## set by sched_init_workflow() at start-up

  ## connectivity-check phase (between all-ready and first dispatch). "startup"
  ## -> "probing" -> "dispatch". reach_reports[[worker]] = its peer-reachability
  ## report; probe_sent_to = workers awaited for a report. See R/reachability.R.
  st$phase          <- "startup"
  st$probe_deadline <- NULL
  st$probe_sent_to  <- character(0)
  st$reach_reports  <- list()

  ## workflow / plan / scheduling state -- all NULL until sched_init_workflow()
  st$workflow                 <- NULL
  st$exec_plan                <- NULL
  st$task_table               <- NULL
  ## hash index Exe_ID -> Status (O(1) on the hot path). Only "finished" may be
  ## READ from it: it is written through set_task_status(), which updates both
  ## this and the data.frame, but a dispatch that omits it writes the data.frame
  ## alone, so any other status read here can be stale.
  st$task_status_env          <- NULL
  st$sched_state              <- NULL   ## dependency counts + data->workers index (build_scheduler_state())
  st$exe_id_to_idx            <- NULL
  st$required_by              <- NULL   ## reverse index of workflow$required_shapes (parent -> children)
  st$original_required_shapes <- NULL   ## build-time dep_map snapshot
  st$cumulative_replace_op    <- list() ## transitive rename map merged across notifies

  ## when each worker's last heartbeat arrived. Populated on the FIRST beat, which
  ## is also what selects who is watched: a local worker never beats (the beat
  ## exists to keep a firewall flow warm) and so never enters this map, while a
  ## remote one enters it as soon as it has proved it can beat.
  st$last_heartbeat_at        <- list()

  ## structured error recorded by record_error_notify(); read by scheduler_teardown()
  ## and returned to run_workflow() so it can build the user-facing stop() message
  st$err_info                 <- NULL

  st
}


## ── handlers ─────────────────────────────────────────────────────────────────
## Named sched_handle_*, against the supervisor's sup_handle_* in
## R/supervisor_handlers.R: both files are sourced into the same package namespace,
## so the two sets of handlers for the same message types must not collide.

## Send a "terminate" message to every supervisor via its dedicated terminate
## channel (sock_term_<name> -- covers both local IPC and remote TCP supervisors).
## This channel never carries bulk EXECUTE_TASK data, so the terminate cannot be
## head-of-line blocked behind an in-flight chunked task transfer. Best-effort:
## send failures are swallowed.
broadcast_terminate <- function(state){
  term_data <- serialize_data(list("terminate"), "R_object", FALSE)
  term_msg  <- new_message(sender = "central_scheduler", msg_id = "terminate", msg_type = "terminate",
                           data_type = unlist(term_data$data_type$data_struct), data_names = "",
                           data = term_data$data, is_arrow = term_data$data_type$is_arrow)
  for(term_name in names(state$socket_list)[startsWith(names(state$socket_list), "sock_term_")]){
    try(flumen_send_aio(state$socket_list[[term_name]], term_msg$message, mode = "raw"), silent = TRUE)
  }
  invisible(NULL)
}

## Every instance still reading "execution" when the run ends was killed by the
## teardown about to happen, not left running. Mark them "aborted" so a table read
## afterwards can tell the two apart. Mutates state$task_table in place (state is an
## environment) and does not persist: the caller decides when to write.
mark_inflight_aborted <- function(state){
  if(is.null(state$task_table) || is.null(state$task_table$Status) ||
     is.null(state$task_status_env)) return(invisible(NULL))
  ## Both in-flight states: a task that was dispatched and never confirmed would
  ## otherwise sit in "dispatched" after the run is gone.
  for(e in state$task_table$Exe_ID[state$task_table$Status %in% c("execution", "dispatched")])
    state$task_table <- set_task_status(state$task_table, state$task_status_env,
                                        e, "aborted")
  invisible(NULL)
}

## Centralized scheduler teardown. Every exit path -- normal completion,
## operation error, startup timeout, fatal error, interrupt (SIGINT) -- routes
## through here so the teardown is identical regardless of trigger:
## notify every supervisor (broadcast_terminate), let the async send flush,
## force-kill the local supervisor subtrees + close sockets (clean_termination),
## then end the event loop. Each caller keeps only its own prelude (logging,
## error forwarding, artifact saving) before calling this. Does not return: it
## signals the `flumen_scheduler_exit` condition, which run_central_scheduler()
## catches to end the loop and report exit_status, the recorded err_info, and
## whether the run was interrupted.
scheduler_teardown <- function(state, exit_status, interrupted = FALSE){
  ## try() catches errors but NOT interrupts, and a second interrupt here is the
  ## normal case: the key repeat of one held ESC delivers it. Cutting this
  ## sequence short leaves a REMOTE supervisor computing for a run that no longer
  ## exists. So the notice goes first and every step catches interrupts too, the
  ## wait included: the send is async and that wait is where it leaves the process.
  safely <- function(expr) tryCatch(expr, error = function(e) NULL,
                                    interrupt = function(e) NULL)
  safely(broadcast_terminate(state))
  safely(Sys.sleep(0.5))   ## give send_aio time to deliver before sockets close

  ## record_error_notify() already marks on the error paths; doing it here covers
  ## the paths with no error notify to run -- a user interrupt above all, after
  ## which the killed instance would keep reading "execution" and flumen_status()
  ## could not tell it from a run still in progress.
  safely(mark_inflight_aborted(state))
  if(!is.null(state$task_table))
    safely(persist_task_table(state, full = TRUE))

  sup_pids <- if(!is.null(state$supervisor_info)) as.integer(state$supervisor_info$PID) else integer(0)
  sup_pids <- sup_pids[!is.na(sup_pids)]
  safely(clean_termination(sup_pids, state$socket_list))
  safely(to_log(paste0("Central scheduler terminated (exit ", exit_status, ")"), "Central scheduler"))
  stop(structure(
    list(message     = paste0("central scheduler exit ", exit_status),
         call        = NULL,
         exit_status = exit_status,
         err_info    = state$err_info,
         interrupted = interrupted),
    class = c("flumen_scheduler_exit", "condition")
  ))
}

## "task_execution" from a supervisor: op `op_name` is now running on `sender`. Record
## its status + worker assignment and persist the task table.
sched_handle_task_execution <- function(msg, state, recv_socket){
  sender  <- msg[[1]]$sender
  op_name <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  to_log(paste0("TASK_EXECUTION received from supervisor ", sender, " for operation ", op_name), "Central scheduler")
  state$task_table <- set_task_status(state$task_table, state$task_status_env, op_name, "execution")
  ## worker assignment via match() -- avoids the O(N) df[df$Exe_ID==X,"Worker"]<-...
  ## at large N
  .idx <- match(op_name, state$task_table$Exe_ID)
  if(!is.na(.idx)) state$task_table$Worker[.idx] <- sender
  ## the Worker lands here and not in set_task_status(), so the row has to be
  ## marked again or the persisted line would carry an empty Worker
  mark_task_row_dirty(state$task_status_env, op_name)
  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  persist_task_table(state)
  invisible(NULL)
}

## "file_received" from a supervisor: a chunked file transfer finished. Move those paths
## from in_transit to completed for `sender`, then retry scheduling -- tasks held back in
## the ready queue because the file was in_transit can now be dispatched.
sched_handle_file_received <- function(msg, state, recv_socket){
  sender         <- msg[[1]]$sender
  received_paths <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  to_log(paste0("file_received from ", sender, " for ", length(received_paths), " file(s)"), "Central scheduler")
  state$sched_state  <- mark_files_completed(state$sched_state, sender, as.character(received_paths))
  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  
  if(nrow(state$available_fifo)){
    sched_try_schedule(state)
  }
  
  invisible(NULL)
}

## "data_received" from a supervisor: the worker now holds a COMPLETE copy of
## (exe_id, out_instance) -- it received the full output via P2P and re-cached
## it (complete_p2p_transfer), so it can re-serve that output to a peer. Record it in
## the holder index, which the fetch routing reads to pull from a holder instead of
## always from the producer. Re-arm the socket like every sock_res handler.
sched_handle_data_received <- function(msg, state, recv_socket){
  sender       <- msg[[1]]$sender
  exe_id       <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  out_inst_str <- decode_message_data(msg[[2]]$data, msg[[2]]$data_type, msg[[2]]$is_arrow)
  ## is_full (msg[[3]]): TRUE = the sender re-cached a COMPLETE copy and may be advertised
  ## as a holder. FALSE = a per-sample-filtered partial copy (release only, no advertise).
  ## Absent, on a message from an older supervisor, reads as TRUE.
  is_full      <- if(length(msg) >= 3)
                    isTRUE(decode_message_data(msg[[3]]$data, msg[[3]]$data_type, msg[[3]]$is_arrow))
                  else TRUE
  to_log(paste0("data_received from ", sender, " holds ", exe_id, "#", out_inst_str,
                if(is_full) "" else " (partial: release-only)"), "Central scheduler")
  ## Advertise as a holder ONLY for a full copy. A partial copy is never a holder.
  if(is_full) state$sched_state <- mark_holder(state$sched_state, exe_id, out_inst_str, sender)
  ## This fetch completed -> release the least-connections serve count for the
  ## source we routed it to. Fires for full AND partial copies (sender is the consumer).
  state$sched_state  <- mesh_release_serve(state$sched_state, sender, paste0(exe_id, "#", out_inst_str))
  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  invisible(NULL)
}

## "ready" from a supervisor: the worker has connected. Decode its free-slot count +
## advertised self_host, re-arm its socket, probe + fill its P2P endpoint, restock the
## free-slot FIFO, and -- once ALL workers are ready -- disarm the startup timeout and
## run a scheduling pass.
sched_handle_worker_ready <- function(msg, state, recv_socket){
  sender <- msg[[1]]$sender
  to_log(paste0("READY received from ", sender), "Central scheduler")
  n_op_to_assign  <- decode_message_data(msg[[2]]$data, msg[[2]]$data_type, msg[[2]]$is_arrow)
  ## msg[[3]] = the worker's self_host (worker-initiated mode); "" for a local worker with
  ## no explicit self_host. No dispatch URL is exchanged: the master uses
  ## sock_ctrl_<name>, the supervisor pulls EXECUTE_TASK from the handshake channel.
  ready_self_host <- if(length(msg) >= 3) decode_message_data(msg[[3]]$data, msg[[3]]$data_type, msg[[3]]$is_arrow) else ""
  ## msg[[4]] = the worker's flumen version. Absent means a build older than the
  ## check itself, which check_worker_version() treats as a mismatch
  ## (R/version_guard.R).
  ready_version <- if(length(msg) >= 4) decode_message_data(msg[[4]]$data, msg[[4]]$data_type, msg[[4]]$is_arrow) else NULL
  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)

  ver_err <- check_worker_version(sender, ready_version)
  if(!is.null(ver_err)){
    to_log(paste0("VERSION MISMATCH: ", ver_err), "Central scheduler")
    record_error_notify(state, new_error_notify(
      supervisor_ID = sender, task_id = "<version>", exe_id = "<version>",
      message = ver_err))
    scheduler_teardown(state, 1)   ## signals flumen_scheduler_exit
  }

  ## Fill this worker's P2P endpoint (Host + Socket_data_req) in supervisor_info.
  ## Reachability is not probed here: every edge is checked uniformly in the
  ## connectivity-check phase after all workers register (R/reachability.R).
  state$supervisor_info <- register_worker_p2p_endpoint(state$supervisor_info, sender, ready_self_host)

  ## restock the free-slot FIFO (one row per slot on `sender`)
  if(n_op_to_assign > 0){
    state$available_fifo <- rbind(state$available_fifo, data.frame(Supervisor_ID = rep(sender, n_op_to_assign)))
  }
  state$workers_ready <- union(state$workers_ready, sender)

  ## All workers connected: disarm the startup timeout, then run the
  ## connectivity check (probe phase) before the first dispatch -- or, for an
  ## all-local/IPC workflow, dispatch straight away. See R/reachability.R.
  if(nrow(state$available_fifo) && length(state$workers_ready) >= state$n_workers_expected){
    state$startup_deadline <- NULL
    sched_enter_probe_or_dispatch(state)
  }
  invisible(NULL)
}

## Attach the failing instance's WILDCARD BINDING (e.g. sample = "alfa") to the
## error notify, so a failure is identified by its sample name and not by exe_id
## alone. The binding rides on the expanded exe_object as op$wildcards, so this
## is a local lookup and nothing changes on the wire.
## Idempotent, so the log path and the record path can both call it.
attach_wildcard_binding <- function(state, err_notify){
  if(!is.null(err_notify$wildcards) || is.null(state$exe_id_to_idx)) return(err_notify)
  eid <- err_notify$exe_id
  if(is.null(eid) || length(eid) != 1L || is.na(eid) ||
     !(eid %in% names(state$exe_id_to_idx))) return(err_notify)
  wc <- state$workflow$task_list[[state$exe_id_to_idx[[eid]]]]$wildcards
  if(!length(wc)) return(err_notify)
  ## Keep only RESOLVED bindings: a dynamic wildcard is still its own "{{name}}"
  ## template at this point and identifies nothing.
  keep <- vapply(wc, function(v)
    is.character(v) && length(v) == 1L && !is.na(v) &&
      !grepl("{{", v, fixed = TRUE), logical(1))
  if(any(keep)) err_notify$wildcards <- wc[keep]
  err_notify
}

## Record a structured error_notify on the scheduler state. The scheduler runs
## in-process, so scheduler_teardown() puts state$err_info into the
## flumen_scheduler_exit condition and run_workflow() builds the user-facing
## stop() message from it.
record_error_notify <- function(state, err_notify){
  ## Enrich here rather than at each call site: every error path -- task error,
  ## dispatch failure, reachability abort, supervisor fatal -- funnels through
  ## this one function, so a single lookup covers them all.
  err_notify     <- attach_wildcard_binding(state, err_notify)
  state$err_info <- err_notify

  ## Record the OUTCOME in the persisted task table.
  ##
  ## Left alone, a failed instance keeps the status it had when dispatched,
  ## "execution", so a crashed run and a running one read identically and a
  ## sibling killed mid-flight is indistinguishable from the one that broke.
  ##
  ## The failing instance becomes "failed". Siblings still in flight are about to
  ## be killed by the teardown that follows, so they become "aborted" rather than
  ## staying "execution" and implying work still in progress. scheduler_teardown()
  ## does not persist the table, so it is saved here or the .Rds keeps the
  ## pre-failure state.
  if(!is.null(state$task_table) && !is.null(state$task_table$Exe_ID)){
    eid <- err_notify$exe_id
    if(!is.null(eid) && length(eid) == 1L && !is.na(eid) &&
       !is.na(match(eid, state$task_table$Exe_ID)))
      state$task_table <- set_task_status(state$task_table, state$task_status_env,
                                          eid, "failed")
    mark_inflight_aborted(state)
    try(persist_task_table(state), silent = TRUE)
  }
  invisible(NULL)
}

## A dispatch that send_message_by_chunks reported as FAILED (send_ok=FALSE) cannot be
## marked "execution": the worker never received the op, so no task_execution/RESULT can
## come. Treat it like any task error -- record it and tear down.
sched_report_dispatch_failure <- function(state, df){
  .idx    <- match(df$exe_id, state$task_table$Exe_ID)
  task_id <- if(!is.na(.idx)) state$task_table$Op_ID[.idx] else df$exe_id
  err_notify <- list(
    task_id       = task_id,
    exe_id        = df$exe_id,
    Supervisor_ID = df$worker,
    message       = paste0("EXECUTE_TASK dispatch to worker '", df$worker,
                           "' failed (control channel reported a send failure); the task ",
                           "was never started. Aborting instead of waiting indefinitely. ",
                           "Check the worker is alive and its control port is reachable."),
    stderr        = ""
  )
  to_log(paste0("WORKFLOW ERROR: EXECUTE_TASK dispatch to worker '", df$worker,
                "' failed for op '", task_id, "' (exe_id ", df$exe_id, ")"), "Central scheduler")
  record_error_notify(state, err_notify)
  scheduler_teardown(state, 1)   ## raises flumen_scheduler_exit -> loop catches, run_workflow reports
}

## Run one scheduling pass over `state` and fold the outcome back into it.
##
## try_schedule() is deliberately state-free: it takes its ten inputs as separate
## arguments and returns the changed ones in a list, so it depends on no scheduler
## state. The threading between it and `state` lives here, so every call site does
## it the same way.
##
## `state` is an environment (see new_scheduler_state), so the assignments below are
## visible to the caller and there is nothing to return.
sched_try_schedule <- function(state){

  res_exe <- try_schedule(state$sched_state, state$workflow, state$exe_id_to_idx,
                          state$available_fifo, state$socket_list, state$task_table,
                          state$chunk_size, state$exec_plan$worker_list,
                          task_status_env = state$task_status_env,
                          supervisor_info = state$supervisor_info)
  state$sched_state    <- res_exe$sched_state
  state$available_fifo <- res_exe$available_fifo
  state$task_table     <- res_exe$task_table
  if(!is.null(res_exe$dispatch_failed))
    sched_report_dispatch_failure(state, res_exe$dispatch_failed)

  invisible(NULL)
}

## Build the scheduler's workflow / scheduling state from a workflow, exec plan,
## and chunk size: the task table + derived indices, spawn the LOCAL supervisors
## and register every worker's record (remote supervisors were already launched by
## launch_remote_workers() before the scheduler started -- see
## launch_local_and_register_supervisors()), open the per-worker dispatch sockets
## (local IPC + remote TCP via open_remote_worker_channels()), arm the startup
## timeout. Mutates `state` in place. Called once by run_central_scheduler() at start-up.
sched_init_workflow <- function(state, workflow, exec_plan, chunk_size){
  state$workflow   <- workflow
  state$exec_plan  <- exec_plan
  state$chunk_size <- chunk_size

  state$task_table <- init_task_table(state$exec_plan, state$workflow)
  state$task_table$Status <- "ready"
  state$task_status_env <- build_task_status_env(state$task_table)
  try(persist_task_table(state, full = TRUE), silent = TRUE)
  state$sched_state     <- build_scheduler_state(state$workflow)
  state$exe_id_to_idx   <- setNames(seq_along(state$workflow$task_list),
                                    sapply(state$workflow$task_list, "[[", "exe_id"))

  state$required_by              <- build_required_by_index(state$workflow$required_shapes)
  state$original_required_shapes <- state$workflow$required_shapes   ## build-time dep_map snapshot
  state$cumulative_replace_op    <- list()                           ## transitive rename map across notifies

  state$supervisor_info <- launch_local_and_register_supervisors(state$exec_plan$worker_list, state$chunk_size,
                                                 sleep_time    = as.character(state$sleep_time),
                                                 sock_res_path = state$sock_res_path)
  state$n_workers_expected <- nrow(state$supervisor_info)
  state$startup_deadline   <- Sys.time() + state$startup_timeout

  ## per-worker sockets sock_ctrl_<name> (dispatch) and sock_term_<name> (dedicated
  ## terminate channel) for each LOCAL supervisor: scheduler PUSH listens on the IPC
  ## paths launch_local_and_register_supervisors() wrote into
  ## supervisor_info$Socket_ctrl / $Socket_term, which the supervisor pulls from.
  ## Remote workers get theirs below (TCP listen).
  local_sup <- state$supervisor_info[is.na(state$supervisor_info$Port_base), ]
  for(li in seq_len(nrow(local_sup))){
    ctrl_nm <- paste0("sock_ctrl_", local_sup[li, "Supervisor_ID"])
    term_nm <- paste0("sock_term_", local_sup[li, "Supervisor_ID"])
    ## PAIR, not PUSH: bidirectional so the worker (dialer) can also send its
    ## keepalive heartbeat. Local workers run over IPC (no firewall) and skip the
    ## heartbeat, so the up-direction is idle here; arming recv stays uniform with
    ## the remote path (drained by the sched_step "heartbeat" case).
    lw_ctrl <- nanonext::socket("pair", listen = local_sup[li, "Socket_ctrl"])
    state$socket_list  <- add_socket(state$socket_list, lw_ctrl, ctrl_nm)
    state$message_list <- add_message(state$message_list, state$socket_list, ctrl_nm)
    lw_term <- nanonext::socket("pair", listen = local_sup[li, "Socket_term"])
    state$socket_list  <- add_socket(state$socket_list, lw_term, term_nm)
    state$message_list <- add_message(state$message_list, state$socket_list, term_nm)
  }

  ## open TCP res/ctrl listen sockets for each remote worker + send its handshake (config
  ## incl. its own P2P port range). See open_remote_worker_channels() in
  ## R/scheduler_channels.R.
  ch <- open_remote_worker_channels(state$supervisor_info, state$exec_plan$worker_list,
                                    state$socket_list, state$message_list,
                                    state$chunk_size, state$timeout, state$sleep_time)
  state$socket_list  <- ch$socket_list
  state$message_list <- ch$message_list
  ## Retain the handshake send_aio handles for the run: the sends are still
  ## pending until each remote worker dials in, and an unreferenced aio would be
  ## GC-cancelled (see open_remote_worker_channels).
  state$handshake_aios <- ch$handshake_aios

  invisible(NULL)
}

## "result" from a supervisor: an op finished (res_notify). Mark it finished, fold the
## notify into the workflow (wildcard re-expansion, parent renames), rebuild the derived
## indices, advance the scheduling state, restock the free-slot FIFO, run a scheduling
## pass; if every task is now finished, persist the table and tear down (exit 0).
sched_handle_result <- function(msg, state, recv_socket){
  msg_id   <- msg[[1]]$msg_id
  chunk    <- msg[[1]]$chunk
  n_chunks <- msg[[1]]$n_chunks
  sender   <- msg[[1]]$sender

  state$res_chunked_message <- push_chunked_message(state$res_chunked_message, msg, msg_id, chunk)
  to_log(sprintf("[DIAG] RESULT chunk %s/%s received from worker '%s' (accumulated %s/%s)",
                 chunk, n_chunks, sender, length(state$res_chunked_message[[msg_id]]), n_chunks),
         "Central scheduler")
  if(length(state$res_chunked_message[[msg_id]]) != n_chunks){
    state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
    persist_task_table(state)
    return(invisible(NULL))
  }

  msg <- merge_all_chunks(state$res_chunked_message[[msg_id]])
  state$res_chunked_message[[msg_id]] <- NULL
  res_notify <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  to_log(paste0("[DIAG] RESULT fully received from worker '", sender, "' for exe_id ", res_notify$exe_id), "Central scheduler")

  ## Idempotent RESULT handling: a res_notify can be delivered twice. A send that
  ## timed out on a dead-but-ESTABLISHED pipe may still be delivered late by TCP,
  ## AND the worker's retry (sup_handle_result) sends a second copy. Re-processing
  ## decrements dep_remaining a SECOND time in on_task_finished, prematurely
  ## readying a multi-parent child before all its parents finished. Drop a RESULT
  ## whose exe_id is already recorded finished.
  if(identical(state$task_status_env[[res_notify$exe_id]], "finished")){
    to_log(paste0("[DIAG] duplicate RESULT for already-finished ", res_notify$exe_id,
                  " -- ignoring (idempotent)"), "Central scheduler")
    state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
    return(invisible(NULL))
  }

  state$task_table <- set_task_status(state$task_table, state$task_status_env, res_notify$exe_id, "finished")
  ## Serve-release backstop: this consuming task finished, so every serve routed to
  ## feed it
  ## is definitively concluded (transfer/cache/colocal/coalesced alike) -> release them.
  ## Guarantees serving_count can never leak permanently. Keyed by this exe_id only, so
  ## sibling tasks still running on the same worker keep their in-flight serves counted.
  state$sched_state <- mesh_release_by_exe_id(state$sched_state, res_notify$exe_id)

  workflow_update <- process_result_notify_and_update_workflow(
    state$workflow, state$supervisor_info, res_notify,
    exe_id_to_idx            = state$exe_id_to_idx,
    required_by              = state$required_by,
    cumulative_replace_op    = state$cumulative_replace_op)
  
  state$workflow <- workflow_update$workflow
  replace_op     <- workflow_update$replace_op

  ## merge this notify's replace_op into the cumulative map: re-resolve already-cumulated
  ## values through it (K -> V -> V' becomes K -> V'), then add the new keys
  if(!is.null(replace_op)){
    for(k in names(state$cumulative_replace_op)){
      v <- state$cumulative_replace_op[[k]]
      state$cumulative_replace_op[[k]] <- unique(unlist(lapply(v, function(x) if(x %in% names(replace_op)) replace_op[[x]] else x)))
    }
    for(k in names(replace_op)){
      if(!(k %in% names(state$cumulative_replace_op))) state$cumulative_replace_op[[k]] <- replace_op[[k]]
    }
  }
  state$task_table      <- update_task_table(state$workflow$task_list, state$task_table, replace_op)
  ## wildcard expansion may swap one row for N (new exe_ids) and drop the original ->
  ## rebuild the derived indices from the up-to-date data.frame. Two of the three
  ## only need rebuilding when it HAPPENED: with no replacement no instance is
  ## added, removed or renamed, so the exe_id -> index map and the status mirror
  ## come out identical.
  ##
  ## `required_by` is NOT in the guard and must not be: it derives from
  ## `required_shapes`, which shrinks as dependencies resolve, so it changes
  ## without any expansion.
  if(length(replace_op)){
    state$task_status_env <- build_task_status_env(state$task_table)
    state$exe_id_to_idx   <- setNames(seq_along(state$workflow$task_list),
                                      sapply(state$workflow$task_list, "[[", "exe_id"))
    ## An instance rebuilt by re-expansion keeps its exe_id and can carry
    ## different dependencies, so try_schedule's cache of them is dropped here
    ## and nowhere else: this is the only moment the instance set changes.
    if(!is.null(state$sched_state$dep_ids_cache))
      state$sched_state$dep_ids_cache <- new.env(parent = emptyenv())
  }
  state$required_by     <- build_required_by_index(state$workflow$required_shapes)

  ## The instances DYNAMIC expansion just created share one namespace when their
  ## file output has nothing to tell them apart -- the same defect the build-time
  ## check refuses, on instances it cannot see: they do not exist until a
  ## producer has run and said how many of them there are. A dynamic output is
  ## collected by diffing its directory around the task body, so each instance
  ## also claims whatever a sibling wrote while it ran, and the gather receives
  ## the same file several times.
  ##
  ## Refused HERE, not at build time, because only here is the count known: the
  ## same declaration is safe when the producer yields a single value. Nothing
  ## has been dispatched yet, so the instances' work is not wasted.
  if(length(replace_op)){
    ## A STATIC file output on those same instances loses a result by a plainer
    ## route: every instance resolves to the one literal path and they overwrite
    ## each other, each still satisfying the "did this task write its output"
    ## contract. The build refuses exactly this
    ## (validate_no_colliding_resolved_outputs), but only over the instances a
    ## static wildcard produced; applied here, where the run time count is known,
    ## it covers the two shapes the build cannot see. Object outputs and dynamic
    ## patterns are skipped by the validator itself, the latter being the case
    ## the check below handles.
    .coll <- tryCatch({
      validate_no_colliding_resolved_outputs(
        state$workflow$task_list,
        context = "run time expansion",
        remedy  = paste0("return an object output (new_object); instances created at run ",
                         "time have no wildcard value of their own to put in a file name"))
      NULL
    }, error = function(e) e)
    if(!is.null(.coll)){
      ## The envelope's "task" and "binding" lines come from the exe_id, so they
      ## have to be the COLLIDING instance's.
      .owner <- if(!is.null(.coll$exe_id)) .coll$exe_id
                else state$workflow$task_list[[1L]]$exe_id
      record_error_notify(state, new_error_notify(
        supervisor_ID = "central_scheduler",
        task_id = if(!is.null(.coll$task_id)) .coll$task_id
                  else state$workflow$task_list[[1L]]$task_id,
        exe_id  = .owner,
        message = conditionMessage(.coll)))
      scheduler_teardown(state, 1)
    }

    ## Checked once per (task, instance count): a statically expanded task's
    ## instance set never changes and a dynamically expanded one only grows.
    if(is.null(state$shared_ns_checked))
      state$shared_ns_checked <- new.env(parent = emptyenv())
    .ids <- vapply(state$workflow$task_list, function(t) t$task_id, character(1))
    for(.tid in unique(.ids[duplicated(.ids)])){
      .inst <- state$workflow$task_list[.ids == .tid]
      .key  <- paste0(.tid, "\t", length(.inst))
      if(!is.null(state$shared_ns_checked[[.key]])) next
      assign(.key, TRUE, envir = state$shared_ns_checked)
      ## The instances are RESOLVED here, so a static wildcard has already become
      ## its value: 'd/{sample}_bam_{{i}}.txt' reads as 'd/t1_bam_{{i}}.txt' and
      ## 'd/t2_bam_{{i}}.txt', which do tell the instances apart.
      ##
      ## Equal patterns are the obvious collision but not the only one, because
      ## what claims a file is the GLOB. With samples 'A' and 'A_1' the patterns
      ## differ -- 'out/A_{{n}}.txt' and 'out/A_1_{{n}}.txt' -- yet the first globs
      ## as 'A_*.txt' and accepts 'A_1_v.txt', which belongs to the sibling.
      ##
      ## One instance may declare several dynamic outputs, so the flattened
      ## pattern vector is longer than the instance list. The owner index is kept
      ## beside it, so the report below names the instance whose pattern collided.
      .pat_list <- lapply(.inst, dynamic_file_output_patterns)
      .owner    <- rep(seq_along(.inst), lengths(.pat_list))
      .pats     <- unlist(.pat_list, use.names = FALSE)
      if(length(.pats) < 2L) next
      .clean <- strip_backticks(.pats)
      .dir   <- dirname(.clean)
      .glob  <- gsub("[{][{][^}]+[}][}]", "*", basename(.clean))
      .hit <- NULL
      for(.a in seq_along(.pats)){
        for(.b in seq_len(.a - 1L)){
          if(!identical(.dir[.a], .dir[.b])) next
          if(patterns_overlap(.glob[.a], .glob[.b])){ .hit <- c(.b, .a); break }
        }
        if(!is.null(.hit)) break
      }
      if(is.null(.hit)) next
      .why <- if(identical(.pats[.hit[1L]], .pats[.hit[2L]]))
        sprintf("its dynamic file output '%s' carries nothing to tell them apart",
                .pats[.hit[1L]])
      else
        sprintf("its dynamic file outputs '%s' and '%s' overlap",
                .pats[.hit[1L]], .pats[.hit[2L]])
      ## Count the instances that share THIS pattern, not every instance of the
      ## task: the ones under another static binding resolve to a pattern of
      ## their own and are not part of the collision.
      .same    <- .dir == .dir[.hit[1L]] &
                  vapply(.glob, patterns_overlap, logical(1), .glob[.hit[1L]])
      .n_share <- length(unique(.owner[.same]))
      ## Two overlapping outputs of ONE instance are a different defect, the one
      ## the build-time glob check never sees because it does not compare a task
      ## with itself. It is reported with the instance count of the task.
      if(.n_share < 2L) .n_share <- length(.inst)
      record_error_notify(state, new_error_notify(
        supervisor_ID = "central_scheduler", task_id = .tid,
        exe_id = .inst[[.owner[.hit[1L]]]]$exe_id,
        message = sprintf(
          paste0("expanded into %d instances and %s, so they claim one another's files. ",
                 "Fix: return an object output (new_object)."),
          .n_share, .why)))
      scheduler_teardown(state, 1)
    }
  }

  state$sched_state <- update_scheduler_state(state$sched_state, state$workflow$required_shapes, replace_op)
  state$sched_state <- on_task_finished(state$sched_state, res_notify)

  n_op_to_assign_res <- decode_message_data(msg[[2]]$data, msg[[2]]$data_type, msg[[2]]$is_arrow)
  if(n_op_to_assign_res > 0){
    state$available_fifo <- rbind(state$available_fifo, data.frame(Supervisor_ID = rep(sender, n_op_to_assign_res)))
  }
  if(nrow(state$available_fifo)){
    sched_try_schedule(state)
  }

  if(all(state$task_table$Status == "finished")){
    to_log("All ops finished -- scheduler terminating", "Central scheduler")
    ## serving_count invariant: after the LAST task finished the authoritative set
    ## must be empty and every mirror count 0. A residual means a serve was never
    ## released, so it is reported. A clean run says nothing.
    sc_nz <- Filter(function(w) as.integer(state$sched_state$serving_count[[w]]) != 0L,
                    ls(state$sched_state$serving_count))
    n_pending <- length(ls(state$sched_state$serving_pending))
    if(n_pending || length(sc_nz))
      to_log(sprintf(paste0("serve accounting not empty at teardown: serving_pending=%d, ",
                            "non_zero_serving_count=%d [%s]"),
                     n_pending, length(sc_nz),
                     paste(vapply(sc_nz, function(w)
                       sprintf("%s=%s", w, state$sched_state$serving_count[[w]]), ""), collapse = ", ")),
             "Central scheduler")
    persist_task_table(state)
    scheduler_teardown(state, 0)
  }

  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  ## A full rewrite after an expansion, an append otherwise. Expansion REPLACES
  ## rows, so an exe_id can leave the table, and an appended file has no way to
  ## say that one is gone: folding it would resurrect the row it replaced.
  persist_task_table(state, full = length(replace_op) > 0)
  invisible(NULL)
}

## "error" from a supervisor: an op failed somewhere. Accumulate chunks; once complete,
## log it, drop the failed worker's file-state, record the error_notify on the state (so
## run_workflow() stop()s instead of hanging), then tear down (exit 1).
sched_handle_error <- function(msg, state, recv_socket){
  msg_id   <- msg[[1]]$msg_id
  chunk    <- msg[[1]]$chunk
  n_chunks <- msg[[1]]$n_chunks

  state$res_chunked_message <- push_chunked_message(state$res_chunked_message, msg, msg_id, chunk)
  if(length(state$res_chunked_message[[msg_id]]) != n_chunks){
    state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
    return(invisible(NULL))
  }

  msg <- merge_all_chunks(state$res_chunked_message[[msg_id]], strict = FALSE)
  state$res_chunked_message[[msg_id]] <- NULL
  err_notify <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  ## Resolve the wildcard binding up-front so the LOG line names the sample too,
  ## not just the console block: a post-mortem usually starts from the log.
  err_notify <- attach_wildcard_binding(state, err_notify)
  wc_str <- if(length(err_notify$wildcards))
    paste0(" [", paste(sprintf("%s = %s", names(err_notify$wildcards),
                               vapply(err_notify$wildcards, as.character, character(1))),
                       collapse = ", "), "]") else ""
  err_log <- paste0("WORKFLOW ERROR: op '", err_notify$task_id, "' (exe_id: ", err_notify$exe_id,
                    wc_str, ") on worker '", err_notify$Supervisor_ID, "' failed: ", err_notify$message)
  ## Persist the FULL stderr master-side: run_workflow()'s stop() message may
  ## print only the head, and this is the target of the "full stderr in ..."
  ## pointer that format_workflow_error() emits when it truncates.
  if(!is.null(err_notify$stderr) && nzchar(err_notify$stderr))
    err_log <- paste0(err_log, "\n--- full stderr ---\n", err_notify$stderr)
  to_log(err_log, "Central scheduler")

  ## drop file-state for the failed worker so tasks pending on its in_transit files
  ## could be
  ## retried elsewhere (moot since we terminate below, but keeps the state consistent)
  if(!is.null(err_notify$Supervisor_ID)){
    state$sched_state <- drop_worker_files(state$sched_state, err_notify$Supervisor_ID)
    ## Also prune the dead worker from the holder index so the routing never
    ## sends a fetch to a crashed holder.
    state$sched_state <- drop_holder_worker(state$sched_state, err_notify$Supervisor_ID)
    ## Release any serves the dead worker was source/consumer of.
    state$sched_state <- drop_serving_worker(state$sched_state, err_notify$Supervisor_ID)
  }

  record_error_notify(state, err_notify)
  to_log("Workflow terminated due to operation error", "Central scheduler")
  scheduler_teardown(state, 1)
}


## ── main-loop machinery ──────────────────────────────────────────────────────

## A worker that has stopped talking while it still holds work.
##
## A heartbeat is an explicit signal, not an inference from activity: the
## supervisor's main loop sends one every 20s (FLUMEN_HEARTBEAT_SECS) and keeps
## running while its wrappers work, because they are separate processes. Silence
## is therefore distinguishable from being busy.
##
## Watched: only workers whose beat has been HEARD at least once. A local worker
## never beats (the send is gated on is_remote in supervisor.R), so it never
## enters the map and there is no local/remote test here to get wrong.
##
## Fires only while the worker HOLDS work, that is with a task in `dispatched`
## or `execution`: a silent idle worker stops asking for tasks and the others
## finish.
##
## 600 s (FLUMEN_WORKER_SILENCE_SECS), 30 missed beats.
sched_check_worker_silence <- function(state){
  if(!length(state$last_heartbeat_at) || is.null(state$task_table)) return(invisible(NULL))
  lim <- suppressWarnings(as.numeric(Sys.getenv("FLUMEN_WORKER_SILENCE_SECS", "600")))
  if(!isTRUE(is.finite(lim)) || lim <= 0) return(invisible(NULL))

  now <- Sys.time()
  for(w in names(state$last_heartbeat_at)){
    quiet <- as.numeric(difftime(now, state$last_heartbeat_at[[w]], units = "secs"))
    if(quiet < lim) next
    held <- state$task_table$Exe_ID[!is.na(state$task_table$Worker) &
                                    state$task_table$Worker == w &
                                    state$task_table$Status %in% c("dispatched", "execution")]
    if(!length(held)) next

    ops <- state$task_table$Op_ID[match(held, state$task_table$Exe_ID)]
    to_log(sprintf("WORKER SILENT: %s, %.0fs since its last heartbeat, holding %d task(s)",
                   w, quiet, length(held)), "Central scheduler")
    record_error_notify(state, new_error_notify(
      supervisor_ID = "central_scheduler",
      task_id = as.character(ops)[1],
      exe_id  = held[1],
      message = sprintf(
        paste0("worker '%s' silent for %.0fs while holding %d task(s) (%s). ",
               "Fix: raise FLUMEN_WORKER_SILENCE_SECS if a pause this long is expected."),
        w, quiet, length(held), paste(unique(as.character(ops)), collapse = ", "))))
    scheduler_teardown(state, 1)
  }
  invisible(NULL)
}

## Top of every loop iteration: if not all workers have sent READY by the deadline armed
## by sched_init_workflow(), record a diagnostic error and tear down (exit 1). Covers
## workers not started, firewall blocked, wrong host/self_host. (No-op otherwise.)
sched_check_startup_timeout <- function(state){
  if(is.null(state$startup_deadline) ||
     length(state$workers_ready) >= state$n_workers_expected ||
     Sys.time() <= state$startup_deadline) return(invisible(NULL))

  ## Some workers never registered by the deadline = they did not reach the
  ## scheduler. Report it as the reachability matrix (missing worker -> scheduler
  ## = UNREACHABLE) plus a hint, then tear down -- so the "who can't reach whom"
  ## view is uniform with the connectivity-check phase.
  si <- state$supervisor_info
  
  r <- build_reachability(
    all_workers   = if(is.null(si)) character(0) else as.character(si$Supervisor_ID),
    registered     = state$workers_ready,
    worker_reports = state$reach_reports,
    local_workers  = scheduler_local_workers(state))
  
  missing <- if(!is.null(si)) setdiff(as.character(si$Supervisor_ID), state$workers_ready) else character(0)
  hint <- sprintf(paste0(
    "startup timeout after %.0fs: worker(s) %s did not reach the scheduler. ",
    "Fix: check start_worker on each node, the ports, and master_host / self_host."),
    state$startup_timeout,
    if(length(missing)) paste(shQuote(missing), collapse = ", ") else "<unknown>")
  msg_lines <- c(hint, format_reachability(r))
  for(ln in msg_lines) to_log(ln, "Central scheduler")
  record_error_notify(state, new_error_notify(
    supervisor_ID = if(length(missing)) paste(missing, collapse = ",") else "<unknown>",
    task_id = "<startup>", exe_id = "<startup>", message = paste(msg_lines, collapse = "\n")))
  scheduler_teardown(state, 1)
}

## Route one decoded worker message to its handler (switch on msg_type). Returns
## invisible(NULL); the scheduler's teardown paths (all-finished / error /
## startup-timeout / interrupt) signal the flumen_scheduler_exit condition.
sched_step <- function(msg, recv_socket, state){
  msg_type <- msg[[1]]$msg_type
  ## (a dead worker socket -> its recv AIO is an errorValue -> filtered out by the
  ## no_msg/AIO check in the main loop -> never reaches here)
  switch(msg_type,
         ready          = sched_handle_worker_ready(msg, state, recv_socket),
         file_received  = sched_handle_file_received(msg, state, recv_socket),
         data_received  = sched_handle_data_received(msg, state, recv_socket),
         task_execution = sched_handle_task_execution(msg, state, recv_socket),
         result         = sched_handle_result(msg, state, recv_socket),
         error          = sched_handle_error(msg, state, recv_socket),
         reachability_report = sched_handle_reachability_report(msg, state, recv_socket),
         ## Keepalive frame from a supervisor on the shared sock_res: the only
         ## positive evidence the master gets that a remote worker is still there.
         ## sched_check_worker_silence() reads it.
         heartbeat      = {
           state$last_heartbeat_at[[msg[[1]]$sender]] <- Sys.time()
           ## ANSWER it, so the silence is measurable from the worker's side too:
           ## without a reply nothing detects a lost MASTER, and on a network
           ## partition the isolated worker keeps computing for nobody, its
           ## wrappers left as orphans. The master answers on the connection the
           ## WORKER dialed, the same per-worker PAIR socket it dispatches on.
           ##
           ## Answer only the beat that arrived on ctrl. The worker warms all three
           ## channels every cycle (sock_res, sock_ctrl and sock_term), and one
           ## reply per cycle is all its dead-man switch needs. The supervisor
           ## accepts the frame in the "heartbeat" branch of sup_step.
           if(identical(recv_socket, paste0("sock_ctrl_", msg[[1]]$sender))){
             .hb_sock <- state$socket_list[[recv_socket]]
             if(!is.null(.hb_sock)){
               try(send_heartbeat(.hb_sock, "central_scheduler"), silent = TRUE)
               ## Say it ONCE per worker: the worker's dead-man switch arms only
               ## after it has HEARD a reply.
               if(is.null(state$answered_beat)) state$answered_beat <- character(0)
               if(!(msg[[1]]$sender %in% state$answered_beat)){
                 state$answered_beat <- c(state$answered_beat, msg[[1]]$sender)
                 to_log(paste0("keepalive from ", msg[[1]]$sender,
                               " answered on ", recv_socket,
                               " -- two-way liveness established"), "Central scheduler")
               }
             }
           }
           state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
         },
         {
           to_log(paste0("[DIAG] unexpected msg_type '", msg_type,
                         "' on '", recv_socket, "' -- ignoring + re-arming"),
                  "Central scheduler")
           state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
         })
}

## Catch-all for the scheduler loop: an uncaught R error in dispatch /
## select_worker / try_schedule is caught by run_central_scheduler()'s loop and routed
## here. Log it + a stack trace, record the error so run_workflow() stop()s, then tear
## down (exit 1) so no worker is left orphaned.
scheduler_fatal <- function(e, state){
  err_msg <- conditionMessage(e)
  to_log(paste0("[SCHED-FATAL] uncaught R error: ", err_msg), "Central scheduler")
  tryCatch({
    for(ln in capture.output(print(rlang::trace_back()))) to_log(paste0("[SCHED-FATAL] ", ln), "Central scheduler")
  }, error = function(ee){
    ## rlang may not be available -> fall back to traceback()
    for(ln in tryCatch(capture.output(traceback()), error = function(eee) character(0)))
      to_log(paste0("[SCHED-FATAL-FALLBACK] ", ln), "Central scheduler")
  })
  tryCatch(
    record_error_notify(state, new_error_notify(supervisor_ID = "central_scheduler", task_id = "<scheduler>",
                                                exe_id = "<scheduler>",
                                                ## a condition about the user's data (flumen_user_error,
                                                ## raised while re-expanding) is not an engine fault
                                                message = if(inherits(e, "flumen_user_error")) err_msg
                                                          else paste0("Scheduler internal error: ", err_msg))),
    error = function(ee) to_log(paste0("[SCHED-FATAL] could not record the error: ", conditionMessage(ee)), "Central scheduler"))
  scheduler_teardown(state, 1)
}

## A user interrupt (ESC, Ctrl-C, RStudio Stop) in the run_workflow() session, caught by
## run_central_scheduler()'s loop -- always means "stop the workflow": tear down (exit 1).
scheduler_interrupt <- function(state){
  to_log("Interrupt signal caught -- broadcasting terminate to all supervisors", "Central scheduler")
  scheduler_teardown(state, 1, interrupted = TRUE)
}
