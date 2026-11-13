## Message handlers for the supervisor process (inst/scripts/supervisor.R).
##
## The supervisor's mutable runtime state is bundled into a single environment
## built by new_supervisor_state(); it is passed (by reference) to handlers,
## which mutate `state$<field>` in place so the main loop never re-binds it.
## inst/scripts/supervisor.R routes every message through sup_step() below, so
## the script itself holds no handler.

## Bundled mutable runtime state of the supervisor. One environment, created once
## at start-up; `init_sockets()` fills `socket_list`/`message_list` right after,
## and the handshake handler updates `root_path`/`result_cache`/`p2p_port_pool`
## when a remote worker receives its scheduler config.
new_supervisor_state <- function(worker_name, is_remote, n_concurrent_task, chunk_size,
                                 timeout, sleep_time, self_host, root_path, result_cache,
                                 p2p_pool_start = NULL, p2p_pool_end = NULL){
  st <- new.env(parent = emptyenv())

  ## config -- worker_name/n_concurrent_task/chunk_size/timeout/sleep_time are
  ## placeholders for remote workers until sup_handle_handshake() applies the
  ## scheduler config. self_host is
  ## always authoritative from start-up (auto-detected or start_worker(self_host=)).
  st$worker_name       <- worker_name
  st$is_remote         <- is_remote
  st$n_concurrent_task <- n_concurrent_task
  st$chunk_size        <- chunk_size
  st$timeout           <- timeout
  st$sleep_time        <- sleep_time
  st$self_host         <- self_host
  st$root_path         <- root_path        ## base dir for received files (NULL = cwd); set on handshake for remote
  st$result_cache      <- result_cache     ## new_memory_cache(); NULL for remote until handshake

  ## NNG socket / message tables -- populated by init_sockets() right after construction
  st$socket_list  <- list()
  st$message_list <- list()

  ## per-wrapper bookkeeping
  st$wrapper_status <- init_wrapper_status_table()
  st$reg_tasks      <- c()
  st$all_pid        <- c()

  ## chunked-message reassembly buffers, keyed by msg_id
  st$exe_task_chunked_message <- list()
  st$par_req_chunked_message  <- list()
  st$res_chunked_message      <- list()

  ## cross-worker output_req coalescing: when two consumer wrappers ask for the
  ## same upstream output, the second piggy-backs on the first one's P2P request
  ## via this env (see R/p2p_dispatch.R for the state machine + DESIGN INVARIANT).
  st$pending_resolutions_env <- p2p_make_env()
  ## Wrapper sockets are IPC (co-located). Cross-machine peer transfers use TCP recv
  ## sockets allocated from new_worker(ports = ...)[2:N] (ports[1] is the data-request
  ## listen socket); the pool is cycled/reused -- p2p_busy_ports holds the ports bound
  ## by an in-flight recv socket, p2p_recv_dial maps each such socket name -> its dial
  ## URL. NULL pool => peer transfer via IPC (all-local). For remote workers the pool
  ## is filled by the handshake handler from config$ports.
  st$p2p_port_pool  <- if(!is_remote && !is.null(p2p_pool_start) && !is.null(p2p_pool_end)){
                          seq.int(p2p_pool_start, p2p_pool_end)
                        } else NULL
  st$p2p_busy_ports <- integer(0)
  st$p2p_recv_dial  <- list()

  st
}


## ── handlers ─────────────────────────────────────────────────────────────────

## "terminate" from the scheduler: drop the P2P coalescing state explicitly
## (before clean_termination tears down the rest), then kill all wrapper
## processes, close all sockets, and reset the result cache. Returns "quit" so
## the caller (the main loop) performs quit(status = 0) -- handlers never exit
## the process themselves.
sup_handle_terminate <- function(state){
  p2p_reset_env(state$pending_resolutions_env)
  clean_termination(state$all_pid, state$socket_list, state$result_cache)
  to_log("Supervisor terminated", state$worker_name)
  "quit"
}


## "p2p_transfer_done" ACK from a consumer supervisor: the P2P transfer it
## requested is fully received and merged on its side. Close the per-transfer PUSH
## socket we (the producer) opened toward it -- its name in socket_list is the
## consumer's recv URL, carried in the message. Without this the socket leaks until
## clean_termination. The ACK always arrives on the sock_data_req listen socket,
## which is re-armed here.
sup_handle_p2p_transfer_done <- function(msg, state){
  done_url <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  to_log(paste0("p2p_transfer_done received from ", msg[[1]]$sender, " for ", done_url), state$worker_name)
  if(done_url %in% names(state$socket_list)){
    state$socket_list <- close_socket(state$socket_list, done_url)
  }
  state$message_list <- update_message(state$message_list, state$socket_list, "sock_data_req")
  invisible(NULL)
}


## "handshake" from the scheduler -- the scheduler has connected. For remote workers
## the message carries a config payload (msg[[2]]) to apply: worker name, concurrency,
## chunk size, timeouts, root_path, the result cache, and the P2P port allocation
## (config$ports[1] = data-request listen socket, config$ports[2:N] = transfer pool).
## Local workers already have all of that from start-up. Either way: re-arm the
## control socket and reply READY.
sup_handle_handshake <- function(msg, state, recv_socket){
  if(state$is_remote && length(msg) >= 2){
    config <- decode_message_data(msg[[2]]$data, msg[[2]]$data_type, msg[[2]]$is_arrow)
    state$worker_name       <- config$worker_name
    ## self_host is preserved from start-up args (auto-detected, or passed via
    ## start_worker(self_host=)) -- not overwritten from config: the worker is the
    ## authority on its own address.
    state$n_concurrent_task <- config$n_concurrent_task
    ## Fault-detection thresholds decided on the master. NULL from a master that
    ## predates them, and every reader falls back to this node's own environment
    ## and then to the same default, so a mixed cluster degrades to the previous
    ## behaviour rather than to an undefined one.
    state$thresholds        <- config$thresholds
    state$chunk_size        <- config$chunk_size
    state$timeout           <- config$timeout
    state$sleep_time        <- config$sleep_time
    state$root_path         <- config$root_path
    ## The same mismatch new_worker() warns about, checked here where the truth is
    ## known: the master can compare root_path only against ssh_launcher(cwd =),
    ## while a hand-started worker or a custom launcher can be anywhere. Recorded
    ## in this worker's log, which is where one looks once the symptoms appear --
    ## a file.info() returning NA, a tool reporting a missing input. Not refused:
    ## declared paths may be absolute, and then the two directories do not have
    ## to agree.
    if(!is.null(state$root_path) && nzchar(state$root_path) && dir.exists(state$root_path)){
      .rp <- tryCatch(normalizePath(state$root_path, mustWork = FALSE), error = function(e) state$root_path)
      .wd <- tryCatch(normalizePath(getwd(), mustWork = FALSE),         error = function(e) getwd())
      if(!identical(.rp, .wd)){
        .m <- sprintf(paste("started in '%s' but root_path is '%s'. A file received from a peer is",
                            "rooted under root_path, a file sent by the master lands in the working",
                            "directory, and this task resolves relative paths against the working",
                            "directory: they agree only when the two are the same. Start the worker",
                            "in root_path, or declare absolute paths."), .wd, .rp)
        to_log(paste0("WORKING DIRECTORY MISMATCH: ", .m), state$worker_name)
        message(sprintf("[flumen] worker '%s': %s", state$worker_name, .m))
      }
    }
    cache_dir_use <- if(!is.null(config$cache_dir)) config$cache_dir else tempdir()
    state$result_cache <- new_memory_cache(
      max_ram  = config$cache_ram_limit  * 1e6,
      max_disk = config$cache_disk_limit * 1e6,
      dir      = cache_dir_use
    )
    ## Open the P2P data-request listen socket on config$ports[1]; other workers
    ## dial tcp://self_host:ports[1] to pull data from us. config$ports[2:N] become
    ## the (cycled) transfer-port pool.
    if(!is.null(config$ports) && !is.null(state$self_host)){
      sock_data_req <- nanonext::socket(protocol = "pull",
                                        listen   = paste0("tcp://0.0.0.0:", config$ports[1]))
      state$socket_list  <- add_socket(state$socket_list, sock_data_req, "sock_data_req")
      state$message_list <- add_message(state$message_list, state$socket_list, "sock_data_req")
      if(length(config$ports) > 1L){
        state$p2p_port_pool <- as.integer(config$ports[-1])
      }
    }
    ## The thresholds in force, printed where they are in force: they are decided
    ## on the master, carried here, and live in memory and nowhere else.
    to_log(paste0("Thresholds in force -- beat=",
                  worker_threshold(state, "heartbeat_secs", "FLUMEN_HEARTBEAT_SECS", 20),
                  "s scheduler_silence=",
                  worker_threshold(state, "scheduler_silence_secs", "FLUMEN_SCHEDULER_SILENCE_SECS", 600),
                  "s stalled_transfer=",
                  worker_threshold(state, "stalled_transfer_secs", "FLUMEN_STALLED_TRANSFER_SECS", 600),
                  "s dead_wrapper=",
                  worker_threshold(state, "dead_wrapper_secs", "FLUMEN_DEAD_WRAPPER_SECS", 30),
                  "s (from handshake: ", !is.null(state$thresholds), ")"),
           state$worker_name)
    to_log(paste0("Config received -- worker_name=", state$worker_name,
                  " self_host=", state$self_host,
                  " data_req_port=", if(!is.null(config$ports)) config$ports[1] else NA,
                  " p2p_pool=", if(!is.null(config$ports) && length(config$ports) > 1L)
                                  paste0(config$ports[2], "..", config$ports[length(config$ports)])
                                else "IPC",
                  " n_concurrent_task=", state$n_concurrent_task), state$worker_name)
  }
  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  flag <- send_ready_to_central_scheduler(
    state$socket_list[["sock_res"]],
    state$worker_name,
    state$n_concurrent_task,
    self_host = state$self_host
  )
  to_log("Handshake received from scheduler -- READY sent", state$worker_name)
  invisible(NULL)
}


## "parameter_request" from a consumer supervisor: it wants an output we (the
## producer) hold in the result cache. Look it up by (exe_id, out_instance), open a
## transient PUSH socket to the consumer's recv URL (carried in the request), and
## ship the output back as a chunked "parameter_requested" message. If the consumer
## asked for "file_path" (same-machine / shared FS) we relabel file payloads so only
## the path travels, not the bytes. Re-arms the sock_data_req listener afterwards.
sup_handle_parameter_request <- function(msg, state){
  par_name            <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  exe_id_req          <- decode_message_data(msg[[2]]$data, msg[[2]]$data_type, msg[[2]]$is_arrow)
  socket_par_req_path <- decode_message_data(msg[[3]]$data, msg[[3]]$data_type, msg[[3]]$is_arrow)
  wrapper_id          <- decode_message_data(msg[[4]]$data, msg[[4]]$data_type, msg[[4]]$is_arrow)
  out_instance        <- decode_message_data(msg[[5]]$data, msg[[5]]$data_type, msg[[5]]$is_arrow)
  requested_type      <- if(length(msg) >= 6)
    decode_message_data(msg[[6]]$data, msg[[6]]$data_type, msg[[6]]$is_arrow) else NULL
  ## Fields #7, #8, #9: the three narrowings the consumer asked for, each ""
  ## when it does not apply. Read by position, in the order the builder in this
  ## same file writes them.
  req_output_sel      <- decode_message_data(msg[[7]]$data, msg[[7]]$data_type, msg[[7]]$is_arrow)
  req_output_path_sel <- decode_message_data(msg[[8]]$data, msg[[8]]$data_type, msg[[8]]$is_arrow)
  req_file_sel        <- decode_message_data(msg[[9]]$data, msg[[9]]$data_type, msg[[9]]$is_arrow)

  results  <- cache_get(state$result_cache, exe_id_req)
  inst_arg <- out_instance
  if(is.null(results)){
    ## serve-from-received: we did not PRODUCE this output but may
    ## have RECEIVED it as a P2P consumer and re-cached it under a composite
    ## "exe_id#out_instance" key (see complete_p2p_transfer). The holder stored
    ## EXACTLY the requested instance's payload, so serve the whole stored entry
    ## (instance = NULL), not results[[out_instance]].
    holder_key <- paste0(exe_id_req, "#", if(is.null(out_instance)) "" else out_instance)
    results    <- cache_get(state$result_cache, holder_key)
    if(!is.null(results)) inst_arg <- NULL
  }
  result_instance <- get_result_instance(results, inst_arg)
  param_req       <- result_instance$result
  data_type       <- result_instance$result_type
  result_is_arrow <- result_instance$result_is_arrow
  result_elem_names   <- result_instance$result_elem_names
  ## The declared output each element came from, as the producer stated it.
  result_group_names  <- result_instance$result_group_names

  ## Ship only the files this consumer selected. Guarded to the homogeneous
  ## multi-file case (param_req a list aligned with data_type); other shapes
  ## untouched. n_instances_param below recomputes from the filtered list.
  if((sel_active(req_output_sel) || sel_active(req_output_path_sel) ||
      sel_active(req_file_sel)) &&
     is.list(param_req) && length(param_req) == length(data_type)){
    .flt <- p2p_filter_param(param_req, data_type, result_is_arrow, result_elem_names,
                             result_group_names,
                             req_output_sel, req_output_path_sel, req_file_sel)
    param_req       <- .flt$param_req
    data_type       <- .flt$data_type
    result_is_arrow <- .flt$is_arrow
    result_elem_names   <- .flt$elem_names
    result_group_names  <- .flt$group_names
  }

  ## same-machine requester asked for the path only: skip the byte transfer
  if(identical(requested_type, "file_path")){
    data_type[data_type == "file"] <- "file_path"
  }

  to_log(paste0("Parameter request for ", par_name, " of operation ", exe_id_req,
                " received from ", msg[[1]]$sender), state$worker_name)

  temp_socket <- nanonext::socket(protocol = "push", dial = socket_par_req_path)
  state$socket_list <- add_socket(state$socket_list, temp_socket, socket_par_req_path)

  n_instances_param  <- length(param_req)
  ## propagate is_arrow from the cache per result element; the 3 metadata rows stay qs2
  to_arrow_param_req <- as.logical(result_is_arrow)
  data <- serialize_data(
    c(param_req, par_name, exe_id_req, wrapper_id),
    c(data_type, rep("R_object", 3)),
    c(to_arrow_param_req, FALSE, FALSE, FALSE)
  )
  ## carry the producer's user-list names (if any) on the result rows; "" otherwise
  elem_names_param_req <- if(!is.null(result_elem_names) && length(result_elem_names) == n_instances_param){
    as.character(result_elem_names)
  } else {
    rep("", n_instances_param)
  }
  ## Carry the producer's declared-output label across the worker boundary, so a
  ## consumer on another machine learns which output each element came from.
  ## Read on the other side by R/supervisor_deps.R.
  group_names_param_req <- if(!is.null(result_group_names) && length(result_group_names) == n_instances_param){
    as.character(result_group_names)
  } else {
    rep("", n_instances_param)
  }
  ## The receiver roots a file's name under ITS root_path (see
  ## sup_handle_parameter_requested), so the name must be relative to OURS. A
  ## producer's path already is; a file this worker only RECEIVED was cached
  ## with our root_path in front, and sent as such it landed nested on the
  ## receiver (<their_root>/<our_root>/x), or, on a worker without root_path,
  ## at our absolute path on another machine. The exact inverse of that
  ## file.path(): the bytes are still read from where they are.
  if(!is.null(state$root_path)){
    .root_pre <- paste0(state$root_path, "/")
    .fn <- data$file_name
    .under <- nzchar(.fn) & startsWith(.fn, .root_pre)
    .fn[.under] <- substring(.fn[.under], nchar(.root_pre) + 1L)
    data$file_name <- .fn
  }
  message <- list(
    sender     = rep(state$worker_name, n_instances_param + 3),
    msg_id     = rep(paste0(par_name, "_", wrapper_id, "_", exe_id_req), n_instances_param + 3),  ## must be unique
    msg_type   = c(rep("parameter_requested", n_instances_param),
                   "parameter_requested_name", "parameter_requested_exe_id", "wrapper_id"),
    data_type  = unlist(data$data_type$data_struct),
    ## The three trailer rows (name, exe_id, wrapper_id) carry no group.
    data_names = c(group_names_param_req, "", "", ""),
    ## Per-element producer names ride elem_name. data_names is reserved for
    ## grouping.
    elem_name  = c(elem_names_param_req, "", "", ""),
    data       = data$data,
    is_arrow   = data$data_type$is_arrow,
    file_name  = data$file_name
  )

  send_message_by_chunks(state$worker_name, message, temp_socket,
                         chunk_size = state$chunk_size,
                         defer = list(channel = socket_par_req_path))
  state$message_list <- update_message(state$message_list, state$socket_list, "sock_data_req")
  invisible(NULL)
}


## "result" from a wrapper: it has finished and is streaming back its output.
## Accumulate chunks; once all received, merge, extract the result instances plus the
## dynamic-wildcard / output-count metadata, cache the result, mark co-local
## dependents "ready" and this wrapper "finished", notify the scheduler (result_notify
## + n_op_to_assign to free a concurrent slot), then close the wrapper socket and
## terminate the wrapper process.
sup_handle_result <- function(msg, state){
  msg_id     <- msg[[1]]$msg_id
  msg_sender <- msg[[1]]$sender
  chunk      <- msg[[1]]$chunk
  n_chunks   <- msg[[1]]$n_chunks

  state$res_chunked_message <- push_chunked_message(state$res_chunked_message, msg, msg_id, chunk)
  acc <- length(state$res_chunked_message[[msg_id]])
  to_log(sprintf("[DIAG] wrapper result chunk %s/%s for %s (accumulated %s/%s)",
                 chunk, n_chunks, msg_sender, acc, n_chunks), state$worker_name)
  if(isTRUE(chunk == n_chunks))
    to_log(paste0("Result received from wrapper ", msg_sender), state$worker_name)
  if(acc != n_chunks){
    state$message_list <- update_message(state$message_list, state$socket_list, msg_sender)
    return(invisible(NULL))
  }
  to_log(paste0("[DIAG] wrapper result COMPLETE for ", msg_sender, " -- start processing"), state$worker_name)

  ## -- all chunks received -----------------------------------------------------
  msg <- merge_all_chunks(state$res_chunked_message[[msg_id]])
  state$res_chunked_message[[msg_id]] <- NULL

  results <- get_result_from_msg(msg)
  ## dynamic wildcard + number of outputs (carried as dedicated rows by the wrapper)
  dyn_wildcard <- ""
  n_outputs    <- 1
  types  <- sapply(msg, `[[`, "msg_type")
  w_dyn  <- which(types == "dyn_wildcard")
  w_nout <- which(types == "n_outputs")
  if(length(w_dyn))  dyn_wildcard <- decode_message_data(msg[[w_dyn]]$data,  msg[[w_dyn]]$data_type,  msg[[w_dyn]]$is_arrow)
  if(length(w_nout)) n_outputs    <- decode_message_data(msg[[w_nout]]$data, msg[[w_nout]]$data_type, msg[[w_nout]]$is_arrow)

  to_log(sprintf("[DIAG] result extracted+decoded for %s (n_outputs=%s, dyn_wildcard='%s')",
                 msg_sender, n_outputs, dyn_wildcard), state$worker_name)
  cache_set(state$result_cache, msg_sender, results)
  state$reg_tasks[[msg_sender]]$results <- NULL
  to_log(paste0("[DIAG] result cached for ", msg_sender), state$worker_name)

  ## Dependency rows registered on this producer are NOT released here: a row is
  ## satisfied only by the delivery that fills its constituent, which
  ## sup_handle_wrapper_ready performs from the cache when the consumer announces
  ## itself. The scheduler dispatches a consumer only after its producers
  ## finished, so such rows do not exist on a sound run.
  if(nrow(state$wrapper_status)){
    w <- which(!is.na(state$wrapper_status$Param_dependency_wrapper_ID) &
                 state$wrapper_status$Param_dependency_wrapper_ID == msg_sender)
    if(length(w))
      to_log(sprintf("[CONTRACT] producer %s finished with %d dependency row(s) already registered on it; left for delivery",
                     msg_sender, length(w)), state$worker_name)
  }
  state$wrapper_status[state$wrapper_status$Wrapper_ID == msg_sender, "Status"] <- "finished"

  task_id    <- state$wrapper_status[state$wrapper_status$Wrapper_ID == msg_sender, "Op_ID"]
  ## `results` (from get_result_from_msg above) carries the declared output each
  ## element belongs to; forward it so the scheduler can group by output.
  .opn <- vapply(results, function(r){
            x <- r$output_par_name; if(is.null(x)) "" else as.character(x)[1] },
          character(1))
  ## and the path of each FILE result, so the scheduler knows which file a
  ## per-file consumer instance would receive before it creates it.
  .ofs <- vapply(results, function(r){
            if(identical(r$data_type, "file")) as.character(r$result)[1] else NA_character_ },
          character(1))
  res_notify <- new_result_notify(state$worker_name, task_id, msg_sender, dyn_wildcard, n_outputs,
                                  output_par_names = .opn, output_files = .ofs)

  ## Free a concurrent slot: the scheduler keeps one per-worker dispatch socket alive
  ## for the whole workflow, so a slot is advertised by reporting n_op_to_assign > 0
  ## to restock available_fifo (no per-result socket URL is exchanged).
  n_wrapper_finished <- nrow(state$wrapper_status[
    !duplicated(state$wrapper_status$Wrapper_ID) & state$wrapper_status$Status == "finished", ])
  new_n_op <- n_wrapper_finished
  if(new_n_op > 0){
    state$wrapper_status <- state$wrapper_status[state$wrapper_status$Status != "finished", ]
  }

  data <- serialize_data(list(res_notify, new_n_op), c("", "R_object"), c(FALSE, FALSE))
  message <- list(
    sender     = c(state$worker_name, state$worker_name),
    msg_id     = c(msg_sender, msg_sender),
    msg_type   = c("result", "n_op_to_assign"),
    data_type  = unlist(data$data_type$data_struct),
    data_names = c("", ""),
    data       = data$data,
    is_arrow   = c(FALSE, FALSE),
    file_name  = data$file_name
  )
  to_log(sprintf("[DIAG] SENDING result_notify to scheduler for %s (n_outputs=%s)", msg_sender, n_outputs), state$worker_name)
  ## The result uplink is the ONLY signal that this task finished; if it is lost
  ## the scheduler waits forever and the whole workflow stalls. A long task can
  ## outlast a transient outage (the scheduler host sleeps / a VPN flow drops),
  ## leaving a dead-but-ESTABLISHED pipe that times out the send, so the notify
  ## is retried until accepted. The retries are carried by the send job and
  ## counted by the supervisor's loop: waiting here, or sleeping between
  ## attempts, would stop this loop reading the control channel. A persistent
  ## outage ends in an error raised from the loop when the last attempt fails.
  result_send_max_retries <- 6L
  result_send_retry_sleep <- 5      ## seconds between attempts
  send_message_by_chunks(state$worker_name, message,
                         state$socket_list[["sock_res"]],
                         chunk_size = state$chunk_size,
                         defer = list(
                           channel     = "sock_res",
                           retries     = result_send_max_retries - 1L,
                           retry_delay = result_send_retry_sleep,
                           context     = list(kind       = "result_notify",
                                              msg_sender = msg_sender,
                                              attempts   = result_send_max_retries)))
  to_log(sprintf("[DIAG] result_notify QUEUED to scheduler for %s", msg_sender), state$worker_name)

  state$socket_list  <- close_socket(state$socket_list, msg_sender)
  state$message_list <- close_message(state$message_list, msg_sender)
  pid_wrapper <- state$reg_tasks[[msg_sender]]$pid
  terminate_process(pid_wrapper)
  state$all_pid <- state$all_pid[state$all_pid != pid_wrapper]
  invisible(NULL)
}


## "error" from a wrapper: it hit an op-level error. Accumulate chunks (the payload
## carries stderr, which can be long); once complete, log it, kill the wrapper and
## free its socket, then propagate an error_notify ("error" message) to the scheduler
## -- which broadcasts a global terminate. We then just wait for that terminate (the
## sup_handle_terminate path does the actual teardown).
sup_handle_error <- function(msg, state){
  msg_id     <- msg[[1]]$msg_id
  msg_sender <- msg[[1]]$sender
  chunk      <- msg[[1]]$chunk
  n_chunks   <- msg[[1]]$n_chunks

  state$res_chunked_message <- push_chunked_message(state$res_chunked_message, msg, msg_id, chunk)
  if(isTRUE(chunk == n_chunks))
    to_log(paste0("ERROR received from wrapper ", msg_sender), state$worker_name)
  if(length(state$res_chunked_message[[msg_id]]) != n_chunks){
    state$message_list <- update_message(state$message_list, state$socket_list, msg_sender)
    return(invisible(NULL))
  }

  msg <- merge_all_chunks(state$res_chunked_message[[msg_id]], strict = FALSE)
  state$res_chunked_message[[msg_id]] <- NULL
  err_payload <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  to_log(paste0("OP ERROR detail for ", msg_sender, ": ", err_payload$message), state$worker_name)

  ## kill the wrapper (in case it is hanging) and free its socket
  pid_wrapper <- state$reg_tasks[[msg_sender]]$pid
  if(!is.null(pid_wrapper)) terminate_process(pid_wrapper)
  state$socket_list  <- close_socket(state$socket_list, msg_sender)
  state$message_list <- close_message(state$message_list, msg_sender)
  if(!is.null(pid_wrapper)) state$all_pid <- state$all_pid[state$all_pid != pid_wrapper]

  ## propagate error_notify to the scheduler (it handles global termination)
  task_id_val <- state$wrapper_status[state$wrapper_status$Wrapper_ID == msg_sender, "Op_ID"]
  if(length(task_id_val) == 0) task_id_val <- NA_character_
  send_error_notify_to_scheduler(
    state,
    task_id = task_id_val,
    exe_id  = msg_sender,
    message = err_payload$message,
    stderr  = if(!is.null(err_payload$stderr)) err_payload$stderr else "",
    status  = if(!is.null(err_payload$status)) err_payload$status else NA_integer_
  )
  invisible(NULL)
}

## Build + send a structured error_notify from this supervisor to the scheduler,
## so format_workflow_error() can report task_id / exe_id / worker. Use this for
## any supervisor-originated error that must terminate the workflow (wrapper
## failure, pre-launch missing pre-staged file, P2P pool exhaustion): the bare
## send_error_message() omits Supervisor_ID and exe_id.
send_error_notify_to_scheduler <- function(state, task_id, exe_id, message,
                                           stderr = "", status = NA_integer_){
  err_notify <- new_error_notify(
    supervisor_ID = state$worker_name,
    task_id       = task_id,
    exe_id        = exe_id,
    message       = message,
    stderr        = stderr,
    status        = status
  )
  err_data <- serialize_data(list(err_notify), "R_object", FALSE)
  err_msg_out <- list(
    sender     = state$worker_name,
    msg_id     = exe_id,
    msg_type   = "error",
    data_type  = unlist(err_data$data_type$data_struct),
    data_names = "",
    data       = err_data$data,
    is_arrow   = err_data$data_type$is_arrow,
    file_name  = err_data$file_name
  )
  send_message_by_chunks(state$worker_name, err_msg_out,
                         state$socket_list[["sock_res"]], chunk_size = state$chunk_size,
                         defer = list(channel = "sock_res"))
  to_log(paste0("error_notify sent to scheduler for ", exe_id), state$worker_name)
  invisible(NULL)
}


## "parameter_requested" from a producer supervisor: the (chunked) reply to a P2P
## parameter_request we sent. If this worker has a root_path, received files are
## rooted under it. Accumulate chunks; once complete, complete_p2p_transfer() merges
## them, notifies the scheduler of the received files, fans the resolved param out to
## all coalesced waiters, ACKs the producer, frees the recv-pool port and closes the
## recv/request sockets.
sup_handle_parameter_requested <- function(msg, state, recv_socket){
  msg_id     <- msg[[1]]$msg_id
  msg_sender <- msg[[1]]$sender
  chunk      <- msg[[1]]$chunk
  n_chunks   <- msg[[1]]$n_chunks

  if(isTRUE(chunk == n_chunks))
    to_log(paste0("Parameter requested of operation ", msg_id, " received from ", msg_sender), state$worker_name)

  ## Root a received file under OUR root_path -- but only a RELATIVE name, which
  ## is what "relative to my root" means and is the model the rest of the engine
  ## follows: a holder serves names relative to its own root_path so a re-serve
  ## does not nest twice.
  ##
  ## An ABSOLUTE declared path names a location, not an offset: prepending to it
  ## writes <root>/<declared absolute path>, somewhere the workflow never named.
  ## An R or Python consumer is handed the resolved path and does not notice, but
  ## a shell consumer taking the backtick PREFIX is handed the DECLARED text by
  ## design, so its command looks in a directory that does not exist while the
  ## bytes sit two levels away.
  ##
  ## The three other paths take an absolute name verbatim too: the pre-launch
  ## missing-file check below, the master-to-worker transfer, which never
  ## re-roots, and a dependency read in place under shared_path. "/" as the test
  ## for absolute is the convention used here and in path_under_root().
  if(!is.null(state$root_path)){
    for(j in seq_along(msg)){
      if(isTRUE(msg[[j]]$data_type == "file") &&
         !isTRUE(is_absolute_path(as.character(msg[[j]]$file_name)[1])))
        msg[[j]]$file_name <- file.path(state$root_path, msg[[j]]$file_name)
    }
  }

  state$par_req_chunked_message <- push_chunked_message(state$par_req_chunked_message, msg, msg_id, chunk)
  if(length(state$par_req_chunked_message[[msg_id]]) != n_chunks){
    state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
    return(invisible(NULL))
  }

  res <- complete_p2p_transfer(state$par_req_chunked_message, msg_id,
                               state$socket_list, state$message_list,
                               state$reg_tasks, state$wrapper_status, state$pending_resolutions_env,
                               state$p2p_recv_dial, state$p2p_busy_ports,
                               recv_socket, msg_sender, state$worker_name, state$chunk_size,
                               result_cache = state$result_cache)
  state$par_req_chunked_message <- res$par_req_chunked_message
  state$socket_list             <- res$socket_list
  state$message_list            <- res$message_list
  state$reg_tasks               <- res$reg_tasks
  state$wrapper_status          <- res$wrapper_status
  state$p2p_recv_dial           <- res$p2p_recv_dial
  state$p2p_busy_ports          <- res$p2p_busy_ports
  ## A waiter whose delivery or launch was refused is reported here, where
  ## the state needed to notify the scheduler is available.
  for(.r in res$refused){
    .exe <- state$reg_tasks[[.r$wrapper_id]]$exe_object
    send_error_notify_to_scheduler(state,
      task_id = if(!is.null(.exe$task_id)) .exe$task_id else .r$wrapper_id,
      exe_id  = .r$wrapper_id, message = .r$message)
  }
  invisible(NULL)
}


## "ready" from a wrapper: it has started and is asking for its input parameters.
## Mark its rows ready, then for each declared dependency: if the producer ran on
## this same worker, resolve from the local result cache; otherwise dispatch through
## the cross-worker P2P coalescing state machine (R/p2p_dispatch.R). Once all of the
## wrapper's dependencies are satisfied, ship it its code + parameters.
sup_handle_wrapper_ready <- function(msg, state){
  msg_sender <- msg[[1]]$sender
  state$message_list <- update_message(state$message_list, state$socket_list, msg_sender)

  ## the wrapper's still-unresolved param dependencies
  wrapper_status_req <- state$wrapper_status[state$wrapper_status$Wrapper_ID == msg_sender &
                                               !is.na(state$wrapper_status$Param_dependency), ]
  state$wrapper_status[state$wrapper_status$Wrapper_ID == msg_sender, "Status"] <- "ready"

  if(nrow(wrapper_status_req)){
    for(i in seq_len(nrow(wrapper_status_req))){
      sup_to_req    <- wrapper_status_req[i, "Supervisor_dependency"]
      exe_id_to_req <- wrapper_status_req[i, "Param_dependency_wrapper_ID"]
      param_name    <- wrapper_status_req[i, "Param_dependency"]

      ## The constituent's declared `fetch` chooses the path. A constituent that
      ## states nothing takes the unresolved-dependency error path below.
      .pv    <- state$reg_tasks[[msg_sender]]$exe_object$par_list[[param_name]]
      .req   <- output_req_for(.pv, exe_id_to_req)
      .fetch <- if(is.null(.req) || is.null(.req$fetch)) NA_character_
                else as.character(.req$fetch)[1]

      if(identical(.fetch, "cache")){
        colocal_info <- update_co_local_dependency(state$reg_tasks, msg_sender, param_name,
                                                   exe_id_to_req, state$wrapper_status, state$result_cache)
        if(!is.null(colocal_info$error)){
          ## The resolver refused to deliver (a constituent that would arrive
          ## empty, or a row with no constituent behind it). Report it the way
          ## an unresolved dependency is reported below, and leave the row
          ## unresolved so the wrapper is never launched on partial input.
          exe_obj_req <- state$reg_tasks[[msg_sender]]$exe_object
          task_id_req <- if(!is.null(exe_obj_req$task_id)) exe_obj_req$task_id else msg_sender
          to_log(paste0("DELIVERY REFUSED: ", colocal_info$error), state$worker_name)
          send_error_notify_to_scheduler(state, task_id = task_id_req, exe_id = msg_sender,
                                         message = colocal_info$error)
          next
        }
        if(!is.null(colocal_info$note))
          to_log(paste0("[CONTRACT] ", colocal_info$note), state$worker_name)
        state$reg_tasks      <- colocal_info$reg_tasks
        state$wrapper_status <- colocal_info$wrapper_status

      } else {
        ## Defensive guard: if the scheduler could not resolve this dependency
        ## to a producer, exe_id/address fall to NA, and dialing NA kills the
        ## supervisor. Surface a structured error to the scheduler instead.
        if(is.na(exe_id_to_req) || is.na(sup_to_req) || !nzchar(sup_to_req) || is.na(.fetch)){
          ## Name the task and the dep pattern, not the wrapper's uuid: exe_object
          ## carries task_id, and each output_req carries the unsubstituted
          ## pattern in dyn_wildcard. output_instance is NULL at this point, so
          ## the RESOLVED paths cannot be reported from here.
          exe_obj_req <- state$reg_tasks[[msg_sender]]$exe_object
          task_id_req <- if(!is.null(exe_obj_req$task_id)) exe_obj_req$task_id else msg_sender
          par_req     <- exe_obj_req$par_list[[param_name]]
          pat_req     <- if(inherits(par_req, "output_req_list") && length(par_req))
                           par_req[[1]]$dyn_wildcard else par_req$dyn_wildcard
          err_msg <- sprintf(
            "dependency '%s' of task '%s' has no producer matching %s (exe_id=%s, address=%s, fetch=%s)",
            param_name, task_id_req,
            if(!is.null(pat_req) && nzchar(pat_req)) shQuote(pat_req) else "(pattern not recorded)",
            if(is.na(exe_id_to_req)) "NA" else exe_id_to_req,
            if(is.na(sup_to_req)) "NA" else sup_to_req,
            if(is.na(.fetch)) "NA" else .fetch)
          to_log(paste0("DEPENDENCY UNRESOLVED: ", err_msg), state$worker_name)
          send_error_notify_to_scheduler(state, task_id = task_id_req, exe_id = msg_sender, message = err_msg)
          next
        }
        ## cross-worker output_req -> coalescing state machine in R/p2p_dispatch.R:
        ##   "cached"  -> replay an on-disk hit, no network I/O
        ##   "waiting" -> piggy-back on an in-flight transfer started by another wrapper
        ##   "first"   -> emit a fresh parameter_request (the only send_aio on
        ##                sock_data_req)
        par_val_req  <- state$reg_tasks[[msg_sender]]$exe_object$par_list[[param_name]]
        ## The instance asked for is a field of the constituent of THIS
        ## producer (NULL: the whole result); the container is a list of
        ## constituents and carries none itself.
        .own_req     <- output_req_for(par_val_req, exe_id_to_req)
        out_instance <- if(is.null(.own_req)) NULL else .own_req$output_instance
        is_list_req  <- inherits(par_val_req, "output_req_list")
        ## This exe_id's three narrowings ride in the coalescing key (so
        ## consumers wanting different subsets don't share a fetch) and travel
        ## in the request, so the producer ships only what was asked for.
        sel_req      <- output_req_selectors(par_val_req, exe_id_to_req)
        p2p_key      <- p2p_make_key(exe_id_to_req, out_instance, is_list_req,
                                     sel_req$output_sel, sel_req$output_path_sel,
                                     sel_req$file_sel)
        p2p_decision <- p2p_register_or_check(state$pending_resolutions_env, p2p_key,
                                              waiter = list(wrapper_id = msg_sender, par_name = param_name))

        if(p2p_decision$action == "cached"){
          .res <- apply_p2p_resolution(state$reg_tasks, state$wrapper_status,
                                       msg_sender, param_name, exe_id_to_req,
                                       p2p_decision$par_req, p2p_decision$par_req_types,
                                       p2p_decision$par_req_is_arrow,
                                       p2p_decision$is_from_list_cross, unlist(p2p_decision$paths),
                                       par_req_elem_names = p2p_decision$par_req_elem_names)
          if(!is.null(.res$error)){
            exe_obj_req <- state$reg_tasks[[msg_sender]]$exe_object
            to_log(paste0("DELIVERY REFUSED: ", .res$error), state$worker_name)
            send_error_notify_to_scheduler(state,
              task_id = if(!is.null(exe_obj_req$task_id)) exe_obj_req$task_id else msg_sender,
              exe_id = msg_sender, message = .res$error)
            next
          }
          state$reg_tasks      <- .res$reg_tasks
          state$wrapper_status <- .res$wrapper_status

        } else if(p2p_decision$action == "waiting"){
          ## Only THIS row waits, not every constituent of the same aggregated
          ## dependency: a sibling served from the cache has no delivery pending.
          state$wrapper_status[!is.na(state$wrapper_status$Param_dependency) &
                                 state$wrapper_status$Param_dependency == param_name &
                                 state$wrapper_status$Param_dependency_wrapper_ID == exe_id_to_req &
                                 state$wrapper_status$Wrapper_ID == msg_sender, "Status"] <- "wait_parameter"

        } else {
          ## "first": this wrapper triggers the P2P. Producer config (source of
          ## truth) is in the output_req: uses_tcp => producer listens on TCP (we
          ## need a TCP recv socket); is_local + !is_remote => same physical
          ## machine (shared FS, only the path is needed).
          ##
          ## Those flags are PER-CONSTITUENT, so they are read off the
          ## constituent that belongs to this row's producer (output_req_for),
          ## never off "the first remaining one".
          matching_req <- output_req_for(par_val_req, exe_id_to_req)
          if(is.null(matching_req)){
            exe_obj_req <- state$reg_tasks[[msg_sender]]$exe_object
            task_id_req <- if(!is.null(exe_obj_req$task_id)) exe_obj_req$task_id else msg_sender
            err_msg <- sprintf(paste(
              "parameter '%s' of task '%s' has no constituent from producer '%s',",
              "yet a row of wrapper_status waits for it: the container and the rows",
              "disagree, which is an engine invariant, not a workflow error"),
              param_name, task_id_req, exe_id_to_req)
            to_log(paste0("DELIVERY REFUSED: ", err_msg), state$worker_name)
            send_error_notify_to_scheduler(state, task_id = task_id_req, exe_id = msg_sender, message = err_msg)
            next
          }
          producer_uses_tcp <- isTRUE(matching_req$uses_tcp)
          use_tcp_par <- producer_uses_tcp && !is.null(state$p2p_port_pool)
          if(use_tcp_par){
            ## lowest free port from this worker's P2P pool, cycled and reused:
            ## a port is freed when its recv socket closes
            free_ports <- setdiff(state$p2p_port_pool, state$p2p_busy_ports)
            if(length(free_ports) == 0L){
              err_msg <- sprintf(
                "worker '%s': peer-to-peer port pool (%d..%d, %d ports) exhausted, %d in flight. Fix: widen new_worker(ports = ...).",
                state$worker_name, min(state$p2p_port_pool), max(state$p2p_port_pool),
                length(state$p2p_port_pool), length(state$p2p_busy_ports))
              to_log(paste0("P2P POOL EXHAUSTED: ", err_msg), state$worker_name)
              send_error_notify_to_scheduler(state, task_id = NA_character_, exe_id = msg_sender, message = err_msg)
              next   ## the scheduler will terminate the workflow
            }
            par_port             <- min(free_ports)
            state$p2p_busy_ports <- c(state$p2p_busy_ports, par_port)
            par_sock_info        <- create_temp_socket(state$worker_name, "pull", "listen",
                                                       host = state$self_host, port = par_port)
          } else {
            par_sock_info <- create_temp_socket(state$worker_name, "pull", "listen")
          }
          socket_name <- par_sock_info$socket_name
          socket_path <- par_sock_info$dial_path   ## tcp://self_host:port or ipc://...
          temp_socket <- par_sock_info$socket

          ## "path" when the scheduler declares the source on this machine
          ## (fetch = "path"), "file" for a remote host (fetch = "bytes").
          ## `is_remote` governs the heartbeats; it does not say where data is.
          requested_type  <- if(identical(.fetch, "path")) "file_path" else "file"

          ## Fields #7, #8 and #9: the three narrowings; "" = do not narrow.
          ## Always sent, so the reader takes them by position with no length guard.
          f_out_sel  <- sel_key_part(sel_req$output_sel)
          f_path_sel <- sel_key_part(sel_req$output_path_sel)
          f_file_sel <- sel_key_part(sel_req$file_sel)
          data <- serialize_data(
            list(param_name, exe_id_to_req, socket_path, msg_sender, out_instance, requested_type,
                 f_out_sel, f_path_sel, f_file_sel),
            rep("R_object", 9), rep(FALSE, 9))
          par_req <- new_message(
            sender    = rep(state$worker_name, 9),
            msg_type  = c("parameter_request", "exe_id_req", "socket_data_req",
                          "wrapper_id", "out_instance", "requested_type",
                          "output_sel", "output_path_sel", "file_sel"),
            data_type = unlist(data$data_type$data_struct),
            is_arrow  = data$data_type$is_arrow,
            data      = data$data,
            file_name = data$file_name)

          sock_data_req <- nanonext::socket(protocol = "push", dial = sup_to_req)
          flag <- flumen_send_aio(sock_data_req, par_req$message, mode = "raw")
          ## track sock_data_req, or clean_termination() cannot close it
          state$socket_list  <- add_socket(state$socket_list, sock_data_req,
                                           paste0("send_req_", param_name, "_", exe_id_to_req))
          state$socket_list  <- add_socket(state$socket_list, temp_socket, socket_name)
          state$message_list <- add_message(state$message_list, state$socket_list, socket_name)
          state$p2p_recv_dial[[socket_name]] <- socket_path

          to_log(paste0("Parameter request for ", param_name, " of op ", exe_id_to_req,
                        " sent to ", sup_to_req), state$worker_name)
          p2p_note_peer(state$pending_resolutions_env, p2p_key, sup_to_req)
          ## Only THIS row waits: the request just sent covers this constituent
          ## and no other.
          state$wrapper_status[!is.na(state$wrapper_status$Param_dependency) &
                                 state$wrapper_status$Param_dependency == param_name &
                                 state$wrapper_status$Param_dependency_wrapper_ID == exe_id_to_req &
                                 state$wrapper_status$Wrapper_ID == msg_sender, "Status"] <- "wait_parameter"
        }
      }
    }
  }

  ## all of the wrapper's dependencies satisfied? -> ship its code + parameters.
  ## The rows must say ready AND the container must have nothing left waiting
  ## (container_pending): the rows are a bookkeeping of the input, the
  ## container is the input, and a wrapper is launched only when they agree.
  wrapper_status_req <- state$wrapper_status[state$wrapper_status$Wrapper_ID == msg_sender, ]
  if(all(wrapper_status_req$Status == "ready")){
    .pend <- container_pending(state$reg_tasks[[msg_sender]]$exe_object)
    if(length(.pend)){
      .m <- pending_message(state$reg_tasks[[msg_sender]]$exe_object, msg_sender, .pend)
      to_log(paste0("LAUNCH REFUSED: ", .m), state$worker_name)
      .exe <- state$reg_tasks[[msg_sender]]$exe_object
      send_error_notify_to_scheduler(state,
        task_id = if(!is.null(.exe$task_id)) .exe$task_id else msg_sender,
        exe_id = msg_sender, message = .m)
      return(invisible(NULL))
    }
    message <- prepare_code_and_parameters_for_wrapper(state$reg_tasks[[msg_sender]], state$worker_name, msg_sender)
    send_message_by_chunks(state$worker_name, message, state$socket_list[[msg_sender]],
                           chunk_size = state$chunk_size,
                           defer = list(channel = msg_sender))
    state$wrapper_status[state$wrapper_status$Wrapper_ID == msg_sender, "Status"] <- "execution"
  }
  invisible(NULL)
}


## "execute_task" from the scheduler: a (chunked) op to run here. Accumulate chunks;
## once complete: notify the scheduler of any files that landed via the transfer,
## decode the op + its parameter slots, verify that any "file_path" inputs
## (pre-staged, or already-local) actually exist locally (else error out
## cleanly), open the wrapper's IPC socket, spawn the wrapper process (R / python /
## shell), register it in reg_tasks + wrapper_status, and report task_execution back.
sup_handle_execute_task <- function(msg, state, recv_socket){
  msg_id   <- msg[[1]]$msg_id
  chunk    <- msg[[1]]$chunk
  n_chunks <- msg[[1]]$n_chunks

  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  if(isTRUE(chunk == n_chunks))
    to_log(paste0("EXECUTE_TASK received for operation ", msg_id), state$worker_name)
  state$exe_task_chunked_message <- push_chunked_message(state$exe_task_chunked_message, msg, msg_id, chunk)
  if(length(state$exe_task_chunked_message[[msg_id]]) != n_chunks) return(invisible(NULL))

  ## -- all chunks received -----------------------------------------------------
  msg <- merge_all_chunks(state$exe_task_chunked_message[[msg_id]])
  state$exe_task_chunked_message[[msg_id]] <- NULL

  ## files that arrived via this transfer (dtype="file") are on disk now -- tell the
  ## scheduler so it marks them completed for this worker and unblocks tasks sharing
  ## those paths. (No-op for the local-worker / file_path case: nothing transferred.)
  received_files <- character(0)
  for(j in seq_along(msg)){
    if(j == 1L) next
    if(isTRUE(msg[[j]]$data_type == "file")) received_files <- c(received_files, msg[[j]]$file_name)
  }
  if(length(received_files)){
    send_file_received_to_central_scheduler(state$socket_list[["sock_res"]], state$worker_name, received_files)
  }

  msg_op <- msg[[1]]
  msg_op$data <- decode_message_data(msg_op$data, msg_op$data_type, msg_op$is_arrow)
  ## Pass the executing wrapper's encoding: an R/shell wrapper reads the same
  ## bytes the scheduler sent, so its parameters are forwarded undecoded.
  msg_par_info      <- get_param_info_from_exe_task_msg(
                         msg[-1],
                         consumer_uses_arrow = isTRUE(get_wrapper_info(msg_op$data$type)$use_arrow))
  par_list          <- msg_par_info$par_list
  par_data_types    <- msg_par_info$par_data_types
  op_name <- msg_op$data$exe_id

  ## Pre-launch check for "file_path" params (pre-staged, or already-local on
  ## this worker): the file MUST exist locally now -- a miss means the user's
  ## pre-staging (rsync / NFS / container mount) is incomplete.
  missing_files   <- character(0)
  missing_reasons <- character(0)
  ## Why a file parameter travels as a path is DECLARED per parameter
  ## (params_fetch_reason, uuid-keyed, written where the decision is taken in
  ## R/scheduler_dispatch.R); absent on a message from an older scheduler.
  .reason_of <- function(par_name){
    .i <- match(par_name, msg_op$data$par_names)
    .u <- if(is.na(.i)) NA_character_ else msg_op$data$par_uuids[.i]
    .r <- msg_op$data$params_fetch_reason
    if(is.na(.u) || is.null(.r) || !(.u %in% names(.r))) NA_character_ else as.character(.r[[.u]])
  }
  for(j in seq_along(msg[-1])){
    msg_par <- msg[-1][[j]]
    if(isTRUE(msg_par$data_type == "file_path")){
      par_val <- par_list[[j]]
      paths <- if(is.character(par_val)) par_val
               else if(is.list(par_val)) tryCatch(as.character(unlist(par_val)), error = function(e) character(0))
               else character(0)
      paths <- paths[nzchar(paths)]
      for(p in paths){
        ## Mirror wrapper_shell.R's path resolution: backticks are
        ## static-prefix markers and {{wc}} is glob'd at exec time, so the
        ## pre-launch check only verifies that the parent dir is reachable.
        ## Literal paths are unaffected: gsub is a no-op and the regex does
        ## not match.
        p_check <- strip_backticks(p)
        if(!is.null(find_dynamic_wildcards(p_check))) p_check <- dirname(p_check)
        resolved <- if(is.null(state$root_path) || is_absolute_path(p_check)) p_check else file.path(state$root_path, p_check)
        if(!file.exists(resolved)){
          missing_files   <- c(missing_files, resolved)
          missing_reasons <- c(missing_reasons, .reason_of(as.character(msg_par$data_names)[1]))
        }
      }
    }
  }
  if(length(missing_files) > 0){
    ## The task's own name, not op_name: that is the exe_id, flumen's internal
    ## key, which names nothing the user wrote.
    task_name <- if(!is.null(msg_op$data$task_id)) msg_op$data$task_id else op_name
    ## WHY the bytes were not transferred is declared per parameter by the
    ## scheduler (params_fetch_reason): the local worker, pre_staged(), a
    ## shared_path root, or a file already present from an earlier step. When
    ## the message carries it, the hint names it; the two-way split on
    ## `is_remote` below is kept for a message from a scheduler that does not.
    .known <- unique(missing_reasons[!is.na(missing_reasons)])
    .fix_of <- c(
      local_worker     = "this worker shares the master's filesystem, so nothing is transferred: create the file or correct the path",
      pre_staged       = "the file is declared pre_staged(), so the master did not transfer it: stage it on this worker or drop the declaration",
      shared_path      = "the file lies under the run_workflow(shared_path = ...) root, so it was read in place: make it visible under that root on this worker",
      already_present  = "the file was recorded as already delivered to this worker by an earlier step, so it was not sent again: it has since been removed or moved",
      declared_path    = "the parameter is declared as a path to read in place: create the file or correct the path")
    hint <- if(length(.known) && all(.known %in% names(.fix_of)))
      paste(unname(.fix_of[.known]), collapse = "; ")
    else if(isTRUE(state$is_remote))
      paste0("stage them on the worker, or drop the declaration that marks them as already ",
             "present so that the master transfers them; nothing was transferred because ",
             "the files are declared as already there (pre_staged(), a ",
             "run_workflow(shared_path = ...) root, or an earlier step)")
    else
      paste0("create them or correct the path; this worker shares the master's filesystem, ",
             "so nothing is transferred and pre_staged() makes no difference")
    err_msg <- sprintf(
      "declared file(s) not found on worker '%s': %s. Fix: %s.",
      state$worker_name,
      paste(shQuote(unique(missing_files)), collapse = ", "), hint)
    to_log(paste0("PRE-LAUNCH FILE MISSING: ", err_msg), state$worker_name)
    send_error_notify_to_scheduler(state, task_id = task_name, exe_id = op_name, message = err_msg)
    return(invisible(NULL))   ## skip the wrapper launch; supervisor stays alive for other tasks
  }

  ## supervisor <-> wrapper is always IPC (co-located)
  s_wrapper_name <- paste0(file.path("ipc:/", op_name), ".socket")
  s_wrapper      <- nanonext::socket(protocol = "pair", listen = s_wrapper_name)
  state$socket_list  <- add_socket(state$socket_list, s_wrapper, op_name)
  state$message_list <- update_message(state$message_list, state$socket_list, op_name)
  to_log(paste0("Socket created for operation ", op_name, ": ", s_wrapper_name), state$worker_name)

  ## Multi-path file params are split at dispatch (execute_single_task) and come back
  ## from group_by_name() as a list of N single-path scalars; rebuild the classed
  ## character vector the wrapper expects, using the authoritative per-name data_type.
  ## The "aggregation" marker must state what the user DECLARED, not what the
  ## transport happens to carry: EVERY file param reaches here as a list, a
  ## single declared path included, because emit_param_slots() emits
  ## list(px_kept[[1]]) even at length one. params_is_aggr is the authoritative
  ## declared-gather flag; it is uuid-named and travels on the exe_object.
  exe_object <- msg_op$data
  exe_object$par_list <- group_by_name(par_list, par_data_types)
  .aggr_flags <- exe_object$params_is_aggr
  for(.nm in names(exe_object$par_list)){
    v  <- exe_object$par_list[[.nm]]
    dt <- par_data_types[.nm]
    if(!is.na(dt) && dt %in% c("file", "file_path") && is.list(v) && length(v) > 0L){
      paths <- unlist(v, use.names = FALSE)
      ## group_by_name() keys by parameter NAME, positionally aligned with
      ## par_names/par_uuids; params_is_aggr is keyed by uuid.
      .i       <- match(.nm, exe_object$par_names)
      .k       <- if(is.na(.i)) NA_integer_
                  else match(exe_object$par_uuids[.i], names(.aggr_flags))
      .is_aggr <- !is.na(.k) && isTRUE(unname(.aggr_flags[[.k]]))
      class(paths) <- c(if(.is_aggr) "aggregation",
                        "file_object", "file_path", "flumen_path", "character", "vector")
      exe_object$par_list[[.nm]] <- paths
    }
  }

  ## The container was rebuilt from the slots by their classes (group_by_name).
  ## Its constituent count must equal what the scheduler DECLARED for the
  ## parameter (params_n_constituents): a disagreement means a constituent was
  ## lost or duplicated on the way in, and the wrapper is not launched on it.
  .mismatch <- constituent_count_mismatch(exe_object)
  if(length(.mismatch)){
    task_name <- if(!is.null(exe_object$task_id)) exe_object$task_id else op_name
    err_msg <- sprintf(paste("task '%s': the input rebuilt from the dispatch message disagrees with what the",
                             "scheduler declared: %s. The wrapper is not launched on it."),
                       task_name, paste(.mismatch, collapse = "; "))
    to_log(paste0("CONTAINER MISMATCH: ", err_msg), state$worker_name)
    state$socket_list  <- close_socket(state$socket_list, op_name)
    state$message_list <- close_message(state$message_list, op_name)
    send_error_notify_to_scheduler(state, task_id = task_name, exe_id = op_name, message = err_msg)
    return(invisible(NULL))
  }
  wrapper_info <- get_wrapper_info(msg_op$data$type)

  ## spawn the wrapper; if the interpreter is missing / exec fails, notify the scheduler
  ## (error_notify) rather than waiting forever for a READY that never comes.
  pid <- tryCatch(
    spawn_wrapper(msg_op$data$cmd,
                  c(wrapper_info$wrapper, op_name, s_wrapper_name, state$chunk_size)),
    error = function(e){
      to_log(paste0("Failed to spawn wrapper for ", op_name, ": ", conditionMessage(e)), state$worker_name)
      NA_integer_
    })

  if(is.na(pid)){
    state$socket_list  <- close_socket(state$socket_list, op_name)   ## the pair socket will never be used
    state$message_list <- close_message(state$message_list, op_name)
    err_notify <- new_error_notify(
      supervisor_ID = state$worker_name, task_id = exe_object$task_id, exe_id = op_name,
      message = paste0("Failed to spawn wrapper: '", msg_op$data$cmd, "' (command not found or exec error)"),
      stderr  = "", status = NA_integer_)
    err_data <- serialize_data(list(err_notify), "R_object", FALSE)
    err_msg_out <- list(
      sender = state$worker_name, msg_id = op_name, msg_type = "error",
      data_type = unlist(err_data$data_type$data_struct), data_names = "",
      data = err_data$data, is_arrow = err_data$data_type$is_arrow, file_name = err_data$file_name)
    send_message_by_chunks(state$worker_name, err_msg_out, state$socket_list[["sock_res"]],
                           chunk_size = state$chunk_size,
                           defer = list(channel = "sock_res"))
    to_log(paste0("error_notify sent to scheduler for failed spawn ", op_name), state$worker_name)
    return(invisible(NULL))
  }

  state$all_pid   <- c(state$all_pid, pid)
  state$reg_tasks <- register_task_to_execute(state$reg_tasks, op_name, pid, s_wrapper, exe_object)
  state$wrapper_status <- update_wrapper_status(state$wrapper_status, exe_object$par_list,
                                                op_name, exe_object$task_id, "wait_ready")
  ## report the launch to the scheduler (it tracks free slots via available_fifo)
  data <- serialize_data(list(op_name), "vector", FALSE)
  op_execution <- new_message(sender = state$worker_name, msg_type = "task_execution",
                              data_type = unlist(data$data_type$data_struct), data = data$data,
                              is_arrow = data$data_type$is_arrow)
  flumen_send_aio(state$socket_list[["sock_res"]], op_execution$message, mode = "raw")
  to_log(paste0("Process for ", op_name, " created with PID ", pid), state$worker_name)
  to_log(paste0("TASK_EXECUTION for ", op_name, " sent to central scheduler"), state$worker_name)
  invisible(NULL)
}


## ── main-loop machinery ──────────────────────────────────────────────────────

## Connectivity-check: the scheduler asks this worker (down sock_ctrl) to probe
## a roster of peers' P2P data-request ports. Run the concurrent NNG probe and
## report which peers are reachable back up sock_res. Worker->scheduler is NOT
## probed here (registration already proved it). See R/reachability.R.
sup_handle_probe_peers <- function(msg, state, recv_socket){
  peers <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  reach <- logical(0)
  if(is.data.frame(peers) && nrow(peers) > 0L){
    targets <- lapply(seq_len(nrow(peers)),
                      function(i) c(as.character(peers$Host[i]), peers$Data_req_port[i]))
    res <- probe_tcp_reachable_many(targets, timeout = 5)
    names(res) <- as.character(peers$Supervisor_ID)
    reach <- res
  }
  d <- serialize_data(list(reach), "R_object", FALSE)
  m <- new_message(sender = state$worker_name, msg_type = "reachability_report",
                   data_type = unlist(d$data_type$data_struct), data = d$data,
                   is_arrow = d$data_type$is_arrow, file_name = d$file_name)
  try(flumen_send_aio(state$socket_list[["sock_res"]], m$message, mode = "raw"), silent = TRUE)
  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  invisible(NULL)
}

## Fallback for an unrecognised msg_type in sup_step: drain + re-arm the socket
## that delivered it (otherwise its resolved aio is reprocessed on every loop
## iteration and NO new message is ever received on it -> the socket wedges),
## and log once. Only reachable for a msg_type no handler covers.
sup_default_rearm <- function(msg_type, recv_socket, state, origin = "") {
  to_log(paste0("[DIAG] unexpected msg_type '", msg_type, "' ", origin,
                "on '", recv_socket, "' -- ignoring + re-arming"), state$worker_name)
  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  invisible(NULL)
}

## Route one decoded message to its handler. Messages from the scheduler:
## "terminate" is handled before the switch (it shares a socket with chunked
## EXECUTE_TASK -- act on it without waiting for chunk reassembly). Returns "quit"
## iff the caller (the main loop) should quit(status = 0); invisible(NULL) otherwise.
sup_step <- function(msg, recv_socket, state){
  msg_sender <- msg[[1]]$sender
  msg_type   <- msg[[1]]$msg_type

  if(msg_sender == "central_scheduler"){
    ## Evidence that the master is still there, for the dead-man switch in
    ## supervisor.R. ARMED only by a heartbeat, and deliberately not by any other
    ## message: a master too old to answer beats would otherwise start a clock that
    ## nothing can stop, and every worker would kill itself mid-task after the
    ## threshold. Once armed, ANY message from the scheduler refreshes it -- a
    ## dispatch proves the master is alive just as well as a beat does.
    if(identical(msg_type, "heartbeat")) state$sched_beat_seen <- TRUE
    if(isTRUE(state$sched_beat_seen))    state$last_scheduler_contact <- Sys.time()
    if(identical(msg_type, "terminate")) return(sup_handle_terminate(state))
    ## Keepalive frame from the scheduler (sock_ctrl / sock_term). It carries no
    ## work: just drain + re-arm so it does not fall through the switch below
    ## (which has no default -> the socket would never be re-armed and wedge).
    if(identical(msg_type, "heartbeat")){
      state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
      return(invisible(NULL))
    }
    return(switch(msg_type,
                  handshake    = sup_handle_handshake(msg, state, recv_socket),
                  probe_peers  = sup_handle_probe_peers(msg, state, recv_socket),
                  execute_task = sup_handle_execute_task(msg, state, recv_socket),
                  sup_default_rearm(msg_type, recv_socket, state, "from scheduler ")))
  }

  switch(msg_type,
         ready               = sup_handle_wrapper_ready(msg, state),
         parameter_request   = sup_handle_parameter_request(msg, state),
         p2p_transfer_done   = sup_handle_p2p_transfer_done(msg, state),
         parameter_requested = sup_handle_parameter_requested(msg, state, recv_socket),
         result              = sup_handle_result(msg, state),
         error               = sup_handle_error(msg, state),
         sup_default_rearm(msg_type, recv_socket, state))
}


## Catch-all for the supervisor loop, reached by an unhandled error in any
## handler. Notify the scheduler (it broadcasts terminate -> the whole workflow
## aborts cleanly), tear down, quit(1) -- the supervisor's state may be
## inconsistent after the error.
supervisor_fatal <- function(e, state){
  fatal <- sprintf("supervisor '%s' fatal error: %s", state$worker_name, conditionMessage(e))
  to_log(paste0("SUPERVISOR FATAL: ", fatal), state$worker_name)
  try({
    en <- new_error_notify(supervisor_ID = state$worker_name, task_id = "<supervisor>",
                           exe_id = "<supervisor>", message = fatal, stderr = "", status = NA_integer_)
    ed <- serialize_data(list(en), "R_object", FALSE)
    em <- list(sender = state$worker_name, msg_id = "<supervisor>", msg_type = "error",
               data_type = unlist(ed$data_type$data_struct), data_names = "",
               data = ed$data, is_arrow = ed$data_type$is_arrow, file_name = ed$file_name)
    send_message_by_chunks(state$worker_name, em, state$socket_list[["sock_res"]],
                           chunk_size = state$chunk_size,
                           defer = list(channel = "sock_res"))
  }, silent = TRUE)
  ## The notification is queued, not sent: wait for it here, and only for as
  ## long as it actually needs, instead of a fixed pause.
  try(flush_pending_sends(3), silent = TRUE)
  try(p2p_reset_env(state$pending_resolutions_env), silent = TRUE)
  try(clean_termination(state$all_pid, state$socket_list, state$result_cache), silent = TRUE)
  quit(status = 1)
}


## A wrapper that starts but dies before it ever connects leaves the supervisor
## listening on a PAIR socket nobody will dial. spawn_wrapper() only fails when
## the command cannot be executed at all, so the guard at spawn time cannot
## catch this.
##
## Three conditions must hold together, and each rules out a false positive:
##   - the wrapper's socket is still open. It is closed on the result path and on
##     the error path, so an open one means nothing was ever delivered.
##   - it still has wrapper_status rows. They are dropped once the wrapper
##     finishes, and all(logical(0)) is TRUE, so without this a completed wrapper
##     would look like a dead one.
##   - every row is still "wait_ready". sup_handle_wrapper_ready() flips them all
##     to "ready" the moment the wrapper connects, so this means nothing was ever
##     sent: no result in flight, no chunked message mid-assembly to race with.
##
## `state` is an environment (new_supervisor_state), so this mutates it in place
## and returns nothing, like the sup_handle_* handlers.
check_dead_wrappers <- function(state){

  if(length(state$reg_tasks) == 0L) return(invisible(NULL))

  for(op_name in names(state$reg_tasks)){

    if(!op_name %in% names(state$socket_list)) next

    rows <- state$wrapper_status[state$wrapper_status$Wrapper_ID == op_name, , drop = FALSE]
    if(nrow(rows) == 0L) next

    ## Did this wrapper ever speak? The two accidents are treated differently
    ## below and reported differently to the user.
    connected <- !all(rows$Status == "wait_ready")

    pid <- state$reg_tasks[[op_name]]$pid
    if(is.null(pid) || is.na(pid)) next

    ## The exit status is read ONCE and remembered. wait = FALSE returns NA while
    ## the process lives and the status the first time it has exited -- but that
    ## first read REAPS it, so a second call raises "No child processes".
    ## Polling again would therefore turn a dead wrapper into a permanent skip,
    ## which is exactly the hang this function exists to prevent.
    death <- state$reg_tasks[[op_name]]$death
    if(is.null(death)){
      status <- tryCatch(sys::exec_status(pid, wait = FALSE),
                         error = function(e) NULL)
      if(is.null(status) || length(status) != 1L || is.na(status)) next
      death <- list(status = status, at = Sys.time(), quiet_checks = 0L,
                    seen = length(state$res_chunked_message[[op_name]]))
      state$reg_tasks[[op_name]]$death <- death
    }
    status <- death$status

    ## The wait below applies ONLY to a wrapper that had connected. One that never
    ## dialed back cannot have a partial result in flight, so there is nothing to
    ## wait for and it is reported at once.
    if(connected){

      ## QUIESCENCE, not a deadline. The process is gone, so nothing NEW can be
      ## produced; what may still be pending is the remainder it handed to the
      ## socket before dying. That remainder cannot be waited for by state alone --
      ## "no further chunk will ever arrive" is a claim about the future -- so it is
      ## established by observing that nothing moves.
      ##
      ## Both conditions must hold: the wall clock alone would let a starved
      ## supervisor accumulate the delay without ever having read, and the
      ## consecutive-check count alone would be satisfied too quickly on a fast
      ## loop. The poll and this check run in the SAME thread, with the poll
      ## first, so a blocked loop accumulates neither.
      ##
      ## The thresholds come from the handshake when the master sent them,
      ## otherwise this node's own environment, otherwise the default.
      grace_secs   <- worker_threshold(state, "dead_wrapper_secs", "FLUMEN_DEAD_WRAPPER_SECS", 30)
      grace_checks <- as.integer(worker_threshold(state, "dead_wrapper_checks",
                                                  "FLUMEN_DEAD_WRAPPER_CHECKS", 5))
      if(!is.finite(grace_secs))   grace_secs   <- 30
      if(is.na(grace_checks))      grace_checks <- 5L

      now_seen <- length(state$res_chunked_message[[op_name]])
      if(now_seen != death$seen){
        ## the result is still being assembled -- however slowly, it is progressing
        state$reg_tasks[[op_name]]$death$seen         <- now_seen
        state$reg_tasks[[op_name]]$death$quiet_checks <- 0L
        next
      }
      state$reg_tasks[[op_name]]$death$quiet_checks <- death$quiet_checks + 1L
      if(state$reg_tasks[[op_name]]$death$quiet_checks < grace_checks) next
      if(as.numeric(difftime(Sys.time(), death$at, units = "secs")) < grace_secs) next

      ## Final drain: give this socket one explicit chance to hand over anything the
      ## poll has not picked up yet, so the verdict is never "we did not look".
      if(op_name %in% names(state$message_list)){
        m <- state$message_list[[op_name]]
        if(!isTRUE(nanonext::unresolved(m)) && !isTRUE(nanonext::is_error_value(m))) next
      }
    }

    ## waitpid-shifted status (256 for exit code 1), as in launch_remote_workers().
    if(is.numeric(status) && status > 255L && (status %% 256L) == 0L)
      status <- as.integer(bitwShiftR(status, 8L))

    exe_object <- state$reg_tasks[[op_name]]$exe_object
    task_id    <- if(!is.null(exe_object$task_id)) exe_object$task_id else op_name
    cmd        <- if(!is.null(exe_object$cmd)) exe_object$cmd else "<unknown>"
    type       <- if(!is.null(exe_object$type)) exe_object$type else "<unknown>"

    hint <- if(connected)
      " Fix: check the worker log and the system log, often the OOM killer."
    else if(identical(type, "python"))
      paste0(" Fix: check that '", cmd, "' has pyarrow, pandas, numpy and pynng.")
    else
      " Fix: see the worker log for the command's own error."

    when <- if(connected) "while running the task" else "before connecting"

    to_log(paste0("Wrapper ", op_name, " (pid ", pid, ") exited with status ", status,
                  " ", when, "."), state$worker_name)

    state$socket_list  <- close_socket(state$socket_list, op_name)
    state$message_list <- close_message(state$message_list, op_name)
    state$all_pid      <- state$all_pid[state$all_pid != pid]

    send_error_notify_to_scheduler(
      state,
      task_id = task_id,
      exe_id  = op_name,
      message = paste0("the ", type, " wrapper exited with status ", status, " ", when,
                       ", without delivering a result.", hint),
      status  = status
    )
  }

  invisible(NULL)
}


## A fetch from a peer that stopped answering. Without this check the verdict
## comes from the operating system's own TCP timeout, and it blames the waiting
## consumer: the producer has finished its task, so the master's worker-silence
## check skips it by design ("only workers holding work").
##
## QUIESCENCE, not a deadline, exactly as in check_dead_wrappers above: a 30 GB
## transfer is slow and healthy, so the question asked here is only whether NEW
## chunks are still arriving. The default is long (600 s) because a fetch sitting
## at zero chunks is indistinguishable from a producer that is simply busy and
## has not begun serving yet.
check_stalled_transfers <- function(state){
  env <- state$pending_resolutions_env
  if(is.null(env)) return(invisible(NULL))

  secs   <- worker_threshold(state, "stalled_transfer_secs", "FLUMEN_STALLED_TRANSFER_SECS", 600)
  checks <- as.integer(worker_threshold(state, "stalled_transfer_checks",
                                        "FLUMEN_STALLED_TRANSFER_CHECKS", 5))
  if(!isTRUE(is.finite(secs)) || secs <= 0) return(invisible(NULL))   ## 0 disables
  if(is.na(checks) || checks < 1L) checks <- 5L

  for(key in ls(env)){
    entry <- tryCatch(get(key, envir = env, inherits = FALSE), error = function(e) NULL)
    if(is.null(entry) || !identical(entry$status, "in_transit")) next
    if(is.null(entry$started)) next            ## no start time: nothing to measure from
    if(!length(entry$waiters)) next            ## nobody is waiting: nothing to fail

    ## The chunk accumulator is keyed by the msg_id the PRODUCER stamps on its
    ## reply, which it composes as "<par_name>_<wrapper_id>_<producer exe_id>"
    ## (sup_handle_parameter_request). The coalescing key carries only the exe_id,
    ## in its first ":"-separated field, so the other two are read off the waiter
    ## that triggered the fetch: p2p_register_or_check() creates the entry with
    ## that waiter and appends the coalesced ones after it, so it is waiters[[1]].
    w1       <- entry$waiters[[1]]
    msg_id   <- paste0(w1$par_name, "_", w1$wrapper_id, "_", sub(":.*$", "", key))
    now_seen <- length(state$par_req_chunked_message[[msg_id]])

    if(!identical(as.integer(now_seen), as.integer(entry$seen))){
      entry$seen          <- as.integer(now_seen)   ## progressing, however slowly
      entry$quiet_checks  <- 0L
      entry$last_progress <- Sys.time()
      assign(key, entry, envir = env)
      next
    }

    entry$quiet_checks <- as.integer(entry$quiet_checks) + 1L
    assign(key, entry, envir = env)
    if(entry$quiet_checks < checks) next
    ## Measured from the LAST chunk that arrived, not from the start of the
    ## fetch: otherwise a healthy transfer running for longer than `secs` in
    ## total is failed after a few quiet checks.
    since <- if(is.null(entry$last_progress)) entry$started else entry$last_progress
    if(as.numeric(difftime(Sys.time(), since, units = "secs")) < secs) next

    peer <- if(is.null(entry$peer)) "an unknown peer" else entry$peer
    to_log(paste0("STALLED TRANSFER: nothing received from ", peer, " for ", round(secs),
                  "s (", now_seen, " chunk(s) in); failing the ", length(entry$waiters),
                  " task(s) waiting on it instead of waiting further."), state$worker_name)

    ## Remove BEFORE reporting: a second pass must not fail the same tasks twice
    ## if the notify path itself takes a loop iteration.
    rm(list = key, envir = env)

    for(wt in entry$waiters){
      wid <- wt$wrapper_id
      exe <- tryCatch(state$reg_tasks[[wid]]$exe_object, error = function(e) NULL)
      tid <- if(!is.null(exe) && !is.null(exe$task_id)) exe$task_id else wid
      send_error_notify_to_scheduler(
        state,
        task_id = tid,
        exe_id  = wid,
        message = paste0(
          "no data arrived from '", peer, "' for ", round(secs), "s while fetching parameter '",
          if(is.null(wt$par_name)) "<unknown>" else wt$par_name,
          "' (output of ", msg_id, "), after ", now_seen, " chunk(s). ",
          "Fix: raise FLUMEN_STALLED_TRANSFER_SECS on the master."))
    }
  }
  invisible(NULL)
}
