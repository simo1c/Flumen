## Bringing workers online: spawn local supervisors, open data channels to remote
## supervisors, and register each worker P2P data-request endpoint. Used by the
## central scheduler (run_central_scheduler() in R/scheduler_run.R).

## Build the supervisor_info registry for ALL workers, but only LAUNCH the local
## ones. Asymmetric by design:
##   - LOCAL workers  -> spawned here as child subprocesses (sys::exec_background of
##     supervisor.R), with freshly generated IPC socket paths; the record captures
##     their PID + paths.
##   - REMOTE workers -> NOT launched here. They are worker-initiated: already
##     started before the scheduler by launch_remote_workers() (R/execution.R) via
##     the pluggable launch_worker() launcher (e.g. ssh_launcher), and they dial IN.
##     Here we only RECORD their metadata (Port_base = ctrl_port, Data_req_port,
##     Uses_tcp); Socket_data_req/Host are filled lazily once their READY arrives.
## Returns the supervisor_info data.frame.
launch_local_and_register_supervisors <- function(worker_list,
                                  chunk_size,
                                  timeout        = "NULL",
                                  sleep_time     = "0.1",
                                  sock_res_path  = NULL){  ## scheduler's unique sock_res path (local sup dials here)

  sup_info <- data.frame(Supervisor_ID   = NULL,
                         PID             = NULL,
                         Socket_data_req = NULL,
                         Socket_ctrl     = NULL,
                         Socket_term     = NULL,
                         Host            = NULL,
                         Port_base       = NULL,
                         Data_req_port   = NULL,
                         Uses_tcp        = NULL)

  for(i in seq_along(worker_list)){

    worker          <- worker_list[[i]]
    worker_name     <- worker$name
    n_concurrent_task <- worker$n_concurrent_task
    is_remote       <- !is.null(worker$launcher)

    if(!is_remote){

      ## -- LOCAL WORKER ------------------------------------------------------
      ## If self_host is set (mixed local+remote setup), use TCP so remote
      ## workers can dial in for peer-to-peer parameter fetch: data-request
      ## listen on worker$ports[1], transfer pool worker$ports[2:N].
      ## Otherwise (all-local workflow) use IPC.
      if(!is.null(worker$self_host)){
        req_data_sock  <- generate_socket_path(worker_name,
                                               host = worker$self_host,
                                               port = worker$ports[1])
        p2p_pool_start <- worker$ports[2]
        p2p_pool_end   <- worker$ports[length(worker$ports)]
      } else {
        req_data_sock  <- generate_socket_path(worker_name)
        p2p_pool_start <- "NULL"
        p2p_pool_end   <- "NULL"
      }
      ## Per-worker dispatch channel (sock_ctrl_<name>): scheduler PUSH listen,
      ## supervisor PULL dial. Each local supervisor gets its own unique IPC
      ## path; broadcasts are emulated by the scheduler iterating over every
      ## sock_ctrl_<name> (mirrors the existing pattern for remote workers).
      sock_ctrl_local <- paste0("ipc://", short_ipc_file())
      ## Dedicated terminate channel (see init_sockets()): a separate IPC socket
      ## that never carries bulk EXECUTE_TASK data, so a terminate is never
      ## head-of-line blocked behind a chunked task transfer.
      sock_term_local <- paste0("ipc://", short_ipc_file())
      self_host_arg    <- if(!is.null(worker$self_host)) worker$self_host else "NULL"
      cache_ram_arg    <- if(!is.null(worker$cache_ram_limit))  worker$cache_ram_limit  else 4096L
      cache_disk_arg   <- if(!is.null(worker$cache_disk_limit)) worker$cache_disk_limit else 51200L
      cache_dir_arg    <- if(!is.null(worker$cache_dir)) worker$cache_dir else tempdir()
      sock_res_arg     <- if(!is.null(sock_res_path))  sock_res_path  else "NULL"

      ## tryCatch: if exec_background fails for an intermediate worker, kill
      ## already-spawned supervisors to avoid zombies and stop with a structured
      ## error identifying the failing worker.
      supervisor_script <- system.file("scripts", "supervisor.R", package = "flumen")
      if(!nzchar(supervisor_script)) supervisor_script <- "supervisor.R"  # fallback dev mode
      pid <- tryCatch(
        sys::exec_background(
          rscript_cmd(),
          args = c(
            supervisor_script,
            worker_name,
            req_data_sock$socket_path,
            sock_ctrl_local, ## arg3:  per-worker sock_ctrl_<name> dial path
            n_concurrent_task,
            chunk_size,
            timeout,
            sleep_time,
            sock_res_arg,    ## arg8:  sock_res dial path (scheduler's PULL socket)
            p2p_pool_end,    ## arg9:  P2P transfer pool end port ("NULL" if IPC)
            self_host_arg,   ## arg10: self_host
            p2p_pool_start,  ## arg11: P2P transfer pool start port ("NULL" if IPC)
            cache_ram_arg,   ## arg12: cache_ram_limit (MB)
            cache_disk_arg,  ## arg13: cache_disk_limit (MB)
            cache_dir_arg,   ## arg14: cache_dir
            sock_term_local, ## arg15: per-worker sock_term_<name> dial path (dedicated terminate channel)
            if(!is.null(worker$root_path)) worker$root_path else "NULL"               ## arg16: root_path
          )
        ),
        error = function(e){
          ## Rollback: kill supervisors already spawned (local ones with valid PID)
          prev_pids <- suppressWarnings(as.integer(sup_info$PID))
          prev_pids <- prev_pids[!is.na(prev_pids)]
          for(prev_pid in prev_pids) try(tools::pskill(prev_pid), silent = TRUE)
          stop(flumen_launch_fail(
            worker = worker_name,
            error  = paste0("could not launch the supervisor: ", conditionMessage(e)),
            fix    = "check that Rscript is on PATH on this machine"),
            call. = FALSE)
        }
      )

      new_entry <- data.frame(Supervisor_ID   = worker_name,
                              PID             = as.character(pid),
                              Socket_data_req = req_data_sock$dial_path,
                              Socket_ctrl     = sock_ctrl_local,
                              Socket_term     = sock_term_local,
                              Host            = "",
                              Port_base       = NA_integer_,
                              Data_req_port   = NA_integer_,
                              Uses_tcp        = !is.null(worker$self_host))

    } else {

      ## -- REMOTE WORKER -----------------------------------------------------
      ## Worker-initiated mode: the supervisor (started by start_worker on the
      ## remote host, or auto-launched via the worker's launcher) dials in to
      ## the master. master_url has format "tcp://host:notify_port,ctrl_port":
      ## the master's notify_port is SHARED by all remote supervisors (fan-in
      ## results uplink), while ctrl_port is dedicated per worker (terminate
      ## port = ctrl_port + 1, worker-derived). The worker's own peer-to-peer
      ## data-request port is worker$ports[1] (delivered via the handshake).
      ## Socket_data_req is filled in lazily after the worker's READY arrives
      ## with self_host. Port_base stores ctrl_port (per-worker key);
      ## open_remote_worker_channels() opens listens at ctrl_port and ctrl_port+1.
      body          <- sub("^tcp://", "", worker$master_url)
      pp            <- strsplit(sub("^[^:]+:", "", body), ",", fixed = TRUE)[[1]]
      master_port   <- as.integer(pp[2])   ## ctrl_port (worker-unique)
      data_req_port <- worker$ports[1]
      pid <- NA

      new_entry <- data.frame(Supervisor_ID   = worker_name,
                              PID             = NA_character_,
                              Socket_data_req = NA_character_,    ## set after READY
                              Socket_ctrl     = "NULL",
                              Socket_term     = "NULL",           ## remote: derived from Port_base + 1
                              Host            = NA_character_,    ## set after READY
                              Port_base       = master_port,
                              Data_req_port   = data_req_port,
                              Uses_tcp        = TRUE)
    }

    sup_info <- rbind(sup_info, new_entry)
    if(!is_remote){
      message("[", worker_name, "] Executed with PID ", pid)
    } else {
      message("[", worker_name, "] Remote worker, master ctrl port ", master_port)
    }
  }

  return(sup_info)
}

## Open the TCP res/ctrl/term listen sockets for each remote worker and send its
## handshake. Worker-initiated: the remote supervisor dials in to the master,
## which PULL-listens on Port_base (result/READY uplink), PUSH-listens on
## Port_base + 1 (control/dispatch) and PUSH-listens on Port_base + 2 (dedicated
## terminate channel). The handshake carries the worker's config
## including its own peer-to-peer port range (ports[1] = data-request listen,
## ports[2:N] = transfer pool); NNG buffers the PUSH handshake until the
## worker's PULL dial connects. Returns the updated socket_list/message_list.
## (Local workers' IPC sock_ctrl is opened inline in sched_init_workflow(); only
## the remote side is non-trivial enough to extract.)
open_remote_worker_channels <- function(supervisor_info, 
                                        worker_list,
                                        socket_list, 
                                        message_list,
                                        chunk_size, 
                                        timeout, 
                                        sleep_time){
  
  remote_sup <- supervisor_info[!is.na(supervisor_info$Port_base), ]
  ## Handshake send_aio handles, kept alive past this function: the handshake is
  ## sent before the worker's sock_ctrl PAIR has dialed in, so the send stays
  ## pending until the worker connects. nanonext cancels a pending aio if it is
  ## garbage-collected, which silently drops the handshake (worker then never
  ## registers). Returning these to the caller (-> state) keeps them referenced
  ## for the run, so the pending send survives until delivery.
  hs_aios <- list()
  for(ri in seq_len(nrow(remote_sup))){
    rw_name   <- remote_sup[ri, "Supervisor_ID"]
    ctrl_port <- as.integer(remote_sup[ri, "Port_base"])   ## per-worker ctrl port; terminate = ctrl_port + 1
    term_port <- ctrl_port + 1L
    ctrl_name <- paste0("sock_ctrl_", rw_name)
    term_name <- paste0("sock_term_", rw_name)
    ## tryCatch: NNG can fail to bind if the port is already in use by another
    ## process; surface a structured error (caught by the scheduler's top-level
    ## tryCatch -> workflow aborts cleanly).
    ## Note: sock_res is opened ONCE (shared TCP listen on master_notify_port)
    ## by init_sockets() at scheduler start-up; here only the per-worker
    ## ctrl + term channels are bound.
    socks <- tryCatch({
      ctrl_s <- nanonext::socket("pair", listen = paste0("tcp://0.0.0.0:", ctrl_port))
      term_s <- nanonext::socket("pair", listen = paste0("tcp://0.0.0.0:", term_port))
      list(ctrl = ctrl_s, term = term_s)
    }, error = function(e){
      stop(flumen_launch_fail(
        worker = rw_name,
        error  = sprintf("could not open the ctrl/term listen sockets on ports %d/%d: %s",
                         ctrl_port, term_port, conditionMessage(e)),
        fix    = "a port may be in use, widen run_workflow(ports = ...)"), call. = FALSE)
    })
    socket_list  <- add_socket(socket_list, socks$ctrl, ctrl_name)
    socket_list  <- add_socket(socket_list, socks$term, term_name)
    ## ctrl/term are now PAIR: arm recv so the worker's keepalive heartbeats
    ## (worker->master, up direction) are drained by the sched_step "heartbeat"
    ## case. Without draining, NNG's recv buffer would eventually back-pressure
    ## the worker's heartbeat send. Dispatch (down) still uses the same socket.
    message_list <- add_message(message_list, socket_list, ctrl_name)
    message_list <- add_message(message_list, socket_list, term_name)
    to_log(paste0("Listening for remote worker ", rw_name,
                  " on ports ", ctrl_port, "/", term_port,
                  " (notify via shared scheduler-wide port)"), "Central scheduler")

    rw_worker <- worker_list[[rw_name]]
    rw_config <- list(
      worker_name        = rw_name,
      n_concurrent_task  = rw_worker$n_concurrent_task,
      chunk_size         = chunk_size,
      timeout            = timeout,
      sleep_time         = as.numeric(sleep_time),
      cache_ram_limit    = if(!is.null(rw_worker$cache_ram_limit))  rw_worker$cache_ram_limit  else 4096L,
      cache_disk_limit   = if(!is.null(rw_worker$cache_disk_limit)) rw_worker$cache_disk_limit else 51200L,
      cache_dir          = rw_worker$cache_dir,  ## NULL -> supervisor uses tempdir()
      root_path          = rw_worker$root_path,
      ports              = rw_worker$ports,      ## P2P port range (ports[1]=data_req, ports[2:N]=pool)
      ## Fault-detection thresholds, read from the MASTER's environment and carried
      ## here rather than left to whatever each node happens to have. They are read
      ## on the supervisor side, and nothing else carries them: SSH does not
      ## forward the client's environment and the remote bootstrap exports nothing,
      ## so without this a remote node runs on the compiled-in defaults however
      ## they were set where the run was launched. A localhost worker hides the
      ## gap, being a CHILD of the master and inheriting the lot.
      ##
      ## The launcher is the wrong place to carry them. Only the ssh method builds
      ## a bootstrap line, and a plain function is a documented launcher, so
      ## anything added to ours would never reach one a user writes. This
      ## handshake travels on flumen's own connection, so it is launcher-agnostic
      ## by construction, including for launchers not yet written.
      ##
      ## Every threshold here is needed only AFTER contact, which is what makes the
      ## handshake a valid carrier: a stalled fetch presupposes a fetch, a dead
      ## wrapper presupposes a dispatched task, and the scheduler-silence switch
      ## arms only once a reply has been heard. The one NOT here is the orphan
      ## timeout, which governs precisely the case where this handshake never
      ## arrives.
      thresholds         = worker_thresholds()
    )
    hs_data <- serialize_data(list("handshake", rw_config),
                              c("R_object", "R_object"), c(FALSE, FALSE))
    hs_msg  <- new_message(
      sender     = c("central_scheduler", "central_scheduler"),
      msg_id     = "handshake",
      msg_type   = c("handshake", "handshake"),
      data_type  = unlist(hs_data$data_type$data_struct),
      data_names = c("", "config"),
      data       = hs_data$data,
      is_arrow   = hs_data$data_type$is_arrow,
      ## NOT chunked: this call sends one chunk and no loop follows it, and
      ## sup_handle_handshake() decodes on arrival instead of accumulating. The
      ## config carries only this worker's settings, so it is bounded anyway.
      file_name  = hs_data$file_name
    )
    hs_aios[[as.character(rw_name)]] <- nanonext::send_aio(socks$ctrl, hs_msg$message, mode = "raw")
    to_log(paste0("Handshake sent to remote worker ", rw_name), "Central scheduler")
  }
  list(socket_list = socket_list, message_list = message_list, handshake_aios = hs_aios)
}

## After a remote worker's READY arrives carrying its self_host, fill in the
## worker's Host and Socket_data_req in supervisor_info -- the lazy P2P endpoint
## that peer workers (and a co-located local worker) dial. Returns the updated
## supervisor_info. (Local IPC workers have Data_req_port == NA -> nothing to
## fill; their Socket_data_req keeps its launch-time value.)
## Reachability is NOT probed here: every directed edge (worker->scheduler and
## worker<->worker) is checked uniformly in the connectivity-check phase after
## all workers register (see R/reachability.R).
register_worker_p2p_endpoint <- function(supervisor_info, sender, ready_self_host){
  if(!(nzchar(ready_self_host) && !is.null(supervisor_info))) return(supervisor_info)
  sup_idx <- which(supervisor_info$Supervisor_ID == sender)
  if(!(length(sup_idx) == 1L && !is.na(supervisor_info$Data_req_port[sup_idx]))) return(supervisor_info)

  drp <- as.integer(supervisor_info$Data_req_port[sup_idx])
  supervisor_info$Host[sup_idx]            <- ready_self_host
  supervisor_info$Socket_data_req[sup_idx] <- paste0("tcp://", ready_self_host, ":", drp)
  supervisor_info
}

