## Reachability matrix: pure logic for the connectivity-check phase.
## Inputs are gathered by the scheduler (registration set + per-worker
## reachability_report); these functions turn them into a directed
## who-reaches-whom matrix, decide pass/fail, and render it for the user. Kept
## pure: they touch no socket and hold no state.
##
## Edge "kind":
##   control : worker -> scheduler (from registration; missing = UNREACHABLE)
##   p2p     : worker -> worker     (from each worker's reachability_report)
## There is NO scheduler->worker edge: the scheduler is never a P2P endpoint;
## a worker co-located with the scheduler probes its own peers like any other.

## Build the directed reachability matrix.
##   all_workers    : character vector of all expected worker names
##   registered     : character vector of workers that sent READY
##   worker_reports : named list; worker_reports[[w]] is a named logical vector
##                    peer -> reachable (from w's point of view)
##   local_workers  : names of workers co-located with the scheduler (used only
##                    to enrich the remediation hint for a failing p2p edge)
## Returns list(edges, failures, ok, local_workers): edges/failures are long
## data.frames (from, to, kind, reachable, required); ok iff no required edge down.
build_reachability <- function(all_workers, registered, worker_reports = list(),
                               local_workers = character(0)){
  rows <- list()
  add <- function(from, to, kind, reachable, required)
    rows[[length(rows) + 1L]] <<- data.frame(
      from = from, to = to, kind = kind,
      reachable = isTRUE(reachable), required = isTRUE(required),
      stringsAsFactors = FALSE)

  ## worker -> scheduler : registration is the ground truth (registered = reached)
  for(w in all_workers)
    add(w, "scheduler", "control", w %in% registered, TRUE)

  ## worker <-> worker : each worker reports which peers it reached. A worker
  ## co-located with the scheduler (local_workers) probes its peers like any
  ## other, so the old "scheduler must reach the remote" R3 case is just that
  ## worker's own p2p edges -- the scheduler is NEVER a P2P endpoint itself
  ## (data moves worker<->worker, never through the master), so there is no
  ## scheduler->worker probe.
  for(w in names(worker_reports)){
    rep <- worker_reports[[w]]
    for(peer in names(rep))
      add(w, peer, "p2p", rep[[peer]], TRUE)
  }

  edges <- if(length(rows)) do.call(rbind, rows) else
    data.frame(from = character(), to = character(), kind = character(),
               reachable = logical(), required = logical(), stringsAsFactors = FALSE)
  failures <- edges[edges$required & !edges$reachable, , drop = FALSE]
  list(edges = edges, failures = failures, ok = nrow(failures) == 0L,
       local_workers = local_workers)
}

## Per-edge-kind remediation hint for a failed edge, what to fix by direction.
## The exact ports are in the port plan printed at launch, so the hint points
## there rather than repeating them on every failing edge.
reachability_remediation <- function(kind, from, to, local_workers = character(0)){
  switch(kind,
    control = sprintf("check start_worker on '%s', and run_workflow(ports=), see the port plan",
                      from),
    p2p = {
      base <- sprintf("open '%s' new_worker(ports=) to TCP from '%s', see the port plan", to, from)
      lw <- intersect(c(from, to), local_workers)
      if(length(lw)) sprintf("%s, or drop the local worker '%s'", base, lw[1]) else base
    },
    sprintf("check connectivity from '%s' to '%s'", from, to))
}

## Render the matrix for the user: every edge, then a FAILURES summary of who
## can't reach whom. Returns a character vector (one line each) for to_log().
format_reachability <- function(r){
  out <- "[flumen] reachability check - who reaches whom (from -> to):"
  if(nrow(r$edges))
    out <- c(out, vapply(seq_len(nrow(r$edges)), function(i){
      e <- r$edges[i, ]
      sprintf("  %-12s -> %-12s : %-11s (%s)", e$from, e$to,
              if(isTRUE(e$reachable)) "OK" else "UNREACHABLE", e$kind)
    }, character(1)))
  if(!r$ok){
    out <- c(out, "  FAILURES (who can't reach whom):")
    for(i in seq_len(nrow(r$failures))){
      f <- r$failures[i, ]
      out <- c(out,
               sprintf("    %s -> %s : UNREACHABLE (%s)", f$from, f$to, f$kind),
               paste0("      fix: ", reachability_remediation(f$kind, f$from, f$to, r$local_workers)))
    }
  }
  out
}

## ---- Connectivity-check phase orchestration (scheduler-side) ----------------
## These wire the pure logic above into the scheduler event loop. They are
## inert until the bring-up barrier sets state$phase <- "probing".

## Record a worker's peer-reachability report (named logical: peer ID -> bool).
sched_handle_reachability_report <- function(msg, state, recv_socket){
  sender  <- msg[[1]]$sender
  payload <- decode_message_data(msg[[1]]$data, msg[[1]]$data_type, msg[[1]]$is_arrow)
  state$reach_reports[[sender]] <- payload
  state$message_list <- update_message(state$message_list, state$socket_list, recv_socket)
  invisible(NULL)
}

## Decide who probes whom (pure). PROBERS are every worker with a scheduler control
## channel (`ctrl_worker_ids`), INCLUDING local/IPC ones, which dial out to pull
## data from remote peers and so need their outbound P2P reachability checked.
## TARGETS are workers exposing a TCP data-request endpoint (Host + Data_req_port),
## so a worker with no inbound endpoint is never a target but is still a prober: a
## dialer needs no Host of its own. Returns a named list, prober ID -> peers
## data.frame (Supervisor_ID, Host, Data_req_port), omitting probers with no peers
## to reach.
compute_probe_roster <- function(supervisor_info, ctrl_worker_ids){
  si <- supervisor_info
  if(is.null(si) || nrow(si) == 0L) return(list())
  targets <- si[!is.na(si$Data_req_port) & nzchar(as.character(si$Host)),
                c("Supervisor_ID", "Host", "Data_req_port"), drop = FALSE]
  if(nrow(targets) == 0L) return(list())
  roster <- list()
  for(me in as.character(ctrl_worker_ids)){
    peers <- targets[as.character(targets$Supervisor_ID) != me, , drop = FALSE]
    if(nrow(peers) > 0L) roster[[me]] <- peers
  }
  roster
}

## Send each prober its roster of peers (ID, Host, data_req_port) to probe, down
## its per-worker sock_ctrl. Probers = every worker with an open sock_ctrl_<id>
## (local workers included -- their channel is opened at bring-up). Returns the
## IDs actually sent a roster (those with >=1 peer) -- the set the probe phase
## waits for reports from.
send_probe_peers <- function(state){
  si <- state$supervisor_info
  if(is.null(si)) return(character(0))
  ctrl_ids <- as.character(si$Supervisor_ID)
  ctrl_ids <- ctrl_ids[vapply(ctrl_ids,
    function(id) !is.null(state$socket_list[[paste0("sock_ctrl_", id)]]), logical(1))]
  roster <- compute_probe_roster(si, ctrl_ids)
  sent <- character(0)
  for(me in names(roster)){
    d <- serialize_data(list(roster[[me]]), "R_object", FALSE)
    m <- new_message(sender = "central_scheduler", msg_type = "probe_peers",
                     data_type = unlist(d$data_type$data_struct), data = d$data,
                     is_arrow = d$data_type$is_arrow, file_name = d$file_name)
    sock <- state$socket_list[[paste0("sock_ctrl_", me)]]
    try(flumen_send_aio(sock, m$message, mode = "raw"), silent = TRUE)
    sent <- c(sent, me)
  }
  sent
}

## Schedule ready tasks now (shared by the all-local fast path and the
## post-probe path). Sets phase to "dispatch".
sched_dispatch_now <- function(state){
  state$phase <- "dispatch"
  sched_try_schedule(state)
  invisible(NULL)
}

## Called at the bring-up barrier (all workers ready). If there are <2 TCP
## endpoints there are no cross-host edges to check -> dispatch directly
## (all-local/IPC workflows take this path, unchanged). Otherwise enter the
## "probing" phase: send each worker its peer roster and arm the probe deadline.
sched_enter_probe_or_dispatch <- function(state){
  si  <- state$supervisor_info
  targets <- if(is.null(si)) NULL
             else si[!is.na(si$Data_req_port) & nzchar(as.character(si$Host)), , drop = FALSE]
  n_workers <- if(is.null(si)) 0L else nrow(si)
  ## Probe iff there is at least one addressable TCP peer AND at least one other
  ## worker to reach it from. A local/IPC worker counts as that other worker: it
  ## dials out to pull data, so its outbound edge to a remote peer must be
  ## checked. All-local/IPC workflows (no TCP targets) and single-worker
  ## workflows skip straight to dispatch.
  if(is.null(targets) || nrow(targets) == 0L || n_workers < 2L)
    return(sched_dispatch_now(state))

  ## The scheduler does NOT probe workers: it is never a P2P endpoint. Each
  ## worker (including any co-located local worker) probes its own peers and
  ## reports back -- that covers every worker<->worker edge.
  state$phase          <- "probing"
  state$probe_sent_to  <- send_probe_peers(state)

  ## How long to wait for the reachability reports before deciding on what has
  ## arrived. Expiring is NOT benign: a merely slow worker contributes no edges and
  ## reads as unreachable. Hence generous, covering the worker's own probe (one 5 s
  ## window for all its peers), the round trip, and however long that supervisor
  ## takes to reach the message under load. The healthy case ends when the last
  ## report is in, not at the deadline. NOT tied to startup_timeout even though the
  ## default matches: a launcher submitting to a batch queue raises that to hours,
  ## for time spent before registration.
  state$probe_deadline <- Sys.time() + local({
    v <- suppressWarnings(as.numeric(Sys.getenv("FLUMEN_PROBE_DEADLINE_SECS", "30")))
    if(!isTRUE(is.finite(v)) || v <= 0) 30 else v
  })
  invisible(NULL)
}

## Names of workers co-located with the scheduler (launcher == NULL). Used only
## to enrich the p2p remediation hint ("or run WITHOUT the local worker").
scheduler_local_workers <- function(state){
  wl <- state$exec_plan$worker_list
  nm <- vapply(wl, function(w) if(is.null(w$launcher)) (w$name %||% "") else "", character(1))
  nm[nzchar(nm)]
}

## Top-of-loop check (mirrors sched_check_startup_timeout): while "probing",
## wait for all expected reports OR the probe deadline, then build the matrix.
## On a required-edge failure: log it + tear down (exit 1). Otherwise dispatch.
sched_check_probe_phase <- function(state){
  if(!identical(state$phase, "probing")) return(invisible(NULL))
  all_in  <- length(setdiff(state$probe_sent_to, names(state$reach_reports))) == 0L
  expired <- !is.null(state$probe_deadline) && Sys.time() > state$probe_deadline
  if(!all_in && !expired) return(invisible(NULL))

  si <- state$supervisor_info
  r <- build_reachability(all_workers   = as.character(si$Supervisor_ID),
                          registered     = state$workers_ready,
                          worker_reports = state$reach_reports,
                          local_workers  = scheduler_local_workers(state))
  for(ln in format_reachability(r)) to_log(ln, "Central scheduler")

  if(!r$ok){
    record_error_notify(state, new_error_notify(
      supervisor_ID = "<connectivity>", task_id = "<connectivity>", exe_id = "<connectivity>",
      message = paste(format_reachability(r), collapse = "\n")))
    scheduler_teardown(state, 1)   ## signals flumen_scheduler_exit
  }
  sched_dispatch_now(state)
}

## The fault-detection thresholds a remote supervisor needs, collected on the
## MASTER so they can ride the handshake (see rw_config in R/scheduler_channels.R).
## Defined once, so the defaults live in exactly one place and cannot drift between
## the side that sends them and the side that falls back to them.
##
## FLOOR OF 5 BEATS on the silence thresholds. They are written in seconds but only
## mean anything as a multiple of the keepalive interval, so raising the interval
## without raising them would turn them into false alarms. The floor makes that
## misconfiguration impossible.
worker_thresholds <- function(){
  num <- function(name, default){
    v <- suppressWarnings(as.numeric(Sys.getenv(name, as.character(default))))
    if(!isTRUE(is.finite(v)) || v < 0) default else v
  }
  hb <- num("FLUMEN_HEARTBEAT_SECS", 20)
  if(hb <= 0) hb <- 20
  floor_silence <- function(v) if(v > 0 && v < 5 * hb) 5 * hb else v

  list(
    heartbeat_secs          = hb,
    ## 0 disables a check, and the floor must not resurrect it.
    scheduler_silence_secs  = floor_silence(num("FLUMEN_SCHEDULER_SILENCE_SECS", 600)),
    stalled_transfer_secs   = floor_silence(num("FLUMEN_STALLED_TRANSFER_SECS",  600)),
    stalled_transfer_checks = max(1L, as.integer(num("FLUMEN_STALLED_TRANSFER_CHECKS", 5))),
    dead_wrapper_secs       = num("FLUMEN_DEAD_WRAPPER_SECS", 30),
    dead_wrapper_checks     = max(1L, as.integer(num("FLUMEN_DEAD_WRAPPER_CHECKS", 5)))
  )
}

## Read one threshold on the supervisor side. `state$thresholds` is populated from
## the handshake; a master too old to send them leaves it NULL, and then this
## falls back to the local environment and finally to the same default.
worker_threshold <- function(state, key, env_name, default){
  v <- if(!is.null(state$thresholds)) state$thresholds[[key]] else NULL
  if(!is.null(v) && isTRUE(is.finite(v))) return(v)
  v <- suppressWarnings(as.numeric(Sys.getenv(env_name, as.character(default))))
  if(!isTRUE(is.finite(v)) || v < 0) default else v
}
