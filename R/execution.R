
#' Execute a Flumen workflow
#'
#' Runs a workflow across one or more workers, handling scheduling, data
#' transfer (Arrow IPC or qs2), and result collection. Workers can be local
#' (IPC) or remote (TCP).
#'
#' @param workflow A `workflow_object` from [new_workflow()]. A single task must
#'   be wrapped in a workflow first, e.g. `new_workflow(task_list = list(my_task))`.
#' @param worker_list List of worker objects from [new_worker()]. At least one
#'   worker is required.
#' @param chunk_size Numeric. Maximum size in bytes of a single transport chunk
#'   when splitting messages between processes. Defaults to `1e8` (100 MB).
#'   Lower it on memory-constrained workers.
#' @param master_host Optional character. IP or hostname of the master process as
#'   seen by remote workers. Auto-detected if `NULL`. Set it explicitly when the
#'   auto-detected address is not reachable from the remote workers, for instance
#'   on multi-interface or VPN hosts.
#' @param ports Integer vector. Contiguous TCP port range the master uses for its
#'   inbound listen sockets. `ports[1]` is the shared notify port and two further
#'   consecutive ports are taken per remote worker, so `n` remote workers need
#'   `1 + 2 * n` ports. Defaults to `61000:61099`. Local workers do not consume
#'   from this pool, and each worker's own peer-to-peer range is set separately
#'   with `new_worker(ports = )`. All values must lie in `[1024, 65535]`.
#'   `run_workflow()` errors if the pool is too small for the number of remote
#'   workers, or if it overlaps the `ports` range of a co-located worker.
#' @param log_dir Optional character. Directory where local subprocesses write
#'   their logs. If `NULL` (default), `log/` under the current working directory
#'   is used and created if needed. The resolved path is printed at workflow
#'   start. Remote workers do not inherit it: they write to `log/` under their
#'   own `root_path` on their own node.
#' @param startup_timeout Numeric. Seconds to wait for the scheduler handshake
#'   before giving up. Defaults to 30.
#' @param shared_path Character vector of shared-filesystem root paths, or
#'   `NULL` (default), meaning every file is transferred. A file whose resolved
#'   path lies at or under one of these roots is read from the shared storage by
#'   the consuming task and not transferred over the socket. Matching is lexical
#'   and applies on path-component boundaries, so a relative root never matches
#'   an absolute path. A root that is not visible on a worker raises a
#'   "declared file(s) not found" error naming `shared_path`.
#'
#' @return Invisibly returns `NULL`. `run_workflow()` blocks until the
#'   workflow completes; results are not returned to the calling session.
#'   They remain in the worker caches and in any declared output files.
#'
#' @seealso [new_workflow()], [new_worker()]
#'
#' @examples
#' \dontrun{
#' w <- new_worker(name = "local1", n_concurrent_task = 2)
#' res <- run_workflow(workflow, worker_list = list(w), chunk_size = 1e8)
#' }
#'
#' @export
run_workflow <- function(workflow,
                         worker_list = NULL,
                         chunk_size = 1e8,
                         master_host = NULL,
                         ports = 61000:61099,
                         log_dir = NULL,
                         startup_timeout = 30,
                         shared_path = NULL){

  ## A workflow is the only executable unit: it carries the DAG, the wildcard
  ## expansion and the build-time validation that execution relies on.
  if(!inherits(workflow, "workflow_object")){
    stop(flumen_reject(
      call  = "run_workflow()",
      error = sprintf("'workflow' must be a workflow_object, not '%s'",
                      paste(class(workflow), collapse = "/")),
      fix   = paste("workflow <- new_workflow(workflow_id = \"my_wf\",",
                    "task_list = list(my_task))")), call. = FALSE)
  }

  ## Early input validation: clearly invalid arguments.
  ## Only fails on obviously broken cases (empty worker_list, non-positive
  ## chunk_size, task pinned to a worker not present in worker_list).
  validate_exec_args(workflow, worker_list, chunk_size)

  ## run_workflow(shared_path=): declared shared-filesystem roots. A file parameter
  ## whose resolved path lies under one of these roots is read in place by the
  ## consumer (Model 3) instead of being shipped peer-to-peer (Model 2). It is a
  ## launch-time deployment assertion, the same workflow running with or without it,
  ## and is orthogonal to pre_staged(), which marks individual inputs. The match is
  ## lexical and frame-strict (is_under_shared_path()). The worker still verifies
  ## the file exists locally before the task runs (sup_handle_execute_task()).
  if (!is.null(shared_path)) {
    shared_path <- as.character(shared_path)
    shared_path <- shared_path[nzchar(shared_path)]
    if (length(shared_path) == 0L) {
      shared_path <- NULL
    } else {
      workflow$shared_path <- shared_path
      ## Observability: show the Model 2/3 classification so the user can verify
      ## the match did what they intended before the run starts.
      report_shared_path_classification(workflow$static_file_paths, shared_path)
    }
  }

  ## Set up the log directory for the in-process scheduler and the local supervisors
  ## and wrappers it spawns, and print it so the user knows where to look. Default:
  ## "log/" under the working directory. It must outlive the session: the documented
  ## way to run a long pipeline is `nohup Rscript ... &`, so tempdir() would not do.
  ##
  ## Precedence: an explicit log_dir wins, then an already-set FLUMEN_LOG_DIR,
  ## then "log/" under the cwd.
  ##
  ## The directory reaches subprocesses through FLUMEN_LOG_DIR, which
  ## sys::exec_background inherits automatically and to_log() reads. Remote workers
  ## do not inherit it: theirs is unset, so to_log() falls back to "log/" under the
  ## worker's root_path.
  if(is.null(log_dir)){
    env_log_dir <- Sys.getenv("FLUMEN_LOG_DIR", "")
    log_dir <- if(nzchar(env_log_dir)) env_log_dir else file.path(getwd(), "log")
  }
  if(!dir.exists(log_dir)) dir.create(log_dir, recursive = TRUE)
  log_dir <- normalizePath(log_dir, mustWork = FALSE)
  message(sprintf("[flumen] log directory: %s", log_dir))
  ## The task table is appended to, not rewritten, so a reader who opens it
  ## mid-run meets the same instance on several lines. The message states the
  ## reading rule and that the file is rewritten whole at teardown.
  message(sprintf(paste0(
    "[flumen] task table: %s\n",
    "         one line per state change, so a task's current state is its LAST line\n",
    "         (rewritten one-line-per-task when the run ends)"),
    FLUMEN_TASK_TABLE_FILE))
  .old_flumen_log_env <- Sys.getenv("FLUMEN_LOG_DIR", unset = NA_character_)
  Sys.setenv(FLUMEN_LOG_DIR = log_dir)
  on.exit({
    if(is.na(.old_flumen_log_env)) Sys.unsetenv("FLUMEN_LOG_DIR")
    else                            Sys.setenv(FLUMEN_LOG_DIR = .old_flumen_log_env)
  }, add = TRUE)

  worker_list <- setup_remote_worker_tcp(worker_list, master_host, ports)
  print_port_plan(worker_list)

  ## Refuse to start if another flumen instance already holds this run's master
  ## TCP ports (same / overlapping ports range). Fails fast BEFORE any ssh launch
  ## or socket bind, with an actionable message; non-overlapping ranges pass and
  ## independent runs coexist. No-op for all-local workflows.
  check_master_ports_free(worker_list)

  launch_remote_workers(worker_list)
  
  exec_plan <- new_exec_plan(workflow$DAG, worker_list)

  ## Run the central scheduler IN-PROCESS: run_central_scheduler() builds the
  ## scheduling state, launches the supervisors, and runs the event loop in this
  ## very R session, returning only when the workflow finishes, errors, or is
  ## interrupted (ESC / Ctrl-C). To launch a workflow without tying up the calling
  ## session, run the script in a background OS process (e.g. `nohup Rscript
  ## pipeline.R &`).
  res <- run_central_scheduler(workflow, exec_plan, chunk_size,
                               sleep_time      = 0.001,
                               startup_timeout = startup_timeout)

  ## User interrupt (ESC / Ctrl-C): the interrupt landed on the in-process event
  ## loop, scheduler_interrupt() tore the workers down. Report the abort cleanly.
  if(isTRUE(res$interrupted)){
    stop(structure(
      class = c("flumen_interrupted", "error", "condition"),
      list(message = flumen_block(
             "Flumen workflow interrupted.",
             list(error = "stopped before completion; the workers were terminated")),
           call    = NULL)))
  }
  ## A genuine workflow failure: a structured error was recorded, or the
  ## scheduler ended with a non-zero status. isTRUE() keeps the test NA-safe.
  if(!is.null(res$err_info) || isTRUE(res$status != 0)){
    stop(format_workflow_error(res$status, res$err_info), call. = FALSE)
  }

  message("[flumen] Pipeline finished successfully.")
  return(invisible(NULL))
}


## Pre-flight: refuse to start if another process already holds the master-side TCP
## ports this run needs, BEFORE any ssh launch or socket bind. Two flumen instances
## sharing a `ports` range collide here, while non-overlapping ranges let
## independent runs coexist. A no-op for all-local workflows (no remote worker, no
## master_url).
##
## Checked are the ports bound at scheduler or worker start-up, where a conflict is
## observable: the shared notify port, each remote worker's ctrl/term ports (parsed
## from its master_url, term = ctrl + 1), and in a mixed local+remote setup each
## co-located LOCAL worker's data-request port ports[1], bound as TCP on the master
## host so remote peers can fetch from it (launch_local_and_register_supervisors).
## The per-worker transfer pool (ports[2:N]) is bound on demand, so it cannot be
## probed ahead of time and is left to the runtime error path (supervisor_fatal ->
## error_notify).
##
## A free port is probe-bound then closed immediately, and the scheduler re-binds it
## moments later. The probe uses "pull" whatever the real protocol: any listener on
## a busy TCP port fails to bind.
check_master_ports_free <- function(worker_list){
  is_remote <- vapply(worker_list, function(w) !is.null(w$launcher), logical(1))
  if(!any(is_remote)) return(invisible(NULL))

  ports_to_check <- integer(0)
  ## remote workers: notify (shared) + this worker's ctrl + term (= ctrl + 1)
  for(w in worker_list[is_remote]){
    if(is.null(w$master_url)) next
    body <- sub("^tcp://", "", w$master_url)
    pp   <- strsplit(sub("^[^:]+:", "", body), ",", fixed = TRUE)[[1]]
    if(length(pp) < 2L) next
    notify_port <- as.integer(pp[1]); ctrl_port <- as.integer(pp[2])
    ports_to_check <- c(ports_to_check, notify_port, ctrl_port, ctrl_port + 1L)
  }
  ## co-located local workers (mixed setup): data-request listen = ports[1] (TCP)
  for(w in worker_list[!is_remote]){
    if(!is.null(w$self_host) && !is.null(w$ports))
      ports_to_check <- c(ports_to_check, as.integer(w$ports[1]))
  }
  ports_to_check <- sort(unique(ports_to_check[!is.na(ports_to_check)]))
  if(length(ports_to_check) == 0L) return(invisible(NULL))

  busy <- integer(0)
  for(p in ports_to_check){
    s <- tryCatch(nanonext::socket("pull", listen = paste0("tcp://0.0.0.0:", p)),
                  error = function(e) NULL, warning = function(w) NULL)
    if(is.null(s)) busy <- c(busy, p) else close(s)
  }

  if(length(busy)){
    stop(flumen_reject(
      call  = "run_workflow()",
      error = sprintf(paste("master TCP port(s) %s are already in use on this host.",
                            "Another flumen instance is probably running here, or a",
                            "previous run did not release them"),
                      paste(busy, collapse = ", ")),
      fix   = paste("stop the other instance, or launch with a different,",
                    "non-overlapping run_workflow(ports = ...) range")), call. = FALSE)
  }
  invisible(NULL)
}


## Auto-launch remote workers before the scheduler starts, in two phases.
##  Phase 1 (fire): launch_worker() for each worker. Sync methods (NULL, a plain
##    function, user S3 classes) run and return. Async ones (the built-in
##    ssh_launcher) return integer PIDs from sys::exec_background, so N SSH
##    connections proceed concurrently with no extra dependency.
##  Phase 2 (wait): sys::exec_status blocks on every PID collected and reports a
##    non-zero status with a launcher-specific message. With several failures the
##    first detected is reported.
launch_remote_workers <- function(worker_list) {
  launch_results <- vector("list", length(worker_list))
  for(i in seq_along(worker_list)){
    .w <- worker_list[[i]]
    launch_results[[i]] <- list(
      worker = .w,
      handle = launch_worker(.w$launcher, .w)
    )
  }
  for(lr in launch_results){
    h <- lr$handle
    if(is.numeric(h) && length(h) == 1L){
      status <- sys::exec_status(as.integer(h), wait = TRUE)
      ## On some platforms sys::exec_status returns a waitpid-shifted value (65280 for
      ## exit code 255). Shift right 8 bits when the status is above 255 and an exact
      ## multiple of 256, the shifted shape. Anything else is already an exit code and
      ## is left alone.
      if(is.numeric(status) && status > 255L && (status %% 256L) == 0L){
        status <- as.integer(bitwShiftR(status, 8L))
      }
      if(!identical(status, 0L)){
        .w  <- lr$worker
        stop(launcher_error_message(.w$launcher, .w, status), call. = FALSE)
      }
    }
  }
  invisible(NULL)
}


## Build the user-facing error message for a failed scheduler exit.
## Prefers the structured err_info (task_id, exe_id, worker, message,
## status, stderr) when present; falls back to a generic status-only
## message otherwise.
format_workflow_error <- function(status, err_info) {
  if(is.null(err_info)){
    return(flumen_fail(
      error = sprintf("the scheduler exited with status %d and reported no task-level detail",
                      status),
      fix   = "read the scheduler log in log/"))
  }
  f <- function(x, default = "<unknown>") {
    if(is.null(x) || length(x) == 0L) return(default)
    if(length(x) == 1L && is.na(x)) return(default)
    paste(as.character(x), collapse = "\n")
  }
  ## Wildcard binding of the failing instance.
  wc_txt <- NULL
  if(length(err_info$wildcards)){
    wc <- err_info$wildcards
    wc_txt <- paste(sprintf("%s = %s", names(wc),
                            vapply(wc, as.character, character(1))),
                    collapse = ", ")
  }
  ## task_id can arrive replicated (one entry per aggregated input of a gather
  ## task); collapse to the distinct name(s) so the field stays a scalar. An
  ## absent binding or status drops out of the block on its own: flumen_block()
  ## prints only the fields that carry something.
  msg_txt <- f(err_info$message, "<no message>")
  ## A worker message is one sentence and is wrapped like any other field. The
  ## startup-timeout report is not: it carries the reachability matrix, whose
  ## columns a reflow would destroy. Multi-line means "this text brought its own
  ## layout", so it is left exactly as it arrived.
  nowrap  <- c("stderr", if(grepl("\n", msg_txt, fixed = TRUE)) "error")
  fields  <- list(task    = f(unique(err_info$task_id)),
                  exe_id  = f(err_info$exe_id),
                  binding = wc_txt,
                  worker  = f(err_info$Supervisor_ID),
                  error   = msg_txt,
                  status  = if(!is.null(err_info$status) && !is.na(err_info$status))
                              as.character(err_info$status))
  base <- flumen_block("Flumen workflow failed.", fields, nowrap = nowrap)
  stderr_str  <- f(err_info$stderr, "")
  if(nchar(stderr_str) > 0L){
    ## R's stop() truncates the printed condition message to getOption("warning.length")
    ## (default 1000 bytes), keeping the HEAD. Keep the limit, but trim an overflowing
    ## stderr to its TAIL, where the actionable error lives, and say that it was
    ## truncated and where to read it in full: the scheduler (sched_handle_error) and
    ## the worker both log the complete text.
    orig_n <- nchar(stderr_str)
    wl     <- getOption("warning.length", 1000L)
    budget <- wl - nchar(base) - 160L   # reserve for "  stderr  : " + the marker
    if(budget > 80L && orig_n > budget){
      logdir <- Sys.getenv("FLUMEN_LOG_DIR", "")
      where  <- if(nzchar(logdir)) paste0("full stderr in ", logdir, "/",
                                          log_file_name("Central scheduler"))
                else "full stderr in the scheduler and worker logs"
      stderr_str <- paste0(
        "[truncated: showing last ", budget, " of ", orig_n, " bytes -- ", where, "]\n...",
        substr(stderr_str, orig_n - budget + 1L, orig_n))
    }
    fields$stderr <- stderr_str
    base <- flumen_block("Flumen workflow failed.", fields, nowrap = nowrap)
  }
  base
}


## Format a resources/requires list for human-readable error messages.
## Example: list(cpu = 16, mem = "64G", gpu = 1) -> "cpu=16, mem=64G, gpu=1"
format_resources <- function(resources) {
  if (is.null(resources) || length(resources) == 0) return("<none>")
  paste(sprintf("%s=%s", names(resources),
                vapply(resources, function(v) paste(as.character(v), collapse = "+"), character(1))),
        collapse = ", ")
}


## Argument validation for run_workflow.
## Raises stop() only for clearly broken cases, so that a workable declaration is
## never refused. Three checks:
##   (1) missing or empty worker_list
##   (2) non-numeric or non-positive chunk_size
##   (3) task$requires declares resources that no worker in worker_list can satisfy
validate_exec_args <- function(workflow, worker_list, chunk_size) {

  ## A worker_object is itself a list, so a forgotten list()
  ## (worker_list = my_worker) has non-zero length and reaches the checks below.
  if(inherits(worker_list, "worker_object")){
    stop(flumen_reject(
      call  = "run_workflow()",
      error = "'worker_list' must be a list of workers, not a single worker",
      fix   = "worker_list = list(my_worker)"), call. = FALSE)
  }

  if(is.null(worker_list) || length(worker_list) == 0){
    stop(flumen_reject(
      call  = "run_workflow()",
      error = "'worker_list' must contain at least one worker",
      fix   = "worker_list = list(new_worker(...))"), call. = FALSE)
  }

  not_worker <- !vapply(worker_list, function(w) inherits(w, "worker_object"), logical(1))
  if(any(not_worker)){
    stop(flumen_reject(
      call  = "run_workflow()",
      error = sprintf("'worker_list' element(s) %s have class %s",
                      paste(which(not_worker), collapse = ", "),
                      paste(shQuote(vapply(worker_list[not_worker],
                                           function(x) paste(class(x), collapse = "/"),
                                           character(1))), collapse = ", ")),
      fix   = "'worker_list' must contain only workers built with new_worker()"),
      call. = FALSE)
  }

  ## Windows runs the scheduler, not the workers. A LOCAL worker would need two
  ## things the platform does not give: NNG maps ipc:// onto a named pipe, whose
  ## name cannot carry a drive-lettered temporary path, and a stock Windows has
  ## no POSIX shell for new_shell_task(). A workflow whose workers all carry a
  ## launcher never opens an IPC socket (sock_res, sock_ctrl and sock_term are
  ## all TCP once any worker is remote), so that configuration is unaffected.
  if(.Platform$OS.type == "windows"){
    local_names <- windows_local_workers(worker_list)
    if(length(local_names) > 0){
      stop(flumen_reject(
        call  = "run_workflow()",
        error = sprintf(paste("worker(s) %s have no launcher, so the scheduler would run them",
                              "on this machine; local workers are not supported on Windows"),
                        paste(shQuote(local_names), collapse = ", ")),
        fix   = paste("run the workers on a POSIX host: give each one a launcher,",
                      "new_worker(launcher = ssh_launcher(host_user = \"me@node\")), or start",
                      "it by hand with start_worker() and declare it with a launcher whose",
                      "method does nothing. The scheduler itself runs on Windows")),
        call. = FALSE)
    }
  }

  ## The scheduler identifies each worker by name (supervisor_info$Supervisor_ID),
  ## so two workers sharing a name collide in its tables.
  w_names <- vapply(worker_list, function(w) as.character(w$name), character(1))
  dup_names <- unique(w_names[duplicated(w_names)])
  if(length(dup_names) > 0){
    stop(flumen_reject(
      call  = "run_workflow()",
      error = sprintf("duplicate worker name(s): %s",
                      paste(shQuote(dup_names), collapse = ", ")),
      fix   = paste("give each worker a unique name; a manually started worker must",
                    "pass the same value as start_worker(worker_id = )")), call. = FALSE)
  }

  if(is.null(chunk_size) || !is.numeric(chunk_size) ||
     length(chunk_size) != 1 || is.na(chunk_size) || chunk_size <= 0){
    stop(flumen_reject(
      call  = "run_workflow()",
      error = "'chunk_size' must be a single positive number (bytes)",
      fix   = "run_workflow(chunk_size = 1e6)"), call. = FALSE)
  }

  ## In mixed local+remote setups, local workers inherit self_host from
  ## master_host (injected in run_workflow before this function runs).
  ## new_worker() rejects self_host on local workers upstream, so there is
  ## nothing to validate here.

  for(task in workflow$task_list){
    if(!is.null(task$requires)){
      compatible <- vapply(worker_list, function(w) {
        worker_satisfies_requires(w, task$requires)
      }, logical(1))
      if(!any(compatible)){
        stop(flumen_reject(
          call  = "run_workflow()",
          task  = task$task_id,
          error = sprintf(paste("requires resources that no worker in worker_list can",
                                "satisfy: %s"), format_resources(task$requires)),
          fix   = paste("add a worker that declares matching or greater resources, on",
                        "new_worker() for a local worker or on its launcher for a remote",
                        "one, or relax the requirement")), call. = FALSE)
      }
    }
  }

  invisible(TRUE)
}

## Worker-initiated TCP setup and port-plan validation.
##   - One SHARED notify port (`ports[1]`) on the master: every remote supervisor
##     PUSH-dials it for results, READY and p2p_transfer_done. Push/pull fans in
##     many-to-one over IPC and TCP, and the envelope's `sender` tells them apart.
##   - Each remote worker (non-NULL launcher) takes two consecutive ports from the
##     pool, CONTROL dispatch and TERMINATE. `master_url =
##     "tcp://<master_host>:<notify_port>,<ctrl_port>"` carries both, parsed in
##     init_sockets(). The terminate port is ctrl_port + 1, derived by the worker.
##   - Each local worker (NULL launcher), in a setup with at least one remote
##     worker, gets `self_host <- master_host` injected so cross-machine
##     peer-to-peer transfers reach it. Its own P2P range is `worker$ports`
##     (new_worker(), default 61100:61199).
##   - No two listen ranges on the same host may overlap: the master's per-remote
##     pool against co-located workers' `ports`, and those against each other.
##     Checked at run_workflow() time. Remote workers with self_host = NULL cannot
##     be host-grouped, so two of them on one host with overlapping `ports` go
##     uncaught: set self_host explicitly.
##   - All-local workflow: nothing is allocated, local workers reach peers over IPC.
setup_remote_worker_tcp <- function(worker_list, master_host, ports) {
  is_remote_w <- vapply(worker_list, function(w) !is.null(w$launcher), logical(1))
  if (!any(is_remote_w)) return(worker_list)

  if (!is.numeric(ports) || length(ports) < 2L || any(is.na(ports)) ||
      any(ports != as.integer(ports)) || any(ports < 1024L) || any(ports > 65535L) ||
      any(diff(as.integer(ports)) != 1L)) {
    stop(flumen_reject(
      call  = "run_workflow()",
      error = paste("'ports' must be a contiguous integer range of length >= 2 with all",
                    "values in [1024, 65535]"),
      fix   = "run_workflow(ports = 61000:61099)"), call. = FALSE)
  }
  ports <- as.integer(ports)

  if (is.null(master_host)) master_host <- detect_self_host()

  remote_idx <- which(is_remote_w)
  n_remote   <- length(remote_idx)
  ## Port layout: 1 shared notify (ports[1]) + 2 per remote worker (ctrl + term),
  ## so 1 + 2*N in total.
  n_ports_needed <- 1L + 2L * n_remote
  if (length(ports) < n_ports_needed) {
    stop(flumen_reject(
      call  = "run_workflow()",
      error = sprintf(paste("'ports' has %d ports but %d remote worker(s) need %d",
                            "(1 shared notify + 2 per worker, control and terminate)"),
                      length(ports), n_remote, n_ports_needed),
      fix   = "widen the range, run_workflow(ports = 61000:61099)"), call. = FALSE)
  }
  master_notify_port <- ports[1L]
  master_used <- ports[seq_len(n_ports_needed)]
  for (j in seq_len(n_remote)) {
    ctrl_port <- ports[2L + 2L * (j - 1L)]   ## ports[2], ports[4], ports[6], ...
    ## master_url carries both shared notify port and this worker's ctrl port.
    ## Terminate port is derived by the worker as ctrl_port + 1.
    worker_list[[remote_idx[j]]]$master_url <- paste0(
      "tcp://", master_host, ":", master_notify_port, ",", ctrl_port
    )
  }

  local_idx <- which(!is_remote_w)
  for (j in seq_along(local_idx)) {
    worker_list[[local_idx[j]]]$self_host <- master_host
  }

  ## Overlap validation: group listen ranges by host, error on any overlap.
  ranges_by_host <- list()
  add_range <- function(host, lo, hi, label) {
    if (is.null(host) || !nzchar(host)) return(invisible(NULL))
    ranges_by_host[[host]] <<- c(ranges_by_host[[host]],
                                 list(list(lo = lo, hi = hi, label = label)))
  }
  add_range(master_host, min(master_used), max(master_used),
            sprintf("master listen pool for %d remote worker(s)", n_remote))
  for (w in worker_list) {
    whost <- if (is.null(w$launcher)) master_host else w$self_host
    add_range(whost, min(w$ports), max(w$ports), sprintf("worker '%s' ports", w$name))
  }
  for (host in names(ranges_by_host)) {
    rs <- ranges_by_host[[host]]
    if (length(rs) < 2L) next
    rs <- rs[order(vapply(rs, `[[`, numeric(1), "lo"))]
    for (k in 2:length(rs)) {
      if (rs[[k]]$lo <= rs[[k - 1L]]$hi) {
        stop(flumen_reject(
          call  = "run_workflow()",
          host  = host,
          error = sprintf("overlapping TCP port ranges: '%s' %d..%d and '%s' %d..%d",
                          rs[[k - 1L]]$label, rs[[k - 1L]]$lo, rs[[k - 1L]]$hi,
                          rs[[k]]$label, rs[[k]]$lo, rs[[k]]$hi),
          fix   = paste("give them non-overlapping ranges, in new_worker(ports = ...)",
                        "or in the run_workflow(ports = ...) pool")), call. = FALSE)
      }
    }
  }

  worker_list
}

## The TCP port plan, printed at workflow start so the firewall can be checked
## before anything launches. Sources are IP addresses, not worker names, so a line
## can be pasted straight into a firewall command. The worker's name sits in the
## host header instead.
##
## Above PORT_PLAN_MAX_HOSTS workers the per-host list would be one block per
## machine naming every other machine as a source, so it collapses to the master
## plus one representative worker. The collapse applies only when every worker
## listens on the SAME range. With differing ranges they are grouped by range.
PORT_PLAN_MAX_HOSTS <- 4L

print_port_plan <- function(worker_list) {
  is_remote_w <- vapply(worker_list, function(w) !is.null(w$launcher), logical(1))

  if (!any(is_remote_w)) {
    message("[flumen] port plan: all-local workflow -- IPC sockets only, no TCP ports")
    return(invisible(NULL))
  }

  remote_idx <- which(is_remote_w)

  ## Parse the two ports out of each remote worker's master_url:
  ## "tcp://host:notify,ctrl" (notify shared across remotes, ctrl per worker).
  parse_url <- function(u) {
    body <- sub("^tcp://", "", u)
    host <- sub(":.*$", "", body)
    pp   <- strsplit(sub("^[^:]+:", "", body), ",", fixed = TRUE)[[1]]
    list(host = host, notify = as.integer(pp[1]), ctrl = as.integer(pp[2]))
  }
  parsed       <- lapply(worker_list[remote_idx], function(w) parse_url(w$master_url))
  master_hosts <- vapply(parsed, `[[`, character(1), "host")
  master_h     <- unique(master_hosts)
  remote_hosts <- vapply(worker_list[remote_idx],
                         function(w) w$self_host %||% "<address known at worker start-up>",
                         character(1))
  remote_names <- vapply(worker_list[remote_idx], function(w) w$name, character(1))

  local_ws  <- worker_list[!is_remote_w &
                 vapply(worker_list, function(w) !is.null(w$self_host), logical(1))]
  lw_names  <- vapply(local_ws, function(w) w$name, character(1))
  has_local <- length(lw_names) > 0L

  ## A host's rule uses ITS OWN listen range: a peer must reach the range that
  ## host listens on, never a union across hosts. data_req = ports[1],
  ## pool = ports[2..N].
  rng_of <- function(w) {
    lo <- as.integer(w$ports[1L]); hi <- as.integer(w$ports[length(w$ports)])
    if (hi > lo) sprintf("%d-%d", lo, hi) else as.character(lo)
  }
  local_rng <- if (has_local) {
    llo <- min(vapply(local_ws, function(w) as.integer(w$ports[1L]), integer(1)))
    lhi <- max(vapply(local_ws, function(w) as.integer(w$ports[length(w$ports)]), integer(1)))
    if (lhi > llo) sprintf("%d-%d", llo, lhi) else as.character(llo)
  } else NA_character_
  join <- function(x) paste(unique(x), collapse = ", ")
  rule <- function(range, from, what)
    sprintf("       %-14s from %-38s %s", range, from, what)

  n_remote <- length(remote_idx)
  compact  <- n_remote > PORT_PLAN_MAX_HOSTS
  rngs     <- vapply(worker_list[remote_idx], rng_of, character(1))

  lines <- c("[flumen] port plan -- inbound TCP each host must accept", "")

  ## ---- master host(s): control from its workers, plus P2P if a local worker runs here
  for (mh in master_h) {
    sel  <- which(master_hosts == mh)
    cps  <- unlist(lapply(sel, function(j) c(parsed[[j]]$notify, parsed[[j]]$ctrl,
                                             parsed[[j]]$ctrl + 1L)))
    crng <- if (max(cps) > min(cps)) sprintf("%d-%d", min(cps), max(cps)) else as.character(min(cps))
    lab  <- if (has_local)
              sprintf("master (scheduler) + worker(s) %s",
                      paste(sprintf("'%s'", lw_names), collapse = ", "))
            else "master (scheduler)"
    src  <- if (compact) "every worker" else join(remote_hosts[sel])
    lines <- c(lines, sprintf("  %-16s %s", mh, lab),
               rule(crng, src, "control: dispatch, results, terminate"))
    if (has_local)
      lines <- c(lines, rule(local_rng, src, "data transfer"))
    lines <- c(lines, "")
  }

  ## ---- worker hosts
  if (!compact) {
    for (k in seq_along(remote_idx)) {
      srcs <- setdiff(c(remote_hosts[-k], if (has_local) master_h else character(0)),
                      remote_hosts[k])
      if (length(srcs) == 0L) next   ## one remote, no local worker -> no inbound P2P
      lines <- c(lines,
                 sprintf("  %-16s worker '%s'", remote_hosts[k], remote_names[k]),
                 rule(rngs[k], join(srcs), "data transfer"), "")
    }
  } else {
    peers <- if (has_local) "the master and every other worker" else "every other worker"
    for (r in unique(rngs)) {
      grp <- which(rngs == r)
      lead <- sprintf("  each of the %d worker(s) on %s, e.g. %s (worker '%s')",
                      length(grp), r, remote_hosts[grp[1]], remote_names[grp[1]])
      lines <- c(lines, lead, rule(r, peers, "data transfer"), "")
    }
  }

  ## ---- what is not a per-host rule: port assignment, SSH, one translation
  ctrl_detail <- if (compact)
      sprintf("  control ports: notify %d shared, then two consecutive per worker from %d",
              parsed[[1]]$notify, parsed[[1]]$notify + 1L)
    else
      sprintf("  control ports: notify %d shared; %s", parsed[[1]]$notify,
              paste(vapply(seq_along(parsed), function(j)
                sprintf("%s %d-%d", remote_names[j], parsed[[j]]$ctrl, parsed[[j]]$ctrl + 1L),
                character(1)), collapse = ", "))
  lines <- c(lines, ctrl_detail)

  is_ssh <- vapply(worker_list[remote_idx],
                   function(w) inherits(w$launcher, "ssh_launcher"), logical(1))
  if (any(is_ssh)) {
    ssh_ports <- unique(vapply(worker_list[remote_idx][is_ssh],
                    function(w) as.integer(w$launcher$host_port %||% 22L), integer(1)))
    tgt <- if (compact) "every worker" else join(remote_hosts[is_ssh])
    lines <- c(lines, sprintf(
      "  also needed, not a flumen port: SSH from the master to %s (port %s)",
      tgt, paste(ssh_ports, collapse = ",")))
  }

  ## One translation, not a dump per firewall. macOS differs in kind rather than
  ## in syntax, so it is named only when the master actually runs there.
  ex_ctrl <- { sel <- which(master_hosts == master_h[1])
               cps <- unlist(lapply(sel, function(j) c(parsed[[j]]$notify, parsed[[j]]$ctrl,
                                                       parsed[[j]]$ctrl + 1L)))
               if (max(cps) > min(cps)) sprintf("%d:%d", min(cps), max(cps))
               else as.character(min(cps)) }
  lines <- c(lines, sprintf("  example rule:  ufw allow from %s to any port %s proto tcp",
                            remote_hosts[1], ex_ctrl))
  if (identical(tolower(Sys.info()[["sysname"]]), "darwin"))
    lines <- c(lines,
      "  on macOS the built-in firewall filters by application, not by port: allow",
      "  incoming connections for the R executable instead of opening a range")

  message(paste(lines, collapse = "\n"))
  invisible(NULL)
}

