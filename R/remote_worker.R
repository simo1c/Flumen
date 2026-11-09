#' Start a Flumen worker on this machine
#'
#' Launches a supervisor process that connects outbound to the central
#' scheduler at `master_url` and waits for tasks. Blocks until the
#' scheduler triggers termination (end of workflow or error).
#'
#' @param master_url Character. URL of the scheduler's listen endpoints for this
#'   worker, in the form `"tcp://<master_host>:<notify_port>,<ctrl_port>"`. Both
#'   ports are allocated by the scheduler and printed in the port plan at
#'   workflow start-up. A single port is rejected.
#' @param worker_id Character. Unique identifier matching the `name` of
#'   the corresponding `new_worker()` declaration in the workflow, sent
#'   to the master in the REGISTER message so the master can identify
#'   this worker.
#' @param self_host Optional character. Override the auto-detected
#'   IP/hostname of this machine, used by other workers for peer-to-peer
#'   data transfer. Auto-detected via `detect_self_host()` if `NULL`.
#'   Specify explicitly on multi-interface hosts.
#'
#' @details
#' Run this function on the worker machine. In auto-launched mode (e.g.,
#' via [ssh_launcher()]), [run_workflow()] invokes it remotely with the
#' correct arguments. When the worker's launcher does not start it (a
#' launcher whose [launch_worker()] method does nothing), the user runs
#' this explicitly on the worker host before or shortly after starting the
#' workflow on the master side.
#'
#' All other worker parameters (concurrency, cache, root_path) are received
#' from the scheduler via the handshake message after REGISTER.
#'
#' @examples
#' \dontrun{
#' # Manual start on a worker machine. The notify and ctrl ports are
#' # printed by run_workflow() at start-up (see "[flumen] port plan").
#' # The session must run in the worker's root_path, as ssh_launcher()
#' # arranges with `cd`: started elsewhere, files are placed inconsistently.
#' library(flumen)
#' setwd("/data/flumen_run")   # the worker's root_path
#' start_worker(master_url = "tcp://192.168.1.10:61000,61001",
#'              worker_id  = "win_w1")
#'
#' # With explicit self_host (multi-interface override)
#' start_worker(master_url = "tcp://login01.cluster.edu:61000,61003",
#'              worker_id  = "compute_w1",
#'              self_host  = "10.0.100.5")
#' }
#'
#' @return Called for its side effect. Blocks until the supervisor process
#'   exits, then returns that process's exit status as an integer, `0` after a
#'   clean shutdown.
#'
#' @export
start_worker <- function(master_url, worker_id, self_host = NULL){

  if(missing(master_url) || !is.character(master_url) ||
     length(master_url) != 1 || !nzchar(master_url)){
    stop(flumen_reject(
      call  = "start_worker()",
      error = "'master_url' is required",
      fix   = "start_worker(master_url = \"tcp://host:61000,61001\", worker_id = \"w1\")"),
      call. = FALSE)
  }
  if(missing(worker_id) || !is.character(worker_id) ||
     length(worker_id) != 1 || !nzchar(worker_id)){
    stop(flumen_reject(
      call  = "start_worker()",
      error = "'worker_id' is required and must match the worker name in the workflow",
      fix   = "start_worker(master_url = \"tcp://host:61000,61001\", worker_id = \"w1\")"),
      call. = FALSE)
  }
  if(is.null(self_host)){
    self_host <- detect_self_host()
  }

  message("==============================================================")
  message("  Flumen worker")
  message("==============================================================")
  message("  Host       : ", Sys.info()[["nodename"]])
  message("  Self host  : ", self_host, "  (override via self_host arg)")
  message("  Directory  : ", getwd())
  message("  Worker ID  : ", worker_id)
  message("  Master URL : ", master_url)
  message("--------------------------------------------------------------")
  message("  Connecting to scheduler...")
  message("==============================================================")

  supervisor_script <- system.file("scripts", "supervisor.R", package = "flumen")
  if(!nzchar(supervisor_script)){
    stop(flumen_reject(
      call  = "start_worker()",
      error = "supervisor.R not found in the installed package",
      fix   = "reinstall flumen"), call. = FALSE)
  }

  sys::exec_wait(
    rscript_cmd(),
    args = c(
      supervisor_script,
      build_supervisor_args(worker_id  = worker_id,
                            master_url = master_url,
                            self_host  = self_host)
    )
  )
}

## Build the positional argv vector consumed by inst/scripts/supervisor.R.
## supervisor.R reads `args[1]..args[18]` by index; using a named builder
## here documents the slot meaning and isolates the positional coupling
## to a single place. Any reorder of supervisor.R's argv schema must
## update this function.
build_supervisor_args <- function(worker_id,
                                  master_url = NULL,
                                  self_host  = NULL) {
  argv <- rep("NULL", 18L)
  argv[1]  <- worker_id                                                 ## arg1
  argv[17] <- if(is.null(master_url)) "NULL" else master_url            ## arg17
  argv[18] <- if(is.null(self_host))  "NULL" else self_host             ## arg18
  argv
}
