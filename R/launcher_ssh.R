#' SSH launcher configuration
#'
#' Builds an S3 configuration object describing how to bootstrap a remote
#' worker via SSH. Pass the result to `new_worker()` as the `launcher`
#' argument; [run_workflow()] then dispatches to the matching
#' [launch_worker()] method before starting the scheduler.
#'
#' @param host_user Character. The SSH target as `user@host` (required).
#' @param host_port Integer. SSH port. Defaults to `22`.
#' @param identity Optional character. Path to the SSH private key file
#'   (e.g., `"~/.ssh/id_ed25519"`). `NULL` uses the default SSH identity
#'   resolution (ssh-agent, then default key files).
#' @param cwd Optional character. Working directory on the remote host for the
#'   worker R session. If `NULL` (default), it falls back to
#'   `worker$root_path`, where received files are written. If both are `NULL`
#'   the worker starts in the SSH login directory, and relative paths in tasks
#'   resolve against that.
#' @param R_cmd Character. `Rscript` executable path on the remote host.
#'   Defaults to `"Rscript"` (must be on `PATH`). Set explicitly if R is
#'   installed in a non-standard location.
#' @param resources Optional named list declaring the resources provided by the
#'   workers this launcher starts, matched against a task's `requires` (see
#'   [new_R_task()]). Optional fields `cpu` (non-negative integer), `mem` (size
#'   with unit, for example `"32G"`), `gpu` (non-negative integer) and `tags`
#'   (character vector of capability labels). With `NULL` (default) these
#'   workers accept only tasks with no `requires` constraint.
#'
#' @return An S3 object of class `"ssh_launcher"` with a `print()`
#'   method. When passed to `new_worker(..., launcher = ...)`, the
#'   configuration is carried along with the worker and consumed by
#'   [launch_worker()] at the start of [run_workflow()].
#'
#' @details
#' The actual SSH command executed by the associated [launch_worker()]
#' method is roughly:
#' \preformatted{
#' ssh -f -n [-i <identity>] [-p <host_port>] \
#'     -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
#'     <host_user> \
#'     '[cd <cwd or worker$root_path> &&] nohup <R_cmd> \
#'         -e "flumen::start_worker(master_url=\"...\", worker_id=\"...\")" \
#'         > /tmp/flumen_worker_<worker$name>.log 2>&1 < /dev/null & disown'
#' }
#'
#' Requirements on the remote server: Unix-like (Linux/macOS/BSD/WSL),
#' passwordless SSH (key-based), R + flumen package installed, and the
#' `Rscript` binary reachable on `PATH` (or specified via `R_cmd`).
#'
#' @seealso [launch_worker()], [new_worker()], [start_worker()]
#'
#' @examples
#' \dontrun{
#' # Simple case
#' new_worker(
#'   name      = "remote1",
#'   root_path = "/home/me/project",
#'   launcher  = ssh_launcher(host_user = "me@192.168.1.50")
#' )
#'
#' # Non-default SSH port + explicit key + override remote cwd
#' new_worker(
#'   name      = "hpc1",
#'   launcher  = ssh_launcher(
#'     host_user = "me@hpc.example.edu",
#'     host_port = 2222,
#'     identity  = "~/.ssh/hpc_key",
#'     cwd       = "/scratch/me/job"
#'   )
#' )
#' }
#'
#' @export
ssh_launcher <- function(host_user,
                         host_port = 22L,
                         identity  = NULL,
                         cwd       = NULL,
                         R_cmd     = "Rscript",
                         resources = NULL){
  if(missing(host_user) || is.null(host_user) || !nzchar(host_user)){
    stop(flumen_reject(
      call  = "ssh_launcher()",
      error = "'host_user' is required",
      fix   = "ssh_launcher(host_user = \"user@node01\")"), call. = FALSE)
  }
  validate_resources(resources, context = "ssh_launcher() 'resources'")
  structure(
    list(
      host_user = host_user,
      host_port = as.integer(host_port),
      identity  = identity,
      cwd       = cwd,
      R_cmd     = R_cmd,
      resources = resources
    ),
    class = "ssh_launcher"
  )
}

#' @export
print.ssh_launcher <- function(x, ...){
  cat("<SSH launcher>\n")
  cat("  host_user :", x$host_user, "\n")
  cat("  host_port :", x$host_port, "\n")
  cat("  identity  :", x$identity %||% "<default>", "\n")
  cat("  cwd       :", x$cwd %||% "<worker$root_path>", "\n")
  cat("  R_cmd     :", x$R_cmd, "\n")
  cat_launcher_resources(x$resources)
  invisible(x)
}

## Pre-flight: check that the remote working directory exists, synchronously,
## before the fire-and-forget launch. The launch command is `cd <cwd> && nohup
## Rscript ...`, so a missing <cwd> short-circuits the `&&`, the worker never
## starts, and the only symptom is the generic "worker did not connect within
## <startup_timeout>s", which blames the network instead of a stale or mistyped
## root_path or launcher cwd. Returns NULL on success, or a character error
## message on failure.
probe_remote_dir <- function(config, worker, remote_dir){
  ssh_args <- ssh_argv(config,
                       sprintf("test -d %s", shQuote(remote_dir)),
                       connect_timeout = 10)

  probe <- tryCatch(
    sys::exec_internal("ssh", args = ssh_args, error = FALSE, timeout = 20),
    error = function(e) list(status = 255L, stdout = raw(0),
                             stderr = charToRaw(conditionMessage(e)))
  )
  if(identical(as.integer(probe$status), 0L)) return(NULL)

  stderr_txt <- tryCatch(trimws(rawToChar(probe$stderr)), error = function(e) "")
  said <- if(nzchar(stderr_txt)) paste0(" Remote/ssh said: ", stderr_txt, ".") else ""
  qdir <- shQuote(remote_dir)
  if(identical(as.integer(probe$status), 255L)){
    ## ssh's own failure code: connection / auth, not the remote command.
    flumen_launch_fail(
      worker = worker$name,
      target = sprintf("%s, port %d", config$host_user, config$host_port),
      error  = paste0("cannot reach the host over SSH.", said),
      fix    = sprintf(paste("check passwordless SSH and that the host is reachable;",
                             "verify with: ssh %s echo ok"), config$host_user))
  } else {
    flumen_launch_fail(
      worker = worker$name,
      error  = sprintf(paste("the remote working directory %s does not exist on %s;",
                             "this is the launcher 'cwd', falling back to the worker",
                             "'root_path'.%s"), qdir, config$host_user, said),
      fix    = sprintf(paste("create it on the remote host, or pass a valid path;",
                             "verify with: ssh %s ls -ld %s"), config$host_user, qdir))
  }
}

#' @export
#' @noRd
launch_worker.ssh_launcher <- function(config, worker){

  if(is.null(worker$master_url)){
    stop(flumen_launch_fail(
      call   = "launch_worker.ssh_launcher()",
      worker = worker$name %||% "<unnamed>",
      error  = "the worker has no master_url, which run_workflow() allocates before launch",
      fix    = paste("launch workers through run_workflow(), not by calling",
                     "launch_worker() directly")), call. = FALSE)
  }

  ## Prepend `cd <cwd> &&` so the remote worker starts in the project directory on
  ## the server: a non-interactive SSH otherwise lands in the user's home and ops
  ## with relative paths fail. Default cwd is worker$root_path. The pre-flight
  ## directory check runs before this, for the reason stated there.
  effective_cwd <- config$cwd %||% worker$root_path
  if(!is.null(effective_cwd)){
    err <- probe_remote_dir(config, worker, effective_cwd)
    if(!is.null(err)) stop(err, call. = FALSE)
  }
  cd_prefix <- if(!is.null(effective_cwd)){
    sprintf("cd %s && ", shQuote(effective_cwd))
  } else {
    ""
  }

  ## Worker-initiated dial: pass master_url and worker_id, plus worker$self_host
  ## when set, to the remote start_worker(). Forwarding self_host makes the worker
  ## advertise a master-reachable address. Left unset, it auto-detects, which on
  ## cluster nodes typically yields a hostname the master cannot resolve. Detach
  ## pattern: nohup + & + disown.
  self_host_arg <- if(!is.null(worker$self_host)){
    sprintf(", self_host=\"%s\"", worker$self_host)
  } else ""
  remote_cmd <- sprintf(
    "%snohup %s -e 'flumen::start_worker(master_url=\"%s\", worker_id=\"%s\"%s)' > /tmp/flumen_worker_%s.log 2>&1 < /dev/null & disown",
    cd_prefix,
    config$R_cmd,
    worker$master_url,
    worker$name,
    self_host_arg,
    worker$name
  )

  ssh_args <- ssh_argv(config, remote_cmd, detach = TRUE)

  ssh_spawn(ssh_args, worker$name,
            sprintf("%s, port %d", config$host_user, config$host_port))
}

#' @export
#' @noRd
launcher_error_message.ssh_launcher <- function(config, worker, status){
  flumen_launch_fail(
    worker = worker$name,
    target = sprintf("%s, port %d", config$host_user, config$host_port),
    error  = sprintf("ssh exited with status %d", status),
    fix    = sprintf(paste("common causes are passwordless SSH not configured, host",
                           "unreachable, R or flumen not installed on the remote host;",
                           "check with: ssh %s 'Rscript -e \"library(flumen)\"'"),
                     config$host_user))
}
