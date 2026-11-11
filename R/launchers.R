#' Launch a remote worker
#'
#' S3 generic called by [run_workflow()] once per worker, before the
#' scheduler starts. Dispatches on the class of `config` (the object
#' stored as `worker$launcher`), so new launchers can be added by
#' defining an `ssh_launcher`-style constructor plus a method.
#'
#' Built-in methods:
#' \itemize{
#'   \item `launch_worker.ssh_launcher()`: SSH + `nohup` + `disown`
#'     bootstrap (see [ssh_launcher()] for the exact command).
#'   \item `launch_worker.NULL()`: no-op. A worker with no launcher is
#'     local and is spawned by the scheduler itself, so there is nothing
#'     to bootstrap here.
#'   \item `launch_worker.function()`: calls the plain function as
#'     `config(worker)`. Useful for one-off custom launchers that do
#'     not warrant defining a new class.
#'   \item `launch_worker.default()`: raises an error for unknown
#'     classes.
#' }
#'
#' Define custom launchers by creating an S3 class (e.g., a
#' `batch_launcher()`) and a matching `launch_worker.<class>()` method.
#' Optionally also define a `launcher_error_message.<class>()` method
#' for launcher-specific failure diagnostics.
#'
#' @param config The launcher configuration (an S3 object, a function,
#'   or `NULL`).
#' @param worker The `worker_object` being bootstrapped. Methods may read
#'   `worker$master_url`, `worker$root_path`, `worker$self_host`, etc.
#'   for their own logic.
#'
#' @return One of:
#'   \itemize{
#'     \item `TRUE` (or any non-numeric value): synchronous launch — the
#'       method has already blocked until the worker is on its way, and
#'       the dispatcher treats it as done.
#'     \item An integer PID returned from [sys::exec_background()]: the
#'       method spawned an async subprocess; the dispatcher will
#'       `sys::exec_status(pid, wait = TRUE)` to collect the exit status
#'       after all workers have fired. This pattern lets many workers be
#'       launched concurrently without pulling in the `parallel` package.
#'   }
#'   Methods must `stop()` on failure so the workflow aborts cleanly.
#'
#' @seealso [ssh_launcher()], [launcher_error_message()], [new_worker()],
#'   [run_workflow()]
#'
#' @examples
#' \dontrun{
#' # A custom launcher is an S3 class plus a launch_worker() method. The method
#' # starts flumen::start_worker() on the target machine, passing the worker's
#' # master_url and name, and stop()s if it cannot. Both fields are already set
#' # by the time the method is called. The worker must run in its root_path,
#' # as ssh_launcher() arranges with `cd`: started elsewhere, files are placed
#' # inconsistently.
#' cmd_launcher <- function(template)
#'   structure(list(template = template), class = "cmd_launcher")
#'
#' launch_worker.cmd_launcher <- function(config, worker){
#'   cmd <- sprintf(config$template, worker$root_path, worker$master_url, worker$name)
#'   if(system(cmd) != 0L) stop("could not start worker ", worker$name)
#'   invisible(TRUE)   # synchronous launch, nothing left to wait for
#' }
#'
#' new_worker(
#'   name      = "w1",
#'   self_host = "192.168.1.50",
#'   root_path = "/data/flumen_run",
#'   launcher  = cmd_launcher(
#'     "cd '%s' && Rscript -e 'flumen::start_worker(master_url=\"%s\", worker_id=\"%s\")'")
#' )
#' }
#'
#' @export
launch_worker <- function(config, worker) UseMethod("launch_worker")

#' @export
#' @noRd
launch_worker.NULL <- function(config, worker){
  ## No launcher means a local worker: launch_local_and_register_supervisors()
  ## spawns its supervisor on the master's own machine, so there is nothing to
  ## bootstrap here.
  invisible(TRUE)
}

#' @export
#' @noRd
launch_worker.function <- function(config, worker){
  config(worker)
  invisible(TRUE)
}

#' @export
#' @noRd
launch_worker.default <- function(config, worker){
  stop(flumen_launch_fail(
    call   = "launch_worker()",
    worker = worker$name %||% "<unnamed>",
    error  = sprintf("no launch_worker() method for a launcher of class '%s'",
                     paste(class(config), collapse = "/")),
    fix    = "use ssh_launcher(...), a function(worker), or your own S3 method"), call. = FALSE)
}


#' Launcher-specific error message for a failed async launch
#'
#' S3 generic called by `run_workflow()` when an async launcher
#' (one that returned an integer PID) exits with non-zero status. Lets
#' each launcher class provide its own diagnostic text — e.g., the SSH
#' launcher can suggest checking passwordless SSH and remote R install.
#'
#' Define `launcher_error_message.<class>()` alongside
#' `launch_worker.<class>()` to provide a custom message; the
#' default method covers any launcher without a dedicated handler.
#'
#' @param config The launcher configuration (`worker$launcher`).
#' @param worker The `worker_object` whose launch failed.
#' @param status Integer exit status from [sys::exec_status()].
#'
#' @return A character scalar suitable for `stop()`.
#'
#' @seealso [launch_worker()]
#'
#' @export
launcher_error_message <- function(config, worker, status) UseMethod("launcher_error_message")

#' @export
#' @noRd
launcher_error_message.default <- function(config, worker, status){
  flumen_launch_fail(
    worker = worker$name %||% "<unnamed>",
    error  = sprintf("the launcher exited with status %d", status))
}


## -- ssh transport --
## Assemble the argv for one ssh invocation against `config`, ending with the
## remote command.
##   detach:          add -f, so the ssh client backgrounds itself locally after
##                    authentication and sys::exec_background returns even if the
##                    remote shell lingers around the backgrounded job. A
##                    synchronous probe must NOT set it.
##   connect_timeout: seconds for ssh's own ConnectTimeout, or NULL for the default.
ssh_argv <- function(config, remote_cmd, detach = FALSE, connect_timeout = NULL){

  args <- character(0)
  if(detach) args <- c(args, "-f")

  ## -n redirects ssh's stdin from /dev/null; required with -f, harmless without.
  args <- c(args,
            "-n",
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=accept-new")

  if(!is.null(connect_timeout)){
    args <- c(args, "-o", sprintf("ConnectTimeout=%d", as.integer(connect_timeout)))
  }

  args <- c(args, "-p", as.character(config$host_port))

  if(!is.null(config$identity)){
    args <- c(args, "-i", config$identity)
  }

  c(args,
    config$host_user,   ## "user@host" full SSH target
    remote_cmd)
}

## Spawn an ssh client asynchronously and return its PID. exec_background returns
## at once, and the scheduler dispatcher collects every PID's exit status after its
## fire-all loop, reporting failures through launcher_error_message(). `worker_name`
## and `target` fill the block's own fields, so a host is named the same way here as
## in the launch-failure path.
ssh_spawn <- function(ssh_args, worker_name, target){
  pid <- tryCatch(
    sys::exec_background("ssh", args = ssh_args, std_out = FALSE, std_err = FALSE),
    error = function(e){
      stop(flumen_launch_fail(
        worker = worker_name,
        target = target,
        error  = paste0("the ssh client could not be spawned: ", conditionMessage(e)),
        fix    = "check that ssh is installed and on the master's PATH"), call. = FALSE)
    }
  )
  as.integer(pid)
}

## The resources line of a launcher's print method, identical for every launcher
## that declares them.
cat_launcher_resources <- function(resources){
  if(is.null(resources)){
    cat("  resources : <none declared>\n")
  } else {
    cat("  resources :",
        paste(sprintf("%s=%s", names(resources),
                      vapply(resources, as.character, character(1))),
              collapse = ", "),
        "\n")
  }
}
