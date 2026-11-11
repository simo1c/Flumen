## Master/worker version guard.
##
## Every machine in a run must execute the SAME flumen build. When they do not,
## the failure is never a clean error: an older wrapper mis-reports its output
## count and the run exits 0 having delivered the WRONG FILE, or a newer
## scheduler sends a request an older worker interprets differently and the
## consumer WAITS FOREVER with nothing logged. Neither is diagnosable from the
## symptom.
##
## So the workers announce their version in the READY message and the scheduler
## refuses to start unless every one of them matches its own.

## The version string both sides compare. Kept in one function so master and
## worker can never derive it differently.
flumen_version_string <- function() {
  as.character(utils::packageVersion("flumen"))
}

## Decides whether a worker may join the run. Returns NULL when it may, or the
## message to abort with.
##
## `worker_version = NULL` means the worker sent no version field at all: it is
## running a build that predates this check, which is itself a mismatch, and one
## old enough to differ in the wire contract without saying so.
check_worker_version <- function(worker_name, worker_version,
                                 master_version = flumen_version_string()) {
  if (is.null(worker_version) || !nzchar(worker_version)) {
    return(sprintf(paste0(
      "worker '%s' did not report its flumen version: it runs a build older than this check.\n",
      "  master: %s\n",
      "  Rebuild the package and reinstall it on that worker, then run again:\n",
      "    R CMD build <flumen source dir>\n",
      "    scp flumen_*.tar.gz <worker>:/tmp/ && ssh <worker> 'R CMD INSTALL /tmp/flumen_*.tar.gz'"),
      worker_name, master_version))
  }
  if (!identical(as.character(worker_version), as.character(master_version))) {
    return(sprintf(paste0(
      "worker '%s' runs a different flumen build than the master.\n",
      "  master: %s\n",
      "  worker: %s\n",
      "  A mixed run does not fail cleanly - it delivers wrong data or hangs.\n",
      "  Reinstall flumen %s on that worker, then run again."),
      worker_name, master_version, worker_version, master_version))
  }
  NULL
}
