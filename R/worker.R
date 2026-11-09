## Worker constructor and the validation/parsing helpers it relies on.


## Auto-detect a hostname or IP address for this machine that should be
## reachable from peers on the same network. Used as default for `self_host` of
## remote workers (peer-to-peer data transfer) and for `master_host` of the
## central scheduler in worker-initiated mode.
##
## Strategy, in order:
##   1. Sys.info()[["nodename"]], the system hostname. Works when DNS or
##      /etc/hosts can resolve it from peers (typical LAN, HPC, intra-VPC).
##   2. "127.0.0.1" as last resort, which works only on one machine and warns.
##
## Detection asks the system what it already knows and opens no connection of
## its own, so nothing here reaches a host outside the workflow. A machine
## whose hostname peers cannot resolve must declare `self_host` itself, which
## is what the fallback warning says.
##
## Returns a non-empty character string. Never errors: the caller validates
## semantically, e.g. reachability from peers.
detect_self_host <- function() {
  ## 1) Hostname
  nn <- tryCatch(Sys.info()[["nodename"]], error = function(e) "")
  if (is.character(nn) && length(nn) == 1 && nzchar(nn) && nn != "localhost") {
    return(nn)
  }

  ## 2) Last-resort fallback
  warning(flumen_warn(
    call    = "detect_self_host()",
    warning = paste("could not determine a network-reachable hostname, falling",
                    "back to '127.0.0.1', which works only on one machine"),
    fix     = "pass self_host explicitly for a multi-host workflow"), call. = FALSE)
  "127.0.0.1"
}


## Parse a memory size string into bytes. Used internally for resource matching.
## Accepted: a number with K, M, G or T, optionally followed by 'B' ("32G",
## "128GB", "16384M", "1.5T"); a bare number is bytes. The prefixes are decimal.
parse_mem_to_bytes <- function(s) {
  if (!is.character(s) || length(s) != 1 || is.na(s) || nchar(s) == 0) {
    stop(flumen_reject(
      call  = "parse_mem_to_bytes()",
      error = sprintf("expected a single non-empty string, got '%s'",
                      paste(s, collapse = ", ")),
      fix   = "pass one size, '32G'"), call. = FALSE)
  }

  s_trim <- trimws(s)
  m <- regmatches(s_trim,
                  regexec("^([0-9]+(?:\\.[0-9]+)?)\\s*([KMGTkmgt]?)([Bb]?)$", s_trim))[[1]]

  if (length(m) < 3 || m[2] == "") {
    stop(flumen_reject(
      call  = "parse_mem_to_bytes()",
      error = sprintf("cannot parse memory size '%s'", s),
      fix   = MEM_SIZE_FIX), call. = FALSE)
  }

  num <- as.numeric(m[2])
  unit <- toupper(m[3])
  multiplier <- switch(unit,
                       "K" = 1e3,
                       "M" = 1e6,
                       "G" = 1e9,
                       "T" = 1e12,
                       1)

  num * multiplier
}


## Argument validation for resources / requires lists.
## Both `worker$launcher$resources` and `op$requires` follow the same canonical
## schema: a named list with optional fields 'cpu' (non-negative integer),
## 'mem' (parsable memory string), 'gpu' (non-negative integer) and 'tags'
## (non-empty character vector of capability labels). Unknown fields are
## rejected. NULL is accepted (no constraint / no declaration).
## `context` is used in error messages to identify the source.
## One wording for "that is not a memory size", raised from the field check and
## from the parser it calls.
MEM_SIZE_FIX <- paste("use a size with unit, '32G', '128GB', '16384M', or a bare",
                      "integer of bytes")

validate_resources <- function(resources, context = "resources") {
  if (is.null(resources)) {
    return(invisible(TRUE))
  }

  if (!is.list(resources) || length(resources) == 0 ||
      is.null(names(resources)) || any(names(resources) == "") || any(is.na(names(resources)))) {
    stop(flumen_reject(
      call  = context,
      error = "must be a named list, or NULL",
      fix   = "resources = list(cpu = 16, mem = \"64G\")"), call. = FALSE)
  }

  allowed_fields <- c("cpu", "mem", "gpu", "tags")
  unknown <- setdiff(names(resources), allowed_fields)
  if (length(unknown) > 0) {
    stop(flumen_reject(
      call  = context,
      error = sprintf("unknown field(s) %s; allowed: %s",
                      paste0("'", unknown, "'", collapse = ", "),
                      paste0("'", allowed_fields, "'", collapse = ", ")),
      fix   = "use one of the allowed fields"), call. = FALSE)
  }

  if (any(duplicated(names(resources)))) {
    stop(flumen_reject(
      call  = context,
      error = sprintf("duplicated field name(s) %s",
                      paste0("'", names(resources)[duplicated(names(resources))],
                             "'", collapse = ", ")),
      fix   = "declare each field once"), call. = FALSE)
  }

  if (!is.null(resources$cpu)) {
    if (!is.numeric(resources$cpu) || length(resources$cpu) != 1 ||
        is.na(resources$cpu) || resources$cpu < 0 ||
        resources$cpu != as.integer(resources$cpu)) {
      stop(flumen_reject(
        call  = context,
        error = "'cpu' must be a single non-negative integer",
        fix   = "resources = list(cpu = 16)"), call. = FALSE)
    }
  }

  if (!is.null(resources$mem)) {
    if (!is.character(resources$mem) || length(resources$mem) != 1 ||
        is.na(resources$mem) || nchar(resources$mem) == 0) {
      stop(flumen_reject(
        call  = context,
        error = "'mem' must be a single non-empty string",
        fix   = "resources = list(mem = \"64G\")"), call. = FALSE)
    }
    val <- tryCatch(parse_mem_to_bytes(resources$mem),
                    error = function(e) NA_real_)
    if (is.na(val)) {
      stop(flumen_reject(
        call  = context,
        error = sprintf("'mem' value '%s' cannot be parsed as a memory size", resources$mem),
        fix   = MEM_SIZE_FIX), call. = FALSE)
    }
  }

  if (!is.null(resources$gpu)) {
    if (!is.numeric(resources$gpu) || length(resources$gpu) != 1 ||
        is.na(resources$gpu) || resources$gpu < 0 ||
        resources$gpu != as.integer(resources$gpu)) {
      stop(flumen_reject(
        call  = context,
        error = "'gpu' must be a single non-negative integer",
        fix   = "resources = list(gpu = 1)"), call. = FALSE)
    }
  }

  if (!is.null(resources$tags)) {
    if (!is.character(resources$tags) || length(resources$tags) == 0 ||
        any(is.na(resources$tags)) || any(nchar(resources$tags) == 0)) {
      stop(flumen_reject(
        call  = context,
        error = "'tags' must be a non-empty character vector of non-empty strings",
        fix   = "resources = list(tags = c(\"gpu\", \"highmem\"))"), call. = FALSE)
    }
  }

  invisible(TRUE)
}


## Argument validation for new_worker. Raises stop() only for clearly broken
## cases: a missing or empty 'name', an empty 'python_cmd', a launcher of
## unsupported type, 'resources' declared on both the worker and its launcher or
## not matching the schema, 'ports' that is not a contiguous integer range in
## [1024, 65535] of length >= 2, and a 'self_host' that is not a bare hostname
## or IP.
## In worker-initiated mode the user does not declare a host: the worker
## auto-detects its own address (override via `self_host`) and the
## TCP port footprint is the contiguous `ports` range.
validate_worker <- function(worker) {

  if(is.null(worker$name) || !is.character(worker$name) ||
     length(worker$name) != 1 || is.na(worker$name) || nchar(worker$name) == 0){
    stop(flumen_reject(
      call  = "new_worker()",
      error = "'name' is required and must be a single non-empty string",
      fix   = "new_worker(name = \"w1\")"), call. = FALSE)
  }

  ## A launcher that starts the session somewhere other than root_path leaves the
  ## worker with TWO base directories, and they are not interchangeable: a file
  ## received from a peer is rooted under root_path, a file sent by the master
  ## lands in the process's working directory, and the task resolves its own
  ## relative paths against the working directory. The three agree only when the
  ## two directories are the same, which is why ssh_launcher()'s `cwd` defaults to
  ## root_path. Setting it elsewhere is legal -- the paths may be absolute, and
  ## then none of this applies -- so this warns rather than refuses. Said at
  ## new_worker() time, in the user's own session: the symptoms appear much later
  ## and on the worker, and they never name this as the cause.
  if(!is.null(worker$launcher) && inherits(worker$launcher, "ssh_launcher") &&
     !is.null(worker$launcher$cwd) && !is.null(worker$root_path) &&
     !identical(as.character(worker$launcher$cwd)[1], as.character(worker$root_path)[1])){
    warning(flumen_warn(
      call    = sprintf("new_worker(name = '%s')", worker$name),
      warning = sprintf(paste("the launcher starts the session in '%s' but the worker declares",
                              "root_path = '%s'. A file received from a peer will be rooted under",
                              "the second, a file sent by the master will land in the first, and a",
                              "task will resolve its relative paths against the first"),
                        as.character(worker$launcher$cwd)[1], as.character(worker$root_path)[1]),
      fix     = paste("leave ssh_launcher(cwd = NULL), which defaults to root_path, or set the two",
                      "to the same directory; declared paths that are absolute are unaffected")),
      call. = FALSE)
  }

  ## An empty python_cmd reaches the supervisor as the command to spawn, which
  ## fails at run time with "Failed to spawn wrapper: ''" -- long after the
  ## workflow started. The usual cause is an unset environment variable, since
  ## Sys.getenv() returns "" rather than erroring.
  if(is.null(worker$python_cmd) || !is.character(worker$python_cmd) ||
     length(worker$python_cmd) != 1 || is.na(worker$python_cmd) ||
     nchar(worker$python_cmd) == 0){
    stop(flumen_reject(
      call  = sprintf("new_worker(name = '%s')", worker$name),
      error = sprintf(paste("'python_cmd' must be a single non-empty string naming the",
                            "Python interpreter, got %s"),
                      if(is.null(worker$python_cmd)) "NULL"
                      else deparse(worker$python_cmd)),
      fix   = paste("it defaults to 'python3'; a common cause is Sys.getenv() on a",
                    "variable that is not set, which returns \"\"")), call. = FALSE)
  }

  ## launcher must be NULL, a function, or an object with a class attribute
  ## (for S3 dispatch via launch_worker()). Any other value is rejected.
  if(!is.null(worker$launcher) &&
     !is.function(worker$launcher) &&
     is.null(attr(worker$launcher, "class"))){
    stop(flumen_reject(
      call  = sprintf("new_worker(name = '%s')", worker$name),
      error = "'launcher' must be NULL, a function, or an object with a class attribute",
      fix   = paste("ssh_launcher(host_user = \"user@node01\"), or NULL for a local",
                    "worker")), call. = FALSE)
  }

  ## resources on the worker are for LOCAL workers only. Launched workers
  ## (those with a launcher) declare resources on the launcher, which is
  ## authoritative, because it describes what the launcher actually provisions.
  ## Declaring resources in both places is
  ## ambiguous, so reject it; otherwise validate the worker-level schema.
  if(!is.null(worker$resources)){
    if(!is.null(worker$launcher)){
      stop(flumen_reject(
        call  = sprintf("new_worker(name = '%s')", worker$name),
        error = paste("'resources' on new_worker() is for local workers only, and this",
                      "worker has a launcher"),
        fix   = paste("declare them on the launcher for a remote worker, or set the",
                      "launcher to NULL for a local one")),
        call. = FALSE)
    }
    validate_resources(worker$resources,
                       context = sprintf("new_worker(name = '%s') 'resources'", worker$name))
  }

  ## ports: contiguous integer range, all in [1024, 65535], length >= 2.
  ## ports[1] is the worker's data-request listen port; ports[2:N] form the
  ## peer-to-peer transfer pool (cycled/reused across transfers).
  p <- worker$ports
  if(is.null(p) || !is.numeric(p) || length(p) < 2L || any(is.na(p)) ||
     any(p != as.integer(p)) || any(p < 1024L) || any(p > 65535L) ||
     any(diff(as.integer(p)) != 1L)){
    stop(flumen_reject(
      call  = sprintf("new_worker(name = '%s')", worker$name),
      error = paste("'ports' must be a contiguous integer range of length >= 2 with all",
                    "values in [1024, 65535]"),
      fix   = paste("new_worker(ports = 61100:61199); the first port is the data-request",
                    "listen port, the rest are the peer-to-peer pool")), call. = FALSE)
  }

  ## self_host: a bare hostname or IP. NOT 'user@host' (that belongs in
  ## ssh_launcher(host_user = ...)), and no URL scheme/port/whitespace. A
  ## malformed self_host becomes an invalid P2P dial target
  ## ("tcp://user@host:port"), so it is refused at build time.
  if(!is.null(worker$self_host)){
    sh <- worker$self_host
    if(!is.character(sh) || length(sh) != 1L || is.na(sh) || !nzchar(sh)){
      stop(flumen_reject(
        call  = sprintf("new_worker(name = '%s')", worker$name),
        error = "'self_host' must be a single non-empty string, a bare hostname or IP",
        fix   = "new_worker(self_host = \"10.1.2.3\")"), call. = FALSE)
    }
    if(grepl("@", sh, fixed = TRUE)){
      stop(flumen_reject(
        call  = sprintf("new_worker(name = '%s')", worker$name),
        error = sprintf("'self_host' is '%s', not a bare hostname or IP", sh),
        fix   = sprintf(paste("the 'user@host' form belongs in the launcher,",
                              "ssh_launcher(host_user = \"%s\")"), sh)), call. = FALSE)
    }
    if(grepl("://", sh, fixed = TRUE) || grepl("[[:space:]]", sh)){
      stop(flumen_reject(
        call  = sprintf("new_worker(name = '%s')", worker$name),
        error = sprintf(paste("'self_host' must have no URL scheme, port or whitespace,",
                              "got '%s'"), sh),
        fix   = "new_worker(self_host = \"node01\")"), call. = FALSE)
    }
  }

  invisible(TRUE)
}


#' Define a worker
#'
#' A worker is an execution endpoint identified by `name`. At runtime, every
#' worker -- local or remote -- connects outbound to the central scheduler
#' (worker-initiated model). Workflows are scheduled across one or more
#' workers.
#'
#' @param name Character. Unique worker name within the workflow.
#' @param n_concurrent_task Integer. Maximum number of tasks the worker
#'   runs in parallel. Defaults to `1`.
#' @param self_host Optional character, remote workers only. IP or hostname of
#'   this worker as seen by the master and by peer workers. Auto-detected by
#'   default. Set it explicitly on a multi-interface host, or when the hostname
#'   is not resolvable from the peers. On a local worker (`launcher = NULL`) it
#'   must be `NULL`, and passing it raises an error.
#' @param ports Integer vector. Contiguous TCP port range this worker uses for
#'   peer-to-peer data transfer. `ports[1]` is the data-request listen port and
#'   `ports[2:length(ports)]` form the transfer pool. Defaults to
#'   `61100:61199`. It has no effect in an all-local workflow, where peer
#'   transfer uses IPC. Override it when the firewall imposes a different
#'   range, or when two workers share a host, since their ranges must not
#'   overlap and [run_workflow()] errors if they do. Must be contiguous, of
#'   length at least 2, with all values in `[1024, 65535]`, and wide enough for
#'   the worker's `n_concurrent_task`.
#' @param cache_ram_limit Integer (MB). Maximum RAM for the result cache
#'   before spilling to disk. Defaults to `4096`.
#' @param cache_disk_limit Integer (MB). Maximum disk space for spilled
#'   results. Defaults to `51200`.
#' @param cache_dir Optional character. Directory for spilled results. `NULL`
#'   uses `tempdir()` on the worker machine.
#' @param R_cmd Character. R interpreter for R tasks. Defaults to
#'   `"Rscript"`. Use `"Rscript.exe"` on Windows.
#' @param python_cmd Character. Python interpreter for Python tasks.
#'   Defaults to `"python3"`.
#' @param shell_cmd Character. Shell for shell tasks. Defaults to
#'   `"bash"`. Use `"cmd.exe"` or `"powershell"` on Windows.
#' @param root_path Optional character. Base directory for received files on
#'   this worker. `NULL` uses the current directory.
#' @param launcher Optional. How the worker is bootstrapped. Three forms:
#'   \itemize{
#'     \item `NULL` (default): a local worker, spawned by the scheduler on the
#'       master's own machine and reached over IPC. A worker is remote only if
#'       it has a launcher. Not available on Windows, where [run_workflow()]
#'       rejects a worker with no launcher.
#'     \item An S3-classed object such as [ssh_launcher()], dispatched through
#'       the [launch_worker()] generic. Define your own class and method to
#'       bootstrap through a batch scheduler or a container.
#'     \item A plain function `function(worker)`, called with the worker object.
#'       It carries no `resources`.
#'   }
#'   In every non-`NULL` form the worker must connect outbound to the master
#'   within `startup_timeout`, and the launcher must `stop()` on failure.
#' @param resources Optional named list declaring the resources this worker
#'   provides, matched against a task's `requires` (see [new_R_task()]). Same
#'   schema as [ssh_launcher()] `resources`. Local workers only
#'   (`launcher = NULL`): for a launched worker declare resources on the
#'   launcher instead, and setting both is an error. `NULL` (default) declares
#'   no resources.
#'
#' @seealso [run_workflow()], [start_worker()]
#'
#' @examples
#' \dontrun{
#' # Local worker (auto-spawned by the scheduler)
#' new_worker(name = "local1", n_concurrent_task = 4)
#'
#' # Remote worker started by hand: a launcher whose launch_worker() method
#' # does nothing, so the scheduler reserves the ports and waits while you
#' # run start_worker() on the other machine, in the worker's root_path
#' manual_launcher <- function(resources = NULL)
#'   structure(list(resources = resources), class = "manual_launcher")
#' launch_worker.manual_launcher <- function(config, worker) invisible(TRUE)
#' new_worker(name = "remote1", self_host = "192.168.1.50",
#'            n_concurrent_task = 2, launcher = manual_launcher())
#'
#' # Remote worker with SSH auto-launch (passwordless SSH required)
#' new_worker(
#'   name      = "remote1",
#'   root_path = "/home/myuser/project",
#'   launcher  = ssh_launcher(host_user = "myuser@192.168.1.50")
#' )
#'
#' # Multi-interface override (e.g., HPC node with both Ethernet and
#' # Infiniband; force peers to use the Infiniband address)
#' new_worker(
#'   name      = "hpc_node1",
#'   self_host = "10.0.100.5",
#'   launcher  = ssh_launcher(host_user = "myuser@compute01.cluster.edu")
#' )
#'
#' # Two workers on the same host: give them non-overlapping port ranges
#' new_worker(name = "w_a", ports = 61100:61199,
#'            launcher = ssh_launcher(host_user = "me@bignode"))
#' new_worker(name = "w_b", ports = 61200:61299,
#'            launcher = ssh_launcher(host_user = "me@bignode"))
#' }
#'
#' @return An object of class `worker_object`, to be passed in the
#'   `worker_list` argument of [run_workflow()].
#'
#' @export
new_worker <- function(name = NULL,
                       n_concurrent_task = 1,
                       self_host = NULL,
                       ports = 61100:61199,
                       cache_ram_limit  = 4096L,
                       cache_disk_limit = 51200L,
                       cache_dir        = NULL,
                       R_cmd      = "Rscript",
                       python_cmd = "python3",
                       shell_cmd  = "bash",
                       root_path  = NULL,
                       launcher   = NULL,
                       resources  = NULL
                       ){

  ## 'self_host' is reserved for remote workers. Local workers (launcher=NULL)
  ## inherit the master's address from run_workflow(master_host = ...): they
  ## are co-located with the master process and share its host visibility.
  ## Accepting self_host on a local worker would create two independent values
  ## describing the same machine, leading to silent inconsistencies.
  if(is.null(launcher) && !is.null(self_host)){
    stop(flumen_reject(
      call  = sprintf("new_worker(name = '%s')", name %||% "<unnamed>"),
      error = "'self_host' is reserved for remote workers, those with a launcher",
      fix   = paste("remove it; a local worker takes the master's address from",
                    "run_workflow(master_host = ...)")), call. = FALSE)
  }

  worker_object <- list(
    name = name,
    n_concurrent_task = n_concurrent_task,
    self_host = self_host,
    ports = as.integer(ports),
    cache_ram_limit  = cache_ram_limit,
    cache_disk_limit = cache_disk_limit,
    cache_dir        = cache_dir,
    R_cmd            = R_cmd,
    python_cmd       = python_cmd,
    shell_cmd        = shell_cmd,
    root_path        = root_path,
    launcher         = launcher,
    resources        = resources
  )

  class(worker_object) <- "worker_object"
  validate_worker(worker_object)
  return(worker_object)
}
