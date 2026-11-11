
## Absolute path to the Rscript of the R installation currently running, used
## whenever flumen spawns a subprocess ON THIS MACHINE: a local supervisor, or
## the supervisor that start_worker() launches on the worker host.
##
## A bare "Rscript" resolves through PATH, which is fragile: a machine with
## several R installations can pick a different one, RStudio or a cron job may
## not have it on PATH at all, and under R CMD check a shim comes first whose
## whole body is an error message and `exit 1`. The failure is silent: the
## supervisor never starts and the run ends on a startup timeout that points at
## the network instead.
##
## ssh_launcher keeps taking a bare command in its R_cmd argument: that one runs
## on another machine, where this path means nothing.
rscript_cmd <- function(){
  cand <- file.path(R.home("bin"),
                    if(.Platform$OS.type == "windows") "Rscript.exe" else "Rscript")
  if(file.exists(cand)) cand else "Rscript"
}

## Log file name for a sender. A sender is a human-readable label ("Central
## scheduler", or any name given to new_worker()), so it may contain spaces or
## other characters that are not portable across filesystems. Fold everything
## outside [A-Za-z0-9._-] into "_" so the file name stays portable; the label
## itself is untouched inside the log lines, where it is easier to read.
log_file_name <- function(sender){
  paste0(gsub("[^A-Za-z0-9._-]+", "_", as.character(sender)[1]), "_log.log")
}

#' Append a timestamped message to the Flumen log
#'
#' Writes a timestamped line to a log file under `log/` in the process's
#' current working directory (falling back to `tempdir()` if that directory
#' cannot be created). Used internally by workers and the scheduler; exposed
#' so that user code running inside a task can contribute to the same log
#' stream.
#'
#' @param msg Character. The message to record.
#' @param sender Character. Identifier of the component producing the message
#'   (e.g. a worker name or `"Central scheduler"`).
#' @param log_file Character. File name within the log directory. Defaults to
#'   the sender name with any character outside `[A-Za-z0-9._-]` folded into
#'   `_`, followed by `_log.log`, so the name stays portable across
#'   filesystems.
#'
#' @return Called for its side effect. Invisibly returns `NULL`.
#'
#' @keywords debugging
#' @examples
#' # The log directory comes from FLUMEN_LOG_DIR, which run_workflow(log_dir =)
#' # publishes, and falls back to <cwd>/log when the variable is unset.
#' old <- Sys.getenv("FLUMEN_LOG_DIR", unset = NA)
#' Sys.setenv(FLUMEN_LOG_DIR = file.path(tempdir(), "flumen_example_log"))
#'
#' to_log("worker initialised", "ipc1")
#'
#' if (is.na(old)) Sys.unsetenv("FLUMEN_LOG_DIR") else
#'   Sys.setenv(FLUMEN_LOG_DIR = old)
#' @export
to_log <- function(msg, sender, log_file = log_file_name(sender)) {
  ## run_workflow(log_dir=) publishes its choice in FLUMEN_LOG_DIR, which
  ## sys::exec_background propagates to every local subprocess, so honouring it
  ## here is what makes that argument effective. Without the env var (a remote
  ## worker, or to_log() called outside a run) fall back to "log/" under the
  ## process's current working directory: ssh_launcher cd's the remote
  ## supervisor into worker$root_path before launching R, so on the remote host
  ## logs land under <root_path>/log/.
  env_log_dir <- Sys.getenv("FLUMEN_LOG_DIR", "")
  log_dir <- normalizePath(
    if(nzchar(env_log_dir)) env_log_dir else file.path(getwd(), "log"),
    mustWork = FALSE)

  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  log_entry <- paste0(timestamp, " - ", "[", sender, "] ", msg, "\n")

  ## Surface a dir-creation failure on stderr, naming the path and the reason
  ## (permissions, read-only mount, a cwd that no longer exists).
  if(!dir.exists(log_dir)){
    ok <- tryCatch(
      dir.create(log_dir, recursive = TRUE, showWarnings = TRUE),
      error = function(e){
        message("[to_log] FAILED to create log dir '", log_dir, "': ", conditionMessage(e))
        FALSE
      }
    )
    if(!isTRUE(ok)){
      ## Fallback so we never lose the log entry: append to /tmp so the user
      ## can still find the trace if cwd-based creation fails.
      fallback <- file.path(tempdir(), paste0("flumen_fallback_", Sys.getpid(), ".log"))
      cat(paste0(log_entry, "  (would-be path: ", file.path(log_dir, log_file), ")\n"),
          file = fallback, append = TRUE)
      return(invisible(NULL))
    }
  }

  ## Once per process, and into the log rather than onto the console: the entries
  ## carry neither the pid nor the cwd, and an unexpected cwd is what misplaces a
  ## log on a worker that has no FLUMEN_LOG_DIR.
  if(is.null(getOption("flumen_log_announced"))){
    options(flumen_log_announced = TRUE)
    cat(paste0(timestamp, " - [", sender, "] log at ", log_dir,
               "  (pid=", Sys.getpid(), ", cwd=", getwd(), ")\n"),
        file = file.path(log_dir, log_file), append = TRUE)
  }

  cat(log_entry, file = file.path(log_dir, log_file), append = TRUE)
}

generate_uuid <- function(prefix){
  uuid <- paste0(prefix, "-", uuid::UUIDgenerate())
  return(uuid)
}

create_dir_structure <- function(path) {

  dir_to_create <- normalizePath(dirname(path), mustWork = FALSE)

  if(!dir.exists(dir_to_create)){
    dir.create(dir_to_create, recursive = TRUE)
  }

  return(dir_to_create)
}

create_temp_sh_script <- function(bash_commands, file_prefix = "flumen_op_") {

  ## tempfile() in R session's tempdir(): unique path per op (no collision
  ## between concurrent shell ops) and auto-cleaned at R session end.
  ## The caller (wrapper_shell.R) also performs an explicit unlink after
  ## execution to free space immediately.
  file_path_temp <- tempfile(pattern = file_prefix, fileext = ".sh")

  tryCatch({
    writeLines(bash_commands, con = file_path_temp)
    Sys.chmod(file_path_temp, mode = "0755")

  }, error = function(e) {
    stop(paste("Error creating or writing temporary file:", e$message))
  })

  return(file_path_temp)
}

## --- shared_path matching -----------------------------------------------------
## These helpers decide whether a file path declared in a task lies under one of
## the shared-filesystem roots passed to run_workflow(shared_path=). The match is
## PURELY LEXICAL (no filesystem access, no symlink resolution): the result is
## therefore deployment-independent and does not depend on the master's working
## directory. shared_path is a user assertion about the deployment topology;
## these functions only guarantee the lexical relationship "path is at/under
## root" is computed correctly (no "data" vs "data2" false positives).

## Remove the backticks from a value. In a declared path a backtick is a SHELL
## MARKER, never part of the name, so every site that compares, globs, opens or
## reports such a path drops them first. A bare wrapper with no coercion of its
## own, so it takes any input type.
strip_backticks <- function(x) gsub("`", "", x, fixed = TRUE)

## Split a path into canonical components, collapsing ".", "..", duplicate and
## trailing slashes. Backslashes are treated as separators (lexical only).
path_components <- function(p) {
  p <- gsub("\\\\", "/", p)
  parts <- strsplit(p, "/", fixed = TRUE)[[1]]
  out <- character(0)
  for (seg in parts) {
    if (seg == "" || seg == ".") next
    if (seg == "..") {
      if (length(out) > 0L) out <- out[-length(out)]
      next
    }
    out <- c(out, seg)
  }
  out
}

## TRUE iff `path` names a location on its own rather than one relative to a
## working directory. Pure string logic: nothing on disk is consulted, so the
## path need not exist.
##
## The two platform families disagree on what the question even means, which is
## why this is not a single expression. On a unix-alike exactly one form is
## absolute, "/x", so the POSIX branch tests that and nothing else: on that side
## "C:/x" and "\x" are ordinary RELATIVE names and must be read as such.
## Windows has three forms: drive-absolute ("C:/x" or
## "C:\x"), a UNC share ("\\server\share"), and root-relative ("/x", "\x"),
## which R itself resolves against the current drive. "C:x" is drive-RELATIVE
## and is correctly excluded, since the separator is required.
is_absolute_path <- function(path) {
  if (.Platform$OS.type == "windows")
    grepl("^([A-Za-z]:[/\\\\]|[/\\\\])", path)
  else
    startsWith(path, "/")
}

## Names of the workers the scheduler would spawn on its own machine, i.e. the
## ones with no launcher. Kept apart from the Windows check in
## validate_exec_args() so the rule can be exercised on any platform: the
## predicate is about the worker list, never about the host it runs on.
windows_local_workers <- function(worker_list) {
  if (is.null(worker_list) || length(worker_list) == 0L) return(character(0))
  is_local <- vapply(worker_list, function(w) is.null(w$launcher), logical(1))
  if (!any(is_local)) return(character(0))
  nm <- vapply(worker_list[is_local],
               function(w) if (is.null(w$name)) "<unnamed>" else as.character(w$name)[1],
               character(1))
  nm
}

## TRUE iff `path` is at or under `root`, compared on path-component boundaries.
## An absolute root only matches an absolute path and vice versa (frame-strict):
## a relative/absolute mismatch returns FALSE rather than guessing.
path_under_root <- function(path, root) {
  if (!nzchar(path) || !nzchar(root)) return(FALSE)
  if (is_absolute_path(path) != is_absolute_path(root)) return(FALSE)
  pc <- path_components(path)
  rc <- path_components(root)
  if (length(rc) == 0L) return(FALSE)            # empty/"/" root matches nothing
  if (length(rc) > length(pc)) return(FALSE)
  identical(pc[seq_along(rc)], rc)
}

## Extract the character file paths carried by a parameter value: a character
## vector is returned as-is; a list is flattened (names dropped); anything else
## yields character(0). Defensive: a malformed list that errors on unlist also
## yields character(0). Single source of the extraction rule shared by the
## dispatch path sites (get_task_input_file_paths, the shared_path and
## already-completed skip checks in execute_single_task, emit_param_slots).
param_value_paths <- function(val) {
  if (is.character(val)) val
  else if (is.list(val)) tryCatch(as.character(unlist(val, use.names = FALSE)),
                                  error = function(e) character(0))
  else character(0)
}

## TRUE iff `path` is under ANY of the declared shared roots.
is_under_shared_path <- function(path, shared_path) {
  if (is.null(shared_path) || length(shared_path) == 0L) return(FALSE)
  for (root in shared_path) {
    if (path_under_root(path, root)) return(TRUE)
  }
  FALSE
}

## Does every file this dependency constituent resolves to lie under a declared
## shared root? Used by declare_constituent() to pick Model 3 over a byte
## transfer for a producer-to-consumer edge.
##
## The constituent carries the producer's template in `dyn_wildcard`: a concrete
## path for a static, expanded or aggregated dependency, and the BRACED template
## for a dynamic `{{k}}` one, whose values the producer only discovers at run
## time. Matching the template is sound because path_under_root() compares path
## COMPONENTS and a shared root is literal (it is a mount point the user
## declared at launch): the components spanning the root must match literally,
## so a brace falling inside that span makes the match fail and the bytes
## travel. Whatever `{{k}}` expands to therefore lies under the root whenever
## the template does, because the producer's outputs are discovered BY that
## template and cannot land outside it.
##
## All-or-nothing on several paths, as the non-dependency rule in
## execute_single_task() is, and FALSE when no path is known: an OBJECT
## constituent carries an empty template, so an in-memory output is never
## mistaken for a shared file. Every fallback is "transfer the bytes".
constituent_under_shared_path <- function(req, shared_path) {
  if (is.null(shared_path) || length(shared_path) == 0L) return(FALSE)
  p <- tryCatch(as.character(req$dyn_wildcard), error = function(e) character(0))
  p <- p[!is.na(p) & nzchar(p)]
  if (length(p) == 0L) return(FALSE)
  all(vapply(p, is_under_shared_path, logical(1), shared_path))
}

## Print, at launch, how the workflow's static (wildcard-free) file paths are
## classified under the declared shared roots, so the user can verify the match
## matches their intent before the run starts. Wildcard paths are resolved (and
## classified) only at dispatch and are therefore not listed here.
report_shared_path_classification <- function(static_file_paths, shared_path) {
  message(sprintf("[flumen] shared_path: %s", paste(shared_path, collapse = ", ")))
  if (is.null(static_file_paths) || length(static_file_paths) == 0L) return(invisible())
  is_shared <- vapply(static_file_paths, is_under_shared_path, logical(1), shared_path)
  shared   <- static_file_paths[is_shared]
  transfer <- static_file_paths[!is_shared]
  if (length(shared) > 0L)
    message(sprintf("  SHARED   (Model 3, read from the root): %s",
                    paste(shared, collapse = ", ")))
  if (length(transfer) > 0L)
    message(sprintf("  TRANSFER (Model 2, peer-to-peer):  %s",
                    paste(transfer, collapse = ", ")))
  invisible()
}
