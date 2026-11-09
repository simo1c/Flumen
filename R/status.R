## User-facing inspection of a workflow's execution state. Folds the append-only
## task table the central scheduler writes and returns it as a tidy data frame.
## Read-only: no scheduler state is touched.

#' Inspect the execution status of a Flumen workflow
#'
#' Reads the task table that the central scheduler writes and returns it as a
#' data frame: one row per task instance, with its current status and the worker
#' it was assigned to. Because a state change is appended as it happens, the
#' table can be inspected from a separate R session while a workflow runs, or
#' afterwards. The file is tab separated, so `tail -f` on it works too.
#'
#' @param path Character. Directory holding `central_scheduler_task_table_log.tsv`
#'   -- the working directory from which `run_workflow()` was called. Defaults
#'   to the current working directory.
#'
#' @return A data frame ordered by execution `Level`, with columns `Exe_ID`
#'   (task instance id), `Op_ID` (workflow task id), `Level` (dependency level),
#'   `Worker` (the worker it was sent to, `NA` until it is dispatched) and `Status`. If no
#'   task table is found, returns an empty data frame of the same shape and
#'   emits a warning.
#'
#' @section Status values:
#' `ready` (waiting for its dependencies or for a free worker), `dispatched`
#' (sent to a worker, no confirmation yet), `execution` (the worker confirmed it
#' is running), `finished` (completed, result received),
#' `failed` (this instance is the one that broke the run) and `aborted` (this
#' instance was still running when another one failed, and was killed by the
#' teardown -- it did not fail on its own).
#'
#' A table containing `failed` or `aborted` therefore describes a run that
#' ended badly; one showing `execution` with no `failed` row describes a run
#' still in progress. The failure reason is not stored here -- read it in
#' `Central_scheduler_log.log`, where the `WORKFLOW ERROR` line also names the
#' failing instance's wildcard binding.
#'
#' @seealso [run_workflow()]
#'
#' @examples
#' \dontrun{
#' run_workflow(workflow, worker_list = list(w))
#' flumen_status()
#' }
#'
#' @export
flumen_status <- function(path = getwd()) {
  cols  <- c("Exe_ID", "Op_ID", "Level", "Worker", "Status")
  empty <- data.frame(setNames(rep(list(character(0)), length(cols)), cols),
                       stringsAsFactors = FALSE)

  ## The scheduler APPENDS one line per changed row rather than rewriting the
  ## whole table on every event (R/scheduler_task_table.R::persist_task_table),
  ## so an instance appears once per state it passed through and the current
  ## state is its LAST line. Folding it here costs one pass when a person asks,
  ## instead of a full rewrite on every event during the run.
  ##
  ## The order is the order of FIRST appearance, which is the order the table was
  ## built in, not the order in which things happened to change.
  tsv <- file.path(path, "central_scheduler_task_table_log.tsv")
  if (!file.exists(tsv)) {
    warning(flumen_warn(
      call    = "flumen_status()",
      warning = sprintf("no task table found at '%s'", tsv),
      fix     = "run a workflow from this directory first"), call. = FALSE)
    return(empty)
  }

  lines <- readLines(tsv, warn = FALSE)
  if (length(lines) < 2L) return(empty)
  ## A reader can arrive mid-append: the last line may be half written, and a
  ## short one is discarded rather than parsed into a row of NAs.
  hdr   <- strsplit(lines[1L], "\t", fixed = TRUE)[[1L]]
  parts <- strsplit(lines[-1L], "\t", fixed = TRUE)
  parts <- parts[vapply(parts, length, integer(1)) == length(hdr)]
  if (!length(parts)) return(empty)

  m <- matrix(unlist(parts), ncol = length(hdr), byrow = TRUE,
              dimnames = list(NULL, hdr))
  m[m == ""] <- NA_character_        ## an empty field is a missing value, not ""
  task_table <- as.data.frame(m, stringsAsFactors = FALSE)

  keep <- !duplicated(task_table$Exe_ID, fromLast = TRUE)   ## the last state...
  ord  <- match(unique(task_table$Exe_ID), task_table$Exe_ID[keep])  ## ...in first-seen order
  task_table <- task_table[keep, , drop = FALSE][ord, , drop = FALSE]

  ## Keep only the columns meaningful to a user; Shape/Colour are
  ## visualisation-internal. intersect() tolerates a table written by an
  ## older or newer scheduler with a different column set.
  task_table <- task_table[, intersect(cols, names(task_table)), drop = FALSE]

  if ("Level" %in% names(task_table)) {
    task_table <- task_table[order(suppressWarnings(
      as.numeric(as.character(task_table$Level)))), , drop = FALSE]
  }
  rownames(task_table) <- NULL
  task_table
}
