## Filesystem and dependency DSL: file/folder paths, in-memory object
## placeholders, expansion/aggregation variants, and the new_dep link
## between tasks. Plus new_input/new_output containers that group these
## values into input/output lists for new_*_task().


#' Declare a dependency on another task's output
#'
#' Links an input to the output of a previously defined task. Use inside
#' [new_input()] to link tasks together into a workflow DAG.
#'
#' @param task_id Character. The `task_id` of the upstream task this input
#'   depends on.
#' @param ... Exactly one [new_file()] / [new_object()] value describing which
#'   output to consume. Several values are rejected: to depend on several
#'   outputs of one task, declare one input parameter per output, each with its
#'   own `new_dep()`, or depend on a pattern with [new_file_aggr()].
#'
#' @return An object of class `dep_object`.
#'
#' @seealso [new_input()], [new_object()], [new_file()]
#'
#' @examples
#' new_input(
#'   genome = new_dep(task_id = "indexing", value = new_file("index/{{N}}.bt2"))
#' )
#'
#' @export
new_dep <- function(task_id, ...){

  values <- list(...)

  ## SEVERAL values are refused: N dependencies sharing ONE parameter name
  ## cannot reach a workflow, since build_params() gives them the same name and
  ## validate_workflow() rejects two parameters of a task that share one.
  ##
  ## Depending on several outputs of one task is expressed by one input
  ## parameter per output, each with its own new_dep(), or by a wildcard
  ## aggregation when the names form a pattern.
  if (length(values) > 1L) {
    stop(flumen_reject(
      call  = sprintf("new_dep(\"%s\")", task_id),
      error = sprintf(paste("%d values were passed, and a dependency names ONE output",
                            "of '%s'"), length(values), task_id),
      fix   = sprintf(paste("declare one input parameter per output, each with its own",
                            "new_dep(\"%s\", ...), or depend on a pattern with",
                            "new_dep(\"%s\", new_file_aggr(\"dir/{{any}}.ext\"))"),
                      task_id, task_id)), call. = FALSE)
  }

  ## A dependency value naming SEVERAL literal paths is refused, before the
  ## single-value split so that the check sees every value. The vector
  ## form of new_file() is an INPUT construct: the DSL classifies it as scenario
  ## F4, "explicit multi-file vector (known at build time), input only", and no
  ## example shows it inside a dependency.
  ##
  ## A dependency matching several of the producer's declared outputs has no way
  ## to say which it wants: it resolves into ONE unrestricted request, and an
  ## unrestricted request ships every output of that producer.
  ##
  ## The documented ways to depend on several outputs of one producer are a
  ## wildcard aggregation (scenarios F1, F2, F5) when the names form a pattern,
  ## and one input parameter per output when they do not.
  for (v in values) {
    if (!is.null(v) && inherits(v, "file_object") && length(as.character(v)) > 1L) {
      stop(flumen_reject(
        call  = sprintf("new_dep(\"%s\")", task_id),
        error = sprintf(paste("the dependency value names %d paths, and a multi-path",
                              "file value is an input construct, not a dependency"),
                        length(as.character(v))),
        fix   = sprintf(paste("depend on a pattern, new_dep(\"%s\",",
                              "new_file_aggr(\"dir/{{any}}.ext\")), or declare one input",
                              "parameter per output, each with its own new_dep(\"%s\", ...)"),
                        task_id, task_id)), call. = FALSE)
    }
  }

  ## Reached with zero or one value: the guard above refused every other case.
  {
    val <- if (length(values) == 0) NULL else values[[1]]
    ## A dependency must declare WHICH output of `task_id` it consumes: the
    ## value carries the concrete path / object pattern used both to expand the
    ## consumer (wildcard alignment) and to resolve the producer at runtime.
    ## (`is.null(val)` also catches the explicit `new_dep(task, NULL)` form,
    ## which list(...) keeps as length 1.)
    if (is.null(val)) {
      stop(flumen_reject(
        call  = sprintf("new_dep(\"%s\")", task_id),
        error = sprintf(paste("the dependency has no value, so it does not say which",
                              "output of '%s' to consume"), task_id),
        fix   = sprintf(paste("new_dep(\"%s\", value = new_file(\"path/{wc}.ext\")) or",
                              "new_dep(\"%s\", value = new_object(\"{{out}}\"))"),
                        task_id, task_id)), call. = FALSE)
    }
    dep <- list(
      task_id = task_id,
      value = val
    )
    class(dep) <- "dep_object"
    return(dep)
  }
}

#' Declare a file path
#'
#' Tags a character path as a file. Supports wildcard substitution
#' (e.g., `"sam/{sample}.sam"`) and dynamic-output markers
#' (e.g., `"index.{{N}}.bt2"`).
#'
#' @param file_path Character. The file path, optionally containing static
#'   wildcards (`{wildcard}`) or dynamic placeholders (`{{name}}`).
#'
#' @return A character vector with class `file_path`.
#'
#' @seealso [new_file_exp()], [new_file_aggr()]
#'
#' @examples
#' new_file("fastq/{sample}.fastq")
#'
#' @param auto_mkdir Logical. If `TRUE` (default), the parent directory of this
#'   output path is created before the task body runs. Set it to `FALSE` for a
#'   tool that creates its own output directory and refuses to start if it
#'   already exists, such as `cellranger --id=foo`. Honoured only in
#'   `new_output()`, ignored on inputs.
#'
#' @details To mark a file input as pre-staged on the worker (already
#'   present, so the master must not read or transfer it), wrap it with
#'   [pre_staged()].
#' @export
new_file <- function(file_path, auto_mkdir = TRUE){
  class(file_path) <- c("file_object", "file_path", "flumen_path", "character", "vector")
  ## auto_mkdir: stamp only when explicitly passed so the wrapper's default
  ## (auto-create parent dir, current behaviour) applies to unmarked files
  ## -- fully backward compatible. Set FALSE to opt out for tool-managed dirs.
  if(!missing(auto_mkdir)) attr(file_path, "auto_mkdir") <- isTRUE(auto_mkdir)
  return(file_path)
}

#' Declare an in-memory R/Python object
#'
#' Marks a value (or output placeholder) as an in-memory object passed
#' between tasks without passing through files. Use in [new_output()]
#' with a dynamic placeholder (e.g., `"{{out}}"`) so Flumen generates a
#' unique output key, and in [new_input()] via [new_dep()] to read a prior
#' task's result.
#'
#' @param object Optional character. A placeholder like `"{{out}}"`. If
#'   `NULL` (default), Flumen auto-generates a unique UUID placeholder.
#'
#' @return A character vector with class `object`.
#'
#' @seealso [new_file()], [new_dep()], [new_object_exp()], [new_object_aggr()]
#'
#' @examples
#' new_output(out = new_object("{{out}}"))
#' new_output(out = new_object())  # auto-generated placeholder
#'
#' @export
new_object <- function(object = NULL){

  if(is.null(object)){
    object <- paste0("{{", generate_uuid("w"), "}}")
  }

  class(object) <- c("flumen_object", class(object))
  return(object)
}

#' Expanded object input
#'
#' Marks an object input for wildcard expansion: each wildcard value produces
#' a separate input to a separate task instance. Counterpart of [new_object()]
#' for expansion.
#'
#' @param object Optional character placeholder (e.g., `"{sample}"`). If
#'   `NULL`, an auto-generated placeholder is used.
#'
#' @return A character vector with classes `expansion`, `object`.
#'
#' @seealso [new_object()], [new_object_aggr()], [new_file_exp()]
#'
#' @examples
#' new_input(x = new_object_exp("{sample}"))
#'
#' @export
new_object_exp <- function(object = NULL){

  if(is.null(object)){
    object <- paste0("{{", generate_uuid("w"), "}}")
  }

  class(object) <- c("expansion", "flumen_object", class(object))
  return(object)
}

#' Aggregated object input
#'
#' Marks an object input for wildcard aggregation: all wildcard values are
#' collected into a single list passed to one task instance. With `by`, the
#' consumer is instead cloned once per combination of the `by` wildcard values
#' (SQL `GROUP BY`); every OTHER wildcard is aggregated into the list each
#' instance receives (partial aggregation).
#'
#' @param object Optional character placeholder. If `NULL`, an auto-generated
#'   placeholder is used.
#' @param by Optional character vector of wildcard names to GROUP BY: one
#'   consumer instance per combination of their values, with the remaining
#'   wildcards aggregated into each instance's list. `NULL` aggregates over
#'   all wildcards (a single instance).
#'
#' @return A character vector with classes `aggregation` (or
#'   `partial_aggregation`), `object`.
#'
#' @section Element order and identity:
#' The aggregated list is **not** in wildcard order and its elements carry no
#' names: `names()` on it is `NULL`. The order is the one in which the producer
#' instances **finish**: each result is appended to the list as it arrives.
#'
#' It is therefore **not stable and not predictable**: it depends on how long
#' each instance takes, so it is neither the declared order nor the same from
#' one run to the next. One worker with instances of unequal duration is enough
#' to reorder it.
#'
#' Indexing by position therefore attributes a result to the incorrect instance
#' **silently**, with no error and a successful run: the values are real, only
#' assigned to the incorrect sample. To keep identity, have the producer return a
#' *named* value (named multi-output): the names are carried with the data and
#' arrive aligned with it, so `ms[["s1"]]` is safe while `ms[[1]]` is not.
#'
#' \preformatted{
#' # producer
#' exec = new_script('make <- function(s) setNames(list(compute(s)), paste0("s", s))')
#' # consumer
#' exec = new_script('use <- function(ms) ms[["s1"]]')
#' }
#'
#' It must be a named LIST, not a named vector. `setNames(list(v), id)` puts
#' the names on the aggregate, aligned with the values, which is what makes
#' `ms[["s1"]]` work. `setNames(v, id)` keeps the name inside each element
#' instead, so `names(ms)` is `NULL`, `ms[["s1"]]` returns `NULL`, and the
#' identity has to be read as `names(ms[[i]])`. Only the list form supports
#' lookup by name.
#'
#' Completeness and per-element integrity are guaranteed: every producer
#' instance is present exactly once, and each element arrives bit-identical.
#'
#' @seealso [new_object()], [new_object_exp()], [new_file_aggr()]
#'
#' @examples
#' new_input(x = new_object_aggr("{sample}"))
#' new_input(x = new_object_aggr("{sample}", by = "ref"))
#'
#' @export
new_object_aggr <- function(object = NULL, by = NULL){

  if(is.null(object)){
    object <- paste0("{{", generate_uuid("w"), "}}")
  }

  if(is.null(by)){
    class(object) <- c("aggregation", "flumen_object", class(object))
  } else {
    class(object) <- c("partial_aggregation", "aggregation", "flumen_object", class(object))
    attr(object, "aggr_by") <- by
  }
  return(object)
}


#' Expanded file path
#'
#' File-path counterpart of [new_object_exp()]. Each wildcard value spawns a
#' separate input/output file.
#'
#' @param object Character. File path with wildcards (e.g.,
#'   `"fastq/{sample}.fastq"`).
#'
#' @return A character vector with classes `expansion`, `file_path`.
#'
#' @seealso [new_file()], [new_file_aggr()], [new_object_exp()]
#'
#' @examples
#' new_input(reads = new_file_exp("fastq/{sample}.fastq"))
#'
#' @param auto_mkdir Logical. If `TRUE` (default), Flumen pre-creates the
#'   parent dir of the output path. Set `FALSE` for tool-managed output dirs.
#'   See [new_file()] for the full semantics.
#' @export
new_file_exp <- function(object, auto_mkdir = TRUE){
  class(object) <- c("expansion", "file_object", "file_path", "flumen_path", "character", "vector")
  if(!missing(auto_mkdir)) attr(object, "auto_mkdir") <- isTRUE(auto_mkdir)
  return(object)
}

#' Aggregated file path
#'
#' File-path counterpart of [new_object_aggr()]. Collects the expanded file
#' paths into a list passed to one task instance.
#'
#' @param object Character. File path with wildcards.
#' @param by Optional character vector of wildcard names to GROUP BY: one
#'   consumer instance per combination of their values, with the remaining
#'   wildcards aggregated into each instance's list (SQL `GROUP BY`; see
#'   `new_object_aggr()`). `NULL` aggregates over all wildcards.
#'
#' @return A character vector with classes `aggregation` (or
#'   `partial_aggregation`), `file_path`.
#'
#' @section Element order and identity:
#' **Do not index by position.** The order is not the one declared in
#' [new_wildcard_list()] and no order is guaranteed.
#'
#' Unlike [new_object_aggr()], a file gather does **not** follow the order in
#' which the producers finish: the paths come from wildcard expansion, which
#' happens before any task runs.
#'
#' What does move it is the worker layout. On a single worker the order matches
#' the declaration; across several workers it reflects how the instances were
#' assigned, so it is no longer the declared order and it can differ from run
#' to run when workers contend for instances. The order is therefore
#' insensitive to the data but sensitive to the deployment.
#'
#' Identity is nonetheless recoverable here, unlike [new_object_aggr()]: the
#' elements are file *paths*, and the file name carries the wildcard value, so
#' match on `basename()` rather than on the position. Do this always, even when
#' the order appears correct.
#'
#' Completeness and byte-level integrity are guaranteed: every producer
#' instance is present exactly once, and each file arrives byte-identical,
#' whatever its size relative to `chunk_size` and including zero-length files.
#'
#' @seealso [new_file()], [new_file_exp()], [new_object_aggr()]
#'
#' @examples
#' new_input(dfs = new_file_aggr("data_{id}.tsv"))
#'
#' @param auto_mkdir Logical. If `TRUE` (default), Flumen pre-creates the
#'   parent dir of the output path. Set `FALSE` for tool-managed output dirs.
#'   See [new_file()] for the full semantics.
#' @export
new_file_aggr <- function(object, by = NULL, auto_mkdir = TRUE){
  if(is.null(by)){
    class(object) <- c("aggregation", "file_object", "file_path", "flumen_path", "character", "vector")
  } else {
    class(object) <- c("partial_aggregation", "aggregation", "file_object", "file_path", "flumen_path", "character", "vector")
    attr(object, "aggr_by") <- by
  }
  if(!missing(auto_mkdir)) attr(object, "auto_mkdir") <- isTRUE(auto_mkdir)
  return(object)
}

#' Declare inputs for a task
#'
#' Groups named or positional values as inputs to [new_R_task()],
#' [new_python_task()], or [new_shell_task()]. Values can be literals, [new_file()]
#' paths, [new_object()] placeholders, [new_dep()] dependencies, or
#' wildcard-expanded variants (e.g., [new_file_exp()]).
#'
#' @param ... Named or unnamed arguments describing inputs. Names become
#'   argument names in the script (R/Python) or CLI flag names (shell). Wrap
#'   a file input in [pre_staged()] to mark it as already present on the
#'   worker (read in place, not transferred from the master).
#'
#' @return An `input_object` list.
#'
#' @seealso [new_output()], [new_file()], [new_object()], [new_dep()],
#'   [pre_staged()]
#'
#' @examples
#' new_input(threads = 4, reads = new_file("{sample}.fastq"))
#' new_input(genome = pre_staged(new_file("/ref/genome.fa")))
#'
#' @export
new_input <- function(...){
  par <- list(...)
  class(par) <- c("input_object", class(par))
  return(par)
}

#' Mark a file input as pre-staged on the worker
#'
#' Wraps a [new_file()] or [new_file_exp()] value to declare that the file is
#' already present on every candidate worker (pre-staged via rsync, an NFS
#' mount, a container volume, etc.). Flumen then reads it in place on the
#' worker instead of reading and shipping its bytes from the master.
#'
#' Only `new_file()` and `new_file_exp()` values may be wrapped. Wrapping a
#' `new_file_aggr()` (aggregations belong inside a [new_dep()]), a
#' [new_object()], a [new_dep()], or a plain literal raises an error.
#'
#' @param x A value produced by [new_file()] or [new_file_exp()].
#'
#' @return `x`, tagged so the worker reads it in place.
#'
#' @seealso [new_file()], [new_file_exp()], [new_input()]
#'
#' @examples
#' new_input(
#'   genome = pre_staged(new_file("/ref/genome.fa")),
#'   reads  = pre_staged(new_file_exp("{sample}.fastq"))
#' )
#'
#' @export
pre_staged <- function(x){
  cls <- class(x)
  if(!any(c("file_object", "file_path") %in% cls) || "aggregation" %in% cls){
    stop(flumen_reject(
      call  = "pre_staged()",
      error = sprintf(paste("only new_file() or new_file_exp() values can be pre-staged,",
                            "got '%s'"), cls[1]),
      fix   = paste("wrap a new_file() value; new_file_aggr(), new_object(), new_dep()",
                    "and plain literals are not allowed")), call. = FALSE)
  }
  attr(x, "to_transfer") <- FALSE
  x
}

#' Declare outputs for a task
#'
#' Groups named or positional values as outputs produced by a task.
#' Values are typically [new_file()] paths for shell tasks or [new_object()]
#' placeholders for R/Python tasks.
#'
#' @param ... Named or unnamed arguments describing outputs.
#'
#' @return An `output_object` list.
#'
#' @seealso [new_input()], [new_file()], [new_object()]
#'
#' @examples
#' new_output(out = new_object("{{out}}"))
#' new_output(sam = new_file("sam/{sample}.sam"))
#'
#' @export
new_output <- function(...){
  par <- list(...)
  class(par) <- c("output_object", class(par))
  return(par)
}


## A declared output that exists before the task runs is refused at the start
## (R/wrapper_lib.R::wrapper_refuse_preexisting_outputs), so existence after the
## run is the whole question: what a task produced is never deduced from mtime and
## size against a snapshot.


## Warn when a declared output was produced but is EMPTY (0 bytes).
##
## The output contract is a liveness check, "did your task touch this path", not a
## content check: an empty VCF, an empty BED of filtered regions, an empty "no
## significant hits" table are all legitimate results, and refusing them would
## break correct pipelines.
##
## The warning goes to stderr, the one channel forwarded to the master console in
## every language (a Python task's stdout is swallowed on success), and to the
## per-instance log so it survives the run.
warn_empty_declared_output <- function(path, task_id){
  sz <- tryCatch(file.info(path)$size, error = function(e) NA_real_)
  if(!isTRUE(is.finite(sz)) || sz != 0) return(invisible(FALSE))
  msg <- sprintf(paste0("task '%s' declared output '%s' and produced it EMPTY ",
                        "(0 bytes); downstream tasks will read no data from it."),
                 task_id, path)
  cat(paste0("[WR_WARN] ", msg, "\n"), file = stderr())
  try(to_log(paste0("[WR_WARN] ", msg), task_id), silent = TRUE)
  invisible(TRUE)
}


## Name the files a task WROTE into a declared dynamic output's directory that the
## pattern does not match, and which nobody will collect.
##
## The dynamic contract fails loudly only when the pattern matches NOTHING, so
## matching five of six drops the sixth without a word. The static contract has no
## equivalent hole, since zero files there is an error.
##
## A WARNING and not an error, because by-products beside the output are ordinary:
## a `.bai` next to a `.bam`, an index, a log. The engine cannot know the user
## meant six rather than five, so it only names the files it is leaving behind.
##
## Only DYNAMIC, non-recursive patterns are examined. A static output's neighbours
## are ignored by contract, and a recursive bundle (`dir/`{{any}}) claims its whole
## sub-tree, so nothing under it can be left out.
##
## `taken` must be what EVERY declared output matched, not just this one: a task
## declaring two patterns in one directory would otherwise report each one's files
## as the other one's leftovers. That is why this lives here rather than in
## collect_declared_output(), which sees one output at a time.
warn_unmatched_produced_files <- function(out_files, files_new, task_id,
                                          out_dirs_snapshot, max_named = 5L){
  if(!length(out_files)) return(invisible(FALSE))
  clean <- strip_backticks(out_files)
  keep  <- grepl(DYN_WILDCARD_PATTERN, basename(clean), perl = TRUE) &
           !vapply(clean, is_recursive_wildcard, logical(1), USE.NAMES = FALSE)
  if(!any(keep)) return(invisible(FALSE))

  taken <- if(length(files_new)) normalizePath(files_new, mustWork = FALSE) else character(0)
  hit   <- FALSE
  for(d in unique(dirname(clean[keep]))){
    prod <- produced_files(d, FALSE, out_dirs_snapshot)
    if(!length(prod)) next
    full <- file.path(d, prod)
    ## A directory the task created is not a dropped output.
    full <- full[!dir.exists(full)]
    if(!length(full)) next
    extra <- setdiff(normalizePath(full, mustWork = FALSE), taken)
    if(!length(extra)) next
    ## Bounded: a tool that writes dozens of by-products would otherwise turn one
    ## warning into a wall, so past max_named the rest are counted, not listed.
    shown <- basename(extra)
    more  <- length(shown) - max_named
    if(more > 0L) shown <- c(shown[seq_len(max_named)], sprintf("and %d more", more))
    msg <- sprintf(paste0("task '%s' wrote %d file(s) into '%s' that no declared output ",
                          "pattern matches, so they are NOT delivered downstream: %s. ",
                          "Declared: %s."),
                   task_id, length(extra), d, paste(shown, collapse = ", "),
                   paste(sprintf("'%s'", out_files[keep & dirname(clean) == d]), collapse = ", "))
    ## stderr for the same reason warn_empty_declared_output() uses it: it is the
    ## one channel forwarded to the master console in every language.
    cat(paste0("[WR_WARN] ", msg, "\n"), file = stderr())
    try(to_log(paste0("[WR_WARN] ", msg), task_id), silent = TRUE)
    hit <- TRUE
  }
  invisible(hit)
}


## The names that appeared in `dir` since the pre-execution snapshot was taken.
##
## Read only by warn_unmatched_produced_files(), so its warning about undeclared
## by-products does not name the directory's unrelated neighbours. NOT used to
## decide what a declared output produced (see
## R/wrapper_lib.R::wrapper_refuse_preexisting_outputs).
produced_files <- function(dir, recursive, snapshot){
  key  <- paste0(dir, "\t", recursive)
  before <- if(key %in% names(snapshot)) snapshot[[key]]$files else character(0)
  all_files <- if(dir.exists(dir)) list.files(dir, recursive = recursive) else character(0)
  setdiff(all_files, before)
}

## What one DECLARED output contributed, or why the task failed to produce it.
##
## The declared-output contract, "a task that declares an output must produce it",
## is decided here for both inst/scripts/wrapper.R and wrapper_shell.R: the same
## three branches, the same message strings. wrapper.py keeps its own copy, being
## Python, so the three must be kept in step by hand.
##
## Returns list(files = <paths to hand downstream>, error = <NULL, or the message
## to report>). It does NOT stop the run: each wrapper notifies the supervisor over
## its own socket and exits its own way, so the fatal part stays with the caller
## and this function only reports.
collect_declared_output <- function(file, task_id){
  file <- strip_backticks(file)
  dir  <- dirname(file)
  base <- basename(file)

  ## Everything the pattern matches now is the task's own: nothing matching
  ## existed before the body ran (wrapper_refuse_preexisting_outputs), so no
  ## comparison with a snapshot is needed, or made. recursive=TRUE for a bare
  ## {{any}} pattern (whole-folder transfer).
  current_files <- if(dir.exists(dir)) list.files(dir, recursive = is_recursive_wildcard(file)) else character(0)
  file_matched  <- extract_multiple_wildcard_values(base, current_files)$matches

  if(length(file_matched))
    return(list(files = file.path(dir, file_matched), error = NULL))

  if(grepl(DYN_WILDCARD_PATTERN, base, perl = TRUE))
    ## Dynamic pattern declared but matched ZERO files. Fail at the producer
    ## rather than propagate the literal pattern string downstream.
    return(list(files = character(0), error = sprintf(
      paste0("task '%s' declared dynamic output pattern '%s' but produced no ",
             "matching files in directory '%s'. Fix: check the tool actually ",
             "wrote files matching the pattern (working directory, path prefix, ",
             "extension)."),
      task_id, file, dir)))

  if(!file.exists(file))
    ## Static literal output not produced. Existence is enough here: a file already
    ## there before the body ran was refused at the start
    ## (wrapper_refuse_preexisting_outputs), so one present now was written by this run.
    return(list(files = character(0), error = sprintf(
      paste0("task '%s' declared output '%s' but the task did not produce it ",
             "(no such file exists after the task ran). Fix: check the tool ",
             "actually wrote it (working directory, path prefix, file name)."),
      task_id, file)))

  warn_empty_declared_output(file, task_id)
  list(files = file, error = NULL)
}
