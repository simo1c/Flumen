## Static workflow preview ("dry run"): print the execution plan WITHOUT running
## anything. Surfaces, per task: how many instances it spawns (static wildcard
## expansion is already applied by new_workflow(), so this is the count of
## expanded entries sharing a task_id), which workers are eligible (requires),
## what each input consumes (expansion vs aggregation vs by-group), and the
## outputs. Flags new_*_exp over a producer's DYNAMIC wildcard, which runs the
## consumer once per produced file.
##
## Dynamic {{...}} expansion is runtime-dependent (it depends on how many files
## a producer emits), so a task consuming a dynamic dep via _exp is annotated
## "+ runtime: expands per {{wc}}" rather than given a concrete count.

.preview_consume_mode <- function(v) {
  if (inherits(v, "partial_aggregation"))
    return(paste0("aggr by ", paste(attr(v, "aggr_by"), collapse = ",")))
  if (inherits(v, "aggregation")) return("aggr")
  if (inherits(v, "expansion"))   return("exp")
  "fixed"
}
.preview_has_dynamic <- function(v) length(extract_wildcards_from_value(v)$dynamic) > 0L
.preview_dyn_names   <- function(v) extract_wildcards_from_value(v)$dynamic

## Pull the structured input/output info from one (expanded) task entry.
.preview_scan_task <- function(op) {
  inputs <- list(); outputs <- character(0); runtime_exp <- character(0); warns <- character(0)
  for (par in op$par_list) {
    v <- par$value; if (is.null(v)) next
    nm <- if (!is.null(par$name)) par$name else "?"
    if (inherits(par, "out_par_object")) {
      outputs <- c(outputs, as.character(if (inherits(v, "dep_object")) v$value else v))
    } else if (inherits(par, "in_par_object")) {
      if (inherits(v, "dep_object")) {
        inner <- v$value
        mode  <- .preview_consume_mode(inner); dyn <- .preview_has_dynamic(inner)
        inputs[[length(inputs)+1L]] <- list(name = nm, producer = v$task_id,
                                            mode = mode, pattern = as.character(inner), dynamic = dyn)
        if (mode == "exp" && dyn) {
          runtime_exp <- union(runtime_exp, .preview_dyn_names(inner))
          ## the static wildcard this task expands over (e.g. "sample"); used to
          ## phrase "once per <unit>" and to suggest the `by =` argument. NA when
          ## the task expands over nothing static: there is no unit to group by,
          ## and `by =` must NOT be suggested there -- new_workflow() rejects a
          ## `by` naming no declared wildcard, so the advice would not even build.
          stat_wc <- character(0)
          if (!is.null(op$wildcards)) for (wn in names(op$wildcards)) {
            wv <- as.character(op$wildcards[[wn]])
            if (length(wv) && !any(grepl("\\{\\{", wv))) stat_wc <- c(stat_wc, wn)
          }
          unit <- if (length(stat_wc)) stat_wc[1] else NA_character_
          ## actual constructor family the user wrote (file vs object). The same
          ## test names WHAT the producer emits, so the noun below follows it
          ## rather than assuming files.
          is_file  <- inherits(inner, "file_object")
          base     <- if (is_file) "new_file_" else "new_object_"
          noun     <- if (is_file) "file" else "object"
          exp_ctor <- paste0(base, "exp")
          agg_ctor <- paste0(base, "aggr")
          warns <- c(warns, if (is.na(unit)) sprintf(paste0(
            "'%s' will run once for each %s produced by '%s'.\n",
            "      (input '%s' uses %s)\n",
            "      to run it once with all of them, use:\n",
            "         %s(\"...\")"),
            op$task_id, noun, v$task_id, nm, exp_ctor, agg_ctor)
          else sprintf(paste0(
            "'%s' will run once for each %s produced by '%s',\n",
            "      not once per %s. (input '%s' uses %s)\n",
            "      to run it once per %s with all its %ss, use:\n",
            "         %s(\"...\", by = \"%s\")"),
            op$task_id, noun, v$task_id, unit, nm, exp_ctor, unit, noun, agg_ctor, unit))
        }
      } else if (inherits(v, "expansion") || inherits(v, "aggregation")) {
        inputs[[length(inputs)+1L]] <- list(name = nm, producer = NA_character_,
                                            mode = .preview_consume_mode(v), pattern = as.character(v),
                                            dynamic = .preview_has_dynamic(v))
      }
    }
  }
  list(inputs = inputs, outputs = outputs, runtime_exp = runtime_exp, warns = warns)
}

#' Preview a flumen workflow without executing it
#'
#' Prints the static execution plan: per-task instance counts, eligible workers,
#' how each dependency is consumed (expansion / aggregation / by-group) and the
#' outputs. Flags likely semantic mistakes -- chiefly `new_*_exp` over a
#' producer's dynamic `{{wildcard}}`, which runs the consumer once per produced
#' file instead of aggregating them. Use it before a long run to catch
#' mis-wired dependencies, unexpected fan-out, or tasks with no eligible worker.
#'
#' @param workflow A workflow object from `new_workflow()`.
#' @param worker_list Optional list of `new_worker()` objects. When given, the
#'   report also shows the eligible workers per task, the port plan, the
#'   firewall requirements, and any port error that `run_workflow()` would
#'   raise, reported here instead of raised.
#' @param master_host Optional master address for the port plan (default:
#'   auto-detected, as in `run_workflow()`).
#' @param ports Integer range for the master's control ports in the port plan
#'   (default `61000:61099`, as in `run_workflow()`).
#' @return Invisibly, a list with `tasks` (per-task summary) and `warnings`.
#' @export
preview_workflow <- function(workflow, worker_list = NULL, master_host = NULL, ports = 61000:61099) {
  tasks <- workflow$task_list
  wl    <- workflow$wildcard_list

  ## group the (already static-expanded) entries by task_id, preserving order
  ids   <- vapply(tasks, function(op) as.character(op$task_id), character(1))
  order_ids <- unique(ids)

  ## An OBJECT wildcard is identified by its key, never by its content: that is
  ## the contract the user is given, it is why names are mandatory on one, and it
  ## is what the per-task lines below already print. Printing the VALUES here would
  ## deparse a list of data frames into the header and bury the plan under them.
  wc_show <- function(v) if (is.list(v)) paste(names(v), collapse = ",")
                         else            paste(v, collapse = ",")
  cat(sprintf("\n=== flumen workflow preview: %s ===\n", workflow$workflow_id))
  if (!is.null(wl) && length(wl))
    cat("wildcards:", paste(sprintf("%s={%s}", names(wl),
        vapply(wl, wc_show, character(1))), collapse = "  "), "\n")
  cat(strrep("-", 64), "\n")

  warns <- character(0); out_tasks <- list()
  for (tid in order_ids) {
    grp  <- tasks[ids == tid]
    n    <- length(grp)
    rep  <- grp[[1]]                       # representative entry
    type <- if (!is.null(rep$type)) rep$type else "?"
    sc   <- .preview_scan_task(rep)
    warns <- c(warns, unlist(lapply(grp, function(op) .preview_scan_task(op)$warns)))

    ## Static wildcards are already substituted in the expanded entries (sample
    ## -> "85"). Reconstruct the GENERIC template by replacing each instance's
    ## concrete static-wildcard value back with its {name} placeholder, and
    ## collect the distinct values across the group for the instance note.
    static_binds <- list(); wc_vals <- list()
    if (!is.null(rep$wildcards)) for (nm in names(rep$wildcards)) {
      val <- as.character(rep$wildcards[[nm]])
      if (length(val) && !any(grepl("\\{\\{", val))) static_binds[[nm]] <- val[1]
    }
    for (op in grp) for (nm in names(static_binds))
      wc_vals[[nm]] <- union(wc_vals[[nm]], as.character(op$wildcards[[nm]])[1])
    desubst <- function(s) {
      for (nm in names(static_binds)) s <- gsub(static_binds[[nm]], paste0("{", nm, "}"), s, fixed = TRUE)
      s
    }

    ## eligible workers (requires is identical across a task_id's instances)
    req <- rep$requires
    if (!is.null(worker_list)) {
      ok <- Filter(function(w) worker_satisfies_requires(w, req), worker_list)
      elig <- if (length(ok)) paste(vapply(ok, function(w) w$name, character(1)), collapse=", ")
              else "NONE - no eligible worker!"
      if (!length(ok))
        warns <- c(warns, sprintf("task '%s' has NO eligible worker for requires=%s.",
                   tid, format_resources(req)))
    } else elig <- "(pass worker_list to see eligibility)"
    req_str <- if (length(req)) sprintf(" (requires %s)", format_resources(req)) else ""

    over <- if (length(wc_vals))
      sprintf(" (%s)", paste(sprintf("%s: %s", names(wc_vals),
              vapply(wc_vals, function(v) paste(v, collapse=","), character(1))), collapse="; ")) else ""
    inst <- sprintf("%d instance%s%s", n, if (n != 1L) "s" else "", over)
    if (length(sc$runtime_exp))
      ## One axis per dynamic wildcard, each in its own braces. No noun: the axes
      ## can be of mixed kind (a file dep and an object dep on the same task),
      ## so no single one is correct, and the separator stays neutral because
      ## nothing here states how the axes combine.
      inst <- paste0(inst, sprintf("  [+ runtime: expands per %s]",
                                   paste0("{{", sc$runtime_exp, "}}", collapse = ", ")))
    cat(sprintf("TASK '%s' [%s] - %s\n", tid, type, inst))
    cat(sprintf("  workers: %s%s\n", elig, req_str))
    if (length(sc$inputs)) {
      cat("  inputs:\n")
      ## A gather in a task cloned over static wildcards is a PARTIAL aggregation: one
      ## gather per clone, not one for the whole workflow, a difference of N instances
      ## against 1. The "partial_aggregation" class cannot be read here, since
      ## expand_with_partial_aggregation() strips it once the by-wildcard resolves to a
      ## concrete value, and rightly so: at that point the parameter IS a full gather
      ## within its group. The post-expansion facts say the same thing, that this task
      ## has clones and each carries its own value of these wildcards.
      ##
      ## An aggregated template that repeats a wildcard emits each resolved path once
      ## PER OCCURRENCE, so "count_{s}/velocyto/count_{s}.loom" over three samples
      ## carries nine entries for three files. select_worker() in
      ## R/scheduler_dispatch.R compensates with unique(), and without the same
      ## compensation here the surface prints nine lines, which reads as if the task
      ## received each file three times.
      ##
      ## The patterns are UNLISTED, not indexed: `pattern` is as.character(inner) and
      ## IS the whole vector, so taking [1] would drop everything after the first,
      ## including the distinct entries of new_file(c("a.fa","b.fa")).
      k_in <- vapply(sc$inputs, function(ip)
                paste(ip$name, ip$producer, ip$mode, sep = "\x1f"), character(1))
      for (k in unique(k_in)) {
        gr     <- sc$inputs[k_in == k]
        ip     <- gr[[1]]
        pats   <- unique(unlist(lapply(gr, function(x) as.character(x$pattern))))
        prod_s <- if (is.na(ip$producer)) "(direct)" else ip$producer
        mode_s <- if (identical(ip$mode, "aggr") && length(wc_vals))
                    paste0("aggr per ", paste(names(wc_vals), collapse = ","))
                  else ip$mode
        more   <- if (length(pats) > 1L)
                    sprintf("  (+%d more, %d in total)", length(pats) - 1L, length(pats)) else ""
        cat(sprintf("    %-12s <- %-10s [%s]  %s%s\n",
                    ip$name, prod_s, mode_s, desubst(pats[1]), more))
      }
    }
    if (length(sc$outputs)) cat(sprintf("  outputs: %s\n", paste(unique(vapply(sc$outputs, desubst, character(1))), collapse=" ; ")))
    cat("\n")
    out_tasks[[tid]] <- list(type = type, n_instances = n, eligible = elig,
                             inputs = sc$inputs, outputs = sc$outputs, runtime_exp = sc$runtime_exp)
  }

  ## run_workflow() refuses duplicate names in validate_exec_args(); report them
  ## here so the preview does not show a plan the run would reject. tryCatch:
  ## worker_list is not class-checked here, and the preview must not raise.
  if (!is.null(worker_list)) {
    dups <- tryCatch(duplicate_worker_names(worker_list), error = function(e) character(0))
    if (length(dups))
      warns <- c(warns, sprintf("duplicate worker name(s): %s. run_workflow() will refuse this worker_list",
                                paste(shQuote(dups), collapse = ", ")))
  }

  cat(strrep("-", 64), "\n")
  warns <- unique(warns)
  if (length(warns)) { cat("WARNINGS:\n"); for (w in warns) cat("  [!]", w, "\n") }
  else cat("WARNINGS: none\n")
  cat("\n")

  ## Network plan: same setup + validation as run_workflow(), surfaced UPFRONT so
  ## the user sees the port ranges + firewall requirements (and any "widen ports"
  ## / overlap error) BEFORE launching. Non-fatal here -- a setup error is
  ## reported, not raised, so the preview still returns. setup_remote_worker_tcp()
  ## returns a fresh list, so the caller's worker_list is not mutated.
  if (!is.null(worker_list)) {
    if (is.null(master_host)) master_host <- detect_self_host()
    tryCatch(
      print_port_plan(setup_remote_worker_tcp(worker_list, master_host, ports)),
      error = function(e) cat(sprintf("[flumen] port plan unavailable - %s\n", conditionMessage(e)))
    )
    cat("\n")
  }

  invisible(list(tasks = out_tasks, warnings = warns))
}
