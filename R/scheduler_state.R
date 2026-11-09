## The scheduler state object (sched_state): dependency counts, the data->workers
## reverse index, file-locality bookkeeping, the task-status hash env, and the helpers
## that mutate them as tasks finish. Used by the central scheduler (R/scheduler_run.R).

build_scheduler_state <- function(workflow) {
  dep_map <- workflow$required_shapes
  dep_remaining <- sapply(dep_map, length)
  dependents_of <- list()
  for (child in names(dep_map)) {
    for (parent in dep_map[[child]]) {
      dependents_of[[parent]] <- c(dependents_of[[parent]], child)
    }
  }
  ready_queue <- names(dep_remaining[dep_remaining == 0])
  list(
       ## dep_map is the dependency relation; dep_remaining, dependents_of and
       ## ready_queue are views DERIVED from it and from which instances have
       ## produced. Mutated only through the primitives below, which keep the
       ## four in step. A plain list, not an environment: sched_state is passed
       ## and returned by value.
       deps = list(dep_map = dep_map, dep_remaining = dep_remaining,
                   dependents_of = dependents_of, ready_queue = ready_queue),
       worker_data = list(),
       ## Reverse index of worker_data: data_to_workers[[exe_id]] = c(worker, ...)
       ## Maintained by on_task_finished(), read by score_workers_for_task().
       data_to_workers = new.env(hash = TRUE, parent = emptyenv()),
       ## Per-worker file state. Each entry: list(in_transit, completed).
       ## Populated when files are dispatched (in_transit) and by the supervisor's
       ## `file_received` event (completed).
       worker_files = list(),
       ## Holder index: holders[["exe_id#out_instance"]] = c(worker, ...), the
       ## workers that received a COMPLETE copy of that output via P2P and can
       ## re-serve it. Written by the `data_received` event, pruned on worker crash.
       holders = new.env(hash = TRUE, parent = emptyenv()),
       ## Least-connections routing (see mesh_serve_begin/mesh_serve_end).
       ## serving_pending is the source of truth:
       ##   serving_pending[["consumer|holder_key|exe_id|k"]] =
       ##       list(source, consumer, holder_key, exe_id)   -- one per in-flight serve.
       ## serving_count[[worker]] is a pure MIRROR of it, mutated only by those two.
       serving_count   = new.env(hash = TRUE, parent = emptyenv()),
       serving_pending = new.env(hash = TRUE, parent = emptyenv()))
}

## ---------------------------------------------------------------------------
## The dependency relation is mutated ONLY through the five primitives below.
##
## `deps` holds one fact, child -> parents, and three views that follow from it:
## how many of a child's parents are still outstanding, which children a parent
## feeds, and which children have none left. The relation and its views move
## together by construction: there is no way to reach one without the others.
##
## Every primitive takes and RETURNS sched_state, because sched_state is a list
## and R passes it by value.

## A child leaves the graph: detach it from its parents, drop its row and its
## back-reference. Both guards are kept: a replaced id may have a dep_map entry,
## a dependents_of entry, or neither.
deps_unlink <- function(sched_state, child) {
  if (child %in% names(sched_state$deps$dep_map)) {
    for (parent in sched_state$deps$dep_map[[child]]) {
      sched_state$deps$dependents_of[[parent]] <-
        setdiff(sched_state$deps$dependents_of[[parent]], child)
    }
    sched_state$deps$dep_map[[child]] <- NULL
    sched_state$deps$dep_remaining <-
      sched_state$deps$dep_remaining[names(sched_state$deps$dep_remaining) != child]
    sched_state$deps$ready_queue <- setdiff(sched_state$deps$ready_queue, child)
  }
  ## Drop the child's own stale back-reference: without it,
  ## dependents_of[B_1] = "C" survives B_1 -> B_1_1.
  if (child %in% names(sched_state$deps$dependents_of)) {
    sched_state$deps$dependents_of[[child]] <- NULL
  }
  sched_state
}

## A child acquires or changes its parent set. `finished_ids` says which
## instances have already produced, so the outstanding count is right even when
## some parents finished before the child existed.
##
## Only a BRAND NEW child that already has everything goes straight into the
## queue. An existing one does not: it may have been dispatched already, and
## enqueueing it again would run it twice.
deps_link <- function(sched_state, child, parents, finished_ids) {
  old_parents <- sched_state$deps$dep_map[[child]]
  is_new      <- is.null(old_parents)

  if (!is_new) {
    for (p in setdiff(old_parents, parents)) {
      sched_state$deps$dependents_of[[p]] <-
        setdiff(sched_state$deps$dependents_of[[p]], child)
    }
  }
  sched_state$deps$dep_map[[child]] <- parents
  already_done <- if (length(parents) > 0) sum(parents %in% finished_ids) else 0L
  remaining    <- length(parents) - already_done
  sched_state$deps$dep_remaining[[child]] <- remaining
  for (parent in if (is_new) parents else setdiff(parents, old_parents)) {
    sched_state$deps$dependents_of[[parent]] <-
      unique(c(sched_state$deps$dependents_of[[parent]], child))
  }
  if (is_new && remaining == 0) {
    sched_state$deps$ready_queue <- c(sched_state$deps$ready_queue, child)
  }
  sched_state
}

## A producer instance has produced: every child of it is one parent closer to
## eligible, and a child that has just now reached zero joins the queue.
##
## `already_counted` makes this AT MOST ONCE per instance, so a repeat -- a
## re-dispatch after a lost EXECUTE_TASK -- does not move every child twice.
##
## The count is a DECREMENT, and the guard sits at the ENTRY rather than per
## child, for cost: recomputing the child's parent count on every notify is
## quadratic in the fan-in over a run.
##
## The ENQUEUE is an event, hence `== 0` and not `<= 0`: a child put back on the
## state rather than on the transition would return to the queue after the
## dispatcher popped it, and be dispatched twice.
deps_mark_done <- function(sched_state, exe_id, already_counted) {
  if (already_counted) return(sched_state)
  for (child in sched_state$deps$dependents_of[[exe_id]]) {
    sched_state$deps$dep_remaining[[child]] <-
      sched_state$deps$dep_remaining[[child]] - 1
    if (sched_state$deps$dep_remaining[[child]] == 0)
      sched_state$deps$ready_queue <- c(sched_state$deps$ready_queue, child)
  }
  sched_state
}

## The dispatcher removes the child it has chosen from the queue, and puts it
## back when it turns out it cannot be sent after all. Returns both the new
## state and the id, since one call does both.
deps_take <- function(sched_state, idx) {
  exe_id <- sched_state$deps$ready_queue[idx]
  sched_state$deps$ready_queue <- sched_state$deps$ready_queue[-idx]
  list(sched_state = sched_state, exe_id = exe_id)
}

deps_put_back <- function(sched_state, exe_id) {
  sched_state$deps$ready_queue <- c(sched_state$deps$ready_queue, exe_id)
  sched_state
}

## Least-connections routing: choose the fetch source for an output_req and rewrite
## its transport triple. If no peer holds (exe_id, out_instance) the request is left
## UNCHANGED, still pointing at the producer. Otherwise pick, among {producer} +
## received holders (alive, never the consumer's own worker), the one currently
## serving the FEWEST transfers (first on tie, deterministic). The chosen worker's
## socket/uses_tcp/is_local all come from the SAME supervisor_info row, so the
## transport triple stays coherent. Bumps serving_count and records the source for
## later decrement.
mesh_reroute_output_req <- function(req, consumer_worker, sched_state, supervisor_info,
                                    consuming_exe_id = NA_character_) {
  if (is.null(sched_state) || is.null(supervisor_info)) return(req)
  exe_id <- req$exe_id
  if (is.null(exe_id) || is.na(exe_id)) return(req)
  out_str    <- if (is.null(req$output_instance)) "" else as.character(req$output_instance)
  holder_key <- paste0(exe_id, "#", out_str)

  received <- sched_state$holders[[holder_key]]
  if (length(received) == 0L) return(req)                 # no peer holder -> producer (unchanged)
  ## A consumer on the producer's own worker reads the producer's copy, as it
  ## does when nobody holds one. Rerouted, it would fetch its own output back
  ## from a peer, the bytes landing at the path the peer gave.
  if (consumer_worker %in% sched_state$data_to_workers[[exe_id]]) return(req)

  candidates <- unique(c(sched_state$data_to_workers[[exe_id]], received))
  candidates <- intersect(candidates, supervisor_info$Supervisor_ID)
  candidates <- setdiff(candidates, consumer_worker)      # never fetch from our own worker
  if (length(candidates) == 0L) return(req)

  sc     <- vapply(candidates, function(w) {
    v <- sched_state$serving_count[[w]]; if (is.null(v)) 0L else as.integer(v)
  }, integer(1))
  chosen <- candidates[which.min(sc)]                     # least-connections; which.min -> first on tie

  row <- supervisor_info[supervisor_info$Supervisor_ID == chosen, , drop = FALSE]
  if (nrow(row) == 0L) return(req)
  req$socket   <- row$Socket_data_req
  req$uses_tcp <- isTRUE(row$Uses_tcp)
  ## `source` / `fetch` are written afterwards by declare_constituent()
  ## (R/scheduler_dispatch.R), from this same row.

  mesh_serve_begin(sched_state, chosen, consumer_worker, holder_key, consuming_exe_id)
  if (!(chosen %in% sched_state$data_to_workers[[exe_id]])) {
    ## serving_count printed so a leak (monotone growth across a run) is visible
    ## directly in the scheduler log.
    to_log(paste0("[MESH] rerouted fetch of ", holder_key, " by ", consumer_worker,
                  " to peer holder ", chosen, " (serving_count=",
                  sched_state$serving_count[[chosen]], ")"), "Central scheduler")
  }
  req
}

## Least-connections bookkeeping is an AUTHORITATIVE SET, not an optimistic counter:
## serving_pending is the single source of truth (one entry per in-flight serve) and
## serving_count is a pure MIRROR of it. Both are mutated ONLY by the two primitives
## below, so the count cannot drift independently of the pending set.
##
## mesh_serve_begin: record an in-flight serve from `source` to `consumer` for
## `holder_key`, triggered by `consuming_exe_id`. The pending key is made unique
## (…|k) so N coalesced fetches of the same (consumer, holder_key, exe_id) each get
## their own entry instead of overwriting one another. +1 on the source's mirror.
mesh_serve_begin <- function(sched_state, source, consumer, holder_key, consuming_exe_id = NA_character_) {
  eid    <- if (is.null(consuming_exe_id) || is.na(consuming_exe_id)) "" else as.character(consuming_exe_id)
  prefix <- paste0(consumer, "|", holder_key, "|", eid, "|")
  k      <- 1L + sum(startsWith(ls(sched_state$serving_pending), prefix))
  sched_state$serving_pending[[paste0(prefix, k)]] <-
    list(source = source, consumer = consumer, holder_key = holder_key, exe_id = eid)
  cur <- sched_state$serving_count[[source]]
  sched_state$serving_count[[source]] <- (if (is.null(cur)) 0L else as.integer(cur)) + 1L
  invisible(sched_state)
}

## mesh_serve_end: end every in-flight serve matching `pred(entry)` -- remove the
## pending entry and -1 its source's mirror (floored at 0). Idempotent: an
## already-removed serve is a no-op, so overlapping triggers never
## double-decrement.
mesh_serve_end <- function(sched_state, pred) {
  for (key in ls(sched_state$serving_pending)) {
    ent <- sched_state$serving_pending[[key]]
    if (isTRUE(pred(ent))) {
      cur <- sched_state$serving_count[[ent$source]]
      if (!is.null(cur)) sched_state$serving_count[[ent$source]] <- max(0L, as.integer(cur) - 1L)
      rm(list = key, envir = sched_state$serving_pending)
    }
  }
  sched_state
}

## Trigger 1 -- early release: the transfer of `holder_key` to `consumer_worker`
## completed.
## A single P2P transfer satisfies ALL that worker's coalesced waiters on that output, so
## every matching entry is released at once. Wire-carried via data_received.
mesh_release_serve <- function(sched_state, consumer_worker, holder_key) {
  mesh_serve_end(sched_state, function(e)
    identical(e$consumer, consumer_worker) && identical(e$holder_key, holder_key))
}

## Trigger 2 -- backstop: the consuming task `consuming_exe_id` finished (RESULT), so all
## its fetches are concluded however each was resolved (transfer, cache, colocal,
## coalesced). Keyed by the SPECIFIC exe_id -- NOT the worker -- so a sibling task still
## running on the same worker keeps its own in-flight serves counted.
mesh_release_by_exe_id <- function(sched_state, consuming_exe_id) {
  if (is.null(consuming_exe_id) || is.na(consuming_exe_id)) return(sched_state)
  eid <- as.character(consuming_exe_id)
  mesh_serve_end(sched_state, function(e) identical(e$exe_id, eid))
}

## Trigger 3 -- crash cleanup: `worker` died. Release every serve it was the SOURCE of or
## the CONSUMER of, and drop its mirror entry.
drop_serving_worker <- function(sched_state, worker) {
  sched_state <- mesh_serve_end(sched_state, function(e)
    identical(e$source, worker) || identical(e$consumer, worker))
  if (!is.null(sched_state$serving_count[[worker]]))
    rm(list = worker, envir = sched_state$serving_count)
  sched_state
}

## Holder index: record that `worker` holds a complete copy of (exe_id, out_instance).
## out_instance_str is the wire string ("" = whole/aggregated output); the key format
## matches the supervisor's composite cache key.
mark_holder <- function(sched_state, exe_id, out_instance_str, worker) {
  key <- paste0(exe_id, "#", out_instance_str)
  sched_state$holders[[key]] <- unique(c(sched_state$holders[[key]], worker))
  sched_state
}

## Remove `worker` from every holder-index entry (called on worker crash, alongside
## drop_worker_files, so the routing never sends a fetch to a dead holder).
drop_holder_worker <- function(sched_state, worker) {
  for (key in ls(sched_state$holders)) {
    v <- setdiff(sched_state$holders[[key]], worker)
    if (length(v)) sched_state$holders[[key]] <- v
    else rm(list = key, envir = sched_state$holders)
  }
  sched_state
}

## Return the file-state record for `worker`, materialising an empty one if the
## worker has not been seen yet, so callers can treat entries as always-present.
get_worker_files <- function(sched_state, worker) {
  wf <- sched_state$worker_files[[worker]]
  if (is.null(wf)) {
    wf <- list(in_transit = character(0),
               completed  = character(0))
  }
  wf
}

## Mark `files` as currently being transferred to `worker`. Called at dispatch
## time in try_schedule for every chunked file the EXECUTE_TASK will carry. Files
## already in `completed` stay there too; file_received clears in_transit.
mark_files_in_transit <- function(sched_state, worker, files) {
  if (length(files) == 0) return(sched_state)
  wf <- get_worker_files(sched_state, worker)
  wf$in_transit <- union(wf$in_transit, files)
  sched_state$worker_files[[worker]] <- wf
  sched_state
}

## Atomic transition from in_transit to completed. Triggered by the supervisor's
## file_received notification once write_chunk_to_file has merged the last chunk.
mark_files_completed <- function(sched_state, worker, files) {
  if (length(files) == 0) return(sched_state)
  wf <- get_worker_files(sched_state, worker)
  wf$in_transit <- setdiff(wf$in_transit, files)
  wf$completed  <- union(wf$completed, files)
  sched_state$worker_files[[worker]] <- wf
  sched_state
}

## Drop a worker entirely from worker_files. Invoked on error_notify, so tasks
## pending on files in_transit toward a dead worker get a fresh chance on
## alternative workers at the next try_schedule.
drop_worker_files <- function(sched_state, worker) {
  if (worker %in% names(sched_state$worker_files)) {
    sched_state$worker_files[[worker]] <- NULL
  }
  sched_state
}

## Best-effort extraction of input-file paths needed by a task. Used to filter
## workers in select_worker (skip those with the file in_transit) and to mark files
## in_transit at dispatch time. Recognises file/folder classes on either the par
## object or its $value slot. Unknown shapes return empty: no filtering and no
## tracking for that task.
get_task_input_file_paths <- function(task, par_locations = NULL, shared_path = NULL) {
  if (is.null(task$par_list)) return(character(0))
  file_classes <- c("file_object", "file_path")
  paths <- character(0)
  for (par in task$par_list) {
    ## Skip PRE-STAGED inputs (pre_staged(): par_locations[[par_uuid]] == FALSE).
    ## They are already present on every worker and are NEVER transferred, so
    ## they must not enter the in_transit machinery. par_locations only stores
    ## pre-staged entries (value FALSE); absence -> tracked normally.
    if (!is.null(par_locations) && !is.null(par$par_uuid) &&
        isFALSE(par_locations[[par$par_uuid]])) next
    ## Skip OUTPUT params: their files are produced by the task itself, never
    ## transferred from elsewhere, so nothing would ever clear them.
    if ("out_par_object" %in% class(par)) next
    val <- if (!is.null(par$value)) par$value else par
    cls <- class(val)
    if (any(file_classes %in% cls)) {
      p <- param_value_paths(val)
      ## Skip SHARED-FILESYSTEM inputs (run_workflow(shared_path=)): read in
      ## place, never transferred, as with the pre_staged skip above.
      ## All-or-nothing on multi-path params, matching the execute_single_task
      ## shared_path downgrade.
      if (!is.null(shared_path)) {
        pnz <- p[nzchar(p)]
        if (length(pnz) > 0 &&
            all(vapply(pnz, is_under_shared_path, logical(1), shared_path))) next
      }
      paths <- c(paths, p)
    }
  }
  unique(paths[nzchar(paths)])
}

update_scheduler_state <- function(sched_state, new_required_shapes, replace_op = NULL) {
  # Remove genuinely replaced ops (skip ops that map to themselves).
  # Cleanup is symmetric: drop the old_id's dep_map / dep_remaining / ready_queue
  # entries AND its dependents_of entry, and remove old_id from each parent's
  # dependents_of list.
  if (!is.null(replace_op)) {
    for (old_id in names(replace_op)) {
      new_ids <- replace_op[[old_id]]
      if (length(new_ids) == 1 && new_ids == old_id) next
      sched_state <- deps_unlink(sched_state, old_id)
    }
  }

  # Sync dep_map for ALL tasks present in new_required_shapes — not just
  # brand-new ones. EXISTING tasks (e.g. aggregate fan-in like load_counts)
  # may have their dep_map rewired when their parents are runtime-renamed,
  # even if they themselves were not renamed.
  finished_ids <- unlist(sched_state$worker_data, use.names = FALSE)
  ## By POSITION, as in build_required_by_index(): reaching an element by name
  ## scans the names, and this loop runs over all of them on every result.
  nm <- names(new_required_shapes)
  for (i in seq_along(new_required_shapes)) {
    child <- nm[i]
    parents <- new_required_shapes[[i]]
    old_parents <- sched_state$deps$dep_map[[child]]
    changed <- is.null(old_parents) || !setequal(old_parents, parents)
    if (!changed) next   # idempotent skip for unchanged tasks
    sched_state <- deps_link(sched_state, child, parents, finished_ids)
  }
  sched_state
}

on_task_finished <- function(sched_state, res_notify) {
  exe_id <- res_notify$exe_id
  worker <- res_notify$Supervisor_ID
  ## Read before the two indices below are written: afterwards every arrival
  ## looks like the first one.
  already_counted <- !is.null(sched_state$data_to_workers[[exe_id]])
  sched_state$worker_data[[worker]] <- c(sched_state$worker_data[[worker]], exe_id)
  ## Maintain reverse index: which workers have produced exe_id? unique() guards
  ## against a retry logging the same worker twice for the same exe_id.
  sched_state$data_to_workers[[exe_id]] <- unique(c(
    sched_state$data_to_workers[[exe_id]], worker
  ))

  ## The path -> producer pairing of a static gather is written at the producer's
  ## notify (bind_static_gather_constituents).

  ## Counted AT MOST ONCE per producer instance. `already_counted` is read BEFORE
  ## the two indices above are written, so it says whether this instance's
  ## completion had already entered the system, and a repeat skips the whole step.
  sched_state <- deps_mark_done(sched_state, exe_id, already_counted)

  sched_state
}

get_dep_exe_ids_from_task <- function(op) {
  dep_exe_ids <- character(0)
  for (par in op$par_list) {
    val <- if ("dep_object" %in% class(par$value)) par$value$value else par$value
    ## A resolved dependency is a list of constituents (as_output_req_list).
    if (inherits(val, "output_req_list")) {
      for (req in val) {
        dep_exe_ids <- c(dep_exe_ids, req$exe_id)
      }
    }
  }
  unique(dep_exe_ids)
}


## Build a hash environment from a task_table data.frame: maps Exe_ID -> Status,
## for O(1) status lookups and updates. The data.frame is kept in sync for any
## bulk read; per-event mutation goes through `set_task_status()`.
build_task_status_env <- function(task_table){
  env <- new.env(hash = TRUE, parent = emptyenv())
  if(nrow(task_table) > 0L){
    ids      <- as.character(task_table$Exe_ID)
    statuses <- as.character(task_table$Status)
    for(k in seq_along(ids)){
      env[[ids[k]]] <- statuses[k]
    }
  }
  env
}

## Compute locality scores for a set of candidate workers given a list of
## dependency exe_ids and a reverse index data_to_workers (exe_id -> workers
## that have produced/cached it). Returns a named integer vector of length
## length(unique(candidate_workers)), score per unique worker.
##
## The reverse index is what makes this O(D): scoring from worker_data instead
## would scan O(W * D * |worker_data[w]|).
score_workers_for_task <- function(dep_ids, candidate_workers, data_to_workers){
  unique_w <- unique(candidate_workers)
  if(length(unique_w) == 0L){
    return(setNames(integer(0), character(0)))
  }
  if(length(dep_ids) == 0L){
    return(setNames(integer(length(unique_w)), unique_w))
  }
  hit_workers <- character(0)
  for(d in dep_ids){
    workers_with_d <- data_to_workers[[d]]
    if(!is.null(workers_with_d)){
      hit_workers <- c(hit_workers, intersect(workers_with_d, unique_w))
    }
  }
  scores <- setNames(integer(length(unique_w)), unique_w)
  if(length(hit_workers) > 0L){
    counts <- table(hit_workers)
    scores[names(counts)] <- as.integer(counts)
  }
  scores
}

## Build a reverse index for workflow$required_shapes:
##   required_shapes[[child_exe_id]] = c(parent_exe_id_1, parent_exe_id_2, ...)
## becomes
##   required_by[[parent_exe_id]] = c(child_exe_id_1, child_exe_id_2, ...)
## Used by process_result_notify_and_update_workflow() to find which tasks need
## to react to a parent's completion. Walks by POSITION, not by name.
build_required_by_index <- function(required_shapes){
  env <- new.env(hash = TRUE, parent = emptyenv())
  nm  <- names(required_shapes)
  for(i in seq_along(required_shapes)){
    child <- nm[i]
    for(p in required_shapes[[i]]){
      env[[p]] <- c(env[[p]], child)
    }
  }
  env
}

## Atomically update both the data.frame and the hash index for an exe_id.
## Returns the updated data.frame; the env is mutated in place. Uses match()
## for the data.frame write, a single vectorised pass.
set_task_status <- function(task_table, env, exe_id, value){
  env[[exe_id]] <- value
  idx <- match(exe_id, task_table$Exe_ID)
  if(!is.na(idx)) task_table$Status[idx] <- value
  ## Note the row so persist_task_table() can write it out alone instead of
  ## re-serialising the whole table.
  mark_task_row_dirty(env, exe_id)
  task_table
}
