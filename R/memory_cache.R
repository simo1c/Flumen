## In-process result cache for the supervisor: a small RAM tier (LRU-evicted)
## backed by a disk-spill tier. Holds task outputs so co-located dependents and
## P2P peers can be served without re-running the producer. See new_memory_cache();
## used by the supervisor process (inst/scripts/supervisor.R).
##
## On-disk file format (per cache file):
##   [4B n_elems] ( [1B is_raw][1B is_arrow]
##                  [1B dt_len][dt_bytes]
##                  [8B dn_len][dn_bytes = qs2(elem_name)]
##                  [8B gn_len][gn_bytes = qs2(output_par_name)]
##                  [8B data_len][data_bytes] )*n
##   is_raw    -> HOW the bytes were produced: 1 when the result was already a
##                raw vector and is stored as-is, 0 when it was qs2-serialized.
##   is_arrow  -> how to DECODE them, Arrow or qs2. Orthogonal to is_raw and
##                stored separately: a raw result is written as-is (is_raw=1)
##                while carrying is_arrow=0, so conflating the two makes the
##                consumer decode qs2 bytes as Arrow.
##   elem_name -> per-element output name; qs2-serialized so NULL / "" / character
##                all round-trip faithfully (a plain string layout could not tell
##                NULL from "").
##   output_par_name -> which DECLARED OUTPUT the element belongs to. Distinct from
##                elem_name: one output can own several elements, so this repeats
##                across them while elem_name names each one. Same qs2 treatment
##                for the same NULL-vs-"" reason.
## The disk index (state$disk_sizes) lives in memory and starts empty in every
## worker process, so a cache file is only ever read back by the process that
## wrote it and the format needs no versioning. The directory defaults to
## tempdir() and is set per worker by new_worker(cache_dir = ).

write_result_to_disk <- function(value, path) {
  con <- file(path, "wb")
  on.exit(close(con))
  writeBin(as.integer(length(value)), con, size = 4L)
  for (el in value) {
    is_raw  <- is.raw(el$result)
    res_raw <- if (is_raw) el$result else qs2::qs_serialize(el$result)
    dt_raw  <- charToRaw(el$data_type)
    dn_raw  <- qs2::qs_serialize(el$elem_name)
    gn_raw  <- qs2::qs_serialize(el$output_par_name)
    writeBin(as.raw(as.integer(is_raw)), con)
    writeBin(as.raw(as.integer(isTRUE(el$is_arrow))), con)
    writeBin(as.raw(length(dt_raw)), con)
    writeBin(dt_raw, con)
    writeBin(as.double(length(dn_raw)), con, size = 8L)
    writeBin(dn_raw, con)
    writeBin(as.double(length(gn_raw)), con, size = 8L)
    writeBin(gn_raw, con)
    writeBin(as.double(length(res_raw)), con, size = 8L)
    writeBin(res_raw, con)
  }
}

read_result_from_disk <- function(path) {
  con <- file(path, "rb")
  on.exit(close(con))
  n <- readBin(con, "integer", size = 4L)
  results <- vector("list", n)
  for (i in seq_len(n)) {
    is_raw   <- as.integer(readBin(con, "raw", n = 1L)) == 1L
    is_arrow <- as.integer(readBin(con, "raw", n = 1L)) == 1L
    dt_len   <- as.integer(readBin(con, "raw", n = 1L))
    dt       <- rawToChar(readBin(con, "raw", n = dt_len))
    dn_len   <- readBin(con, "double", size = 8L)
    dn       <- qs2::qs_deserialize(readBin(con, "raw", n = dn_len))
    gn_len   <- readBin(con, "double", size = 8L)
    gn       <- qs2::qs_deserialize(readBin(con, "raw", n = gn_len))
    res_len  <- readBin(con, "double", size = 8L)
    res_raw  <- readBin(con, "raw", n = res_len)
    result   <- if (is_raw) res_raw else qs2::qs_deserialize(res_raw)
    results[[i]] <- list(result = result, data_type = dt,
                         is_arrow = is_arrow, elem_name = dn,
                         output_par_name = gn)
  }
  class(results) <- "result_object"
  results
}

## -- LRU memory cache ----------------------------------------------------------

new_memory_cache <- function(max_ram  = 4096L  * 1e6,
                              max_disk = 51200L * 1e6,
                              dir      = tempdir()) {
  state                <- new.env(parent = emptyenv())
  state$ram            <- list()
  state$ram_sizes      <- numeric(0)
  state$ram_used       <- 0
  state$disk_sizes     <- numeric(0)
  state$disk_used      <- 0
  state$last_access    <- integer(0)
  state$access_counter <- 0L
  state$disk_dir       <- file.path(dir, "result_cache")
  dir.create(state$disk_dir, showWarnings = FALSE, recursive = TRUE)

  cache <- list(max_ram = max_ram, max_disk = max_disk, state = state)
  class(cache) <- "memory_cache_object"
  cache
}

cache_disk_path <- function(cache, key)
  file.path(cache$state$disk_dir, paste0(key, ".bin"))

cache_evict_lru <- function(cache, needed) {
  s <- cache$state
  while (s$ram_used + needed > cache$max_ram && length(s$ram) > 0) {
    lru_key <- names(which.min(s$last_access[names(s$ram)]))
    path    <- cache_disk_path(cache, lru_key)
    sz      <- s$ram_sizes[[lru_key]]
    if (!(lru_key %in% names(s$disk_sizes))) {
      if (s$disk_used + sz > cache$max_disk)
        stop(paste0("Disk cache limit exceeded (", cache$max_disk / 1e6,
                    " MB). Increase cache_disk_limit in new_worker()."))
      write_result_to_disk(s$ram[[lru_key]], path)
      s$disk_sizes[[lru_key]] <- sz
      s$disk_used             <- s$disk_used + sz
    }
    s$ram[[lru_key]]        <- NULL
    s$ram_sizes             <- s$ram_sizes[names(s$ram_sizes) != lru_key]
    s$ram_used              <- s$ram_used - sz
    s$last_access           <- s$last_access[names(s$last_access) != lru_key]
  }
}

cache_set <- function(cache, key, value) {
  s  <- cache$state
  ## Only raw payloads count toward the cache budget. The one non-raw branch is
  ## results whose data_type is "file"/"file_path", decoded on the receive path
  ## (R/supervisor_deps.R::get_result_from_msg). Those hold the resolved PATH LIST,
  ## not the file contents, so they are treated as free rather than paid for with an
  ## object.size() traversal on every set. If a non-raw branch ever caches real data,
  ## revisit this accounting: such an entry would never trigger eviction and would
  ## sit in RAM outside cache_ram_limit.
  sz <- sum(vapply(value, function(el) {
    if (is.raw(el$result)) as.numeric(length(el$result)) else 0
  }, numeric(1)))
  ## Reconcile a pre-existing entry BEFORE (re)inserting: drop its old size and old
  ## copy so ram_used and disk_used do not drift upward on an overwrite, which
  ## happens when the mesh re-caches a re-received holder output under the same key.
  ## Without this the counters inflate on every repeat, causing a premature spill or
  ## a false "Disk cache limit exceeded" stop. A key lives in at most one tier in the
  ## normal flow, but both branches are cleaned defensively.
  if (key %in% names(s$ram_sizes)) {
    s$ram_used    <- s$ram_used - s$ram_sizes[[key]]
    s$ram[[key]]  <- NULL
    s$ram_sizes   <- s$ram_sizes[names(s$ram_sizes) != key]
    s$last_access <- s$last_access[names(s$last_access) != key]
  }
  if (key %in% names(s$disk_sizes)) {
    s$disk_used  <- s$disk_used - s$disk_sizes[[key]]
    s$disk_sizes <- s$disk_sizes[names(s$disk_sizes) != key]
    unlink(cache_disk_path(cache, key))
  }
  if (s$ram_used + sz > cache$max_ram) cache_evict_lru(cache, sz)
  s$access_counter <- s$access_counter + 1L
  if (s$ram_used + sz <= cache$max_ram) {
    s$ram[[key]]         <- value
    s$ram_sizes[[key]]   <- sz
    s$ram_used           <- s$ram_used + sz
    s$last_access[[key]] <- s$access_counter
  } else {
    if (s$disk_used + sz > cache$max_disk)
      stop(paste0("Disk cache limit exceeded (", cache$max_disk / 1e6,
                  " MB). Increase cache_disk_limit in new_worker()."))
    write_result_to_disk(value, cache_disk_path(cache, key))
    s$disk_sizes[[key]] <- sz
    s$disk_used         <- s$disk_used + sz
  }
}

cache_get <- function(cache, key) {
  s   <- cache$state
  val <- s$ram[[key]]
  if (!is.null(val)) {
    s$access_counter     <- s$access_counter + 1L
    s$last_access[[key]] <- s$access_counter
    return(val)
  }
  if (key %in% names(s$disk_sizes)) {
    s$access_counter     <- s$access_counter + 1L
    s$last_access[[key]] <- s$access_counter
    needed <- s$disk_sizes[[key]]
    if (s$ram_used + needed > cache$max_ram) cache_evict_lru(cache, needed)
    path <- cache_disk_path(cache, key)
    val  <- read_result_from_disk(path)
    ## Reclaim the on-disk copy ONLY when the entry fits in RAM: the re-promoted RAM
    ## copy becomes the survivor, so deleting the file is safe. An entry larger than
    ## max_ram cannot be promoted, so its disk copy stays, a non-destructive read, for
    ## later co-local or P2P consumers. Otherwise the first read would destroy the
    ## only copy and the next consumer would get NULL, crashing in
    ## get_result_instance. The retained file is reclaimed by cache_reset() at
    ## workflow end.
    if (s$ram_used + needed <= cache$max_ram) {
      file.remove(path)
      s$disk_sizes         <- s$disk_sizes[names(s$disk_sizes) != key]
      s$disk_used          <- s$disk_used - needed
      s$ram[[key]]         <- val
      s$ram_sizes[[key]]   <- needed
      s$ram_used           <- s$ram_used + needed
      s$last_access[[key]] <- s$access_counter
    }
    return(val)
  }
  NULL
}

cache_reset <- function(cache) {
  s <- cache$state
  unlink(s$disk_dir, recursive = TRUE)
  s$ram            <- list()
  s$ram_sizes      <- numeric(0)
  s$ram_used       <- 0
  s$disk_sizes     <- numeric(0)
  s$disk_used      <- 0
  s$last_access    <- integer(0)
  s$access_counter <- 0L
}
