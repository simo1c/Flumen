## Group O — Supervisor result cache (RAM + disk-spill tiers).
## Regression guard for the read-once disk-destruction bug (June 2026):
## cache_get() unconditionally file.remove()d a disk-resident entry after
## reading it. Entries that fit in RAM are re-promoted (the RAM copy survives),
## but an entry LARGER than max_ram cannot be promoted -- so the first read
## destroyed the only copy and the next co-local / P2P consumer got NULL
## (crash on get_result_instance). Fixed by reclaiming the disk copy only when
## the entry is promoted back to RAM; over-RAM entries keep their disk copy and
## are reclaimed by cache_reset() at workflow end. Pure in-process unit tests.

## Minimal result_object holding one raw element of `n` bytes (cache_set sizes
## an entry by the length of its raw elements; non-raw results count as 0).
mk_result <- function(n) {
  v <- list(list(result = as.raw(rep(1L, n)), data_type = "R_object",
                 is_arrow = FALSE, elem_name = ""))
  class(v) <- "result_object"
  v
}

test_that("O1 — over-RAM entry survives repeated reads (the bug)", {
  ## sz (5000) > max_ram (1000) -> entry goes straight to disk and can never be
  ## promoted. Every consumer must still resolve it.
  cache <- flumen:::new_memory_cache(max_ram = 1000, max_disk = 1e9,
                                     dir = tempfile("cacheO1_"))
  v <- mk_result(5000)
  flumen:::cache_set(cache, "X", v)
  r1 <- flumen:::cache_get(cache, "X")
  r2 <- flumen:::cache_get(cache, "X")
  r3 <- flumen:::cache_get(cache, "X")
  expect_identical(r1[[1]]$result, v[[1]]$result)
  expect_identical(r2[[1]]$result, v[[1]]$result)   # was NULL before the fix
  expect_identical(r3[[1]]$result, v[[1]]$result)
})

test_that("O2 — fits-RAM entry is unchanged (multi-read, RAM-resident)", {
  cache <- flumen:::new_memory_cache(max_ram = 10000, max_disk = 1e9,
                                     dir = tempfile("cacheO2_"))
  v <- mk_result(5000)
  flumen:::cache_set(cache, "X", v)
  expect_identical(flumen:::cache_get(cache, "X")[[1]]$result, v[[1]]$result)
  expect_identical(flumen:::cache_get(cache, "X")[[1]]$result, v[[1]]$result)
  ## stays in RAM, never spilled
  expect_true("X" %in% names(cache$state$ram))
  expect_equal(cache$state$disk_used, 0)
})

test_that("O3 — LRU-spilled fits-RAM entry re-promotes and serves again", {
  ## max_ram holds one 5000 entry; the second set evicts the first to disk.
  cache <- flumen:::new_memory_cache(max_ram = 6000, max_disk = 1e9,
                                     dir = tempfile("cacheO3_"))
  X <- mk_result(5000); Y <- mk_result(5000)
  flumen:::cache_set(cache, "X", X)
  flumen:::cache_set(cache, "Y", Y)              # X spilled to disk
  expect_identical(flumen:::cache_get(cache, "X")[[1]]$result, X[[1]]$result)
  expect_identical(flumen:::cache_get(cache, "X")[[1]]$result, X[[1]]$result)
  expect_identical(flumen:::cache_get(cache, "Y")[[1]]$result, Y[[1]]$result)
})

test_that("O4 — over-RAM entry keeps a consistent disk accounting after reads", {
  cache <- flumen:::new_memory_cache(max_ram = 1000, max_disk = 1e9,
                                     dir = tempfile("cacheO4_"))
  flumen:::cache_set(cache, "Z", mk_result(5000))
  for (i in 1:3) invisible(flumen:::cache_get(cache, "Z"))
  s <- cache$state
  expect_true("Z" %in% names(s$disk_sizes))      # still tracked
  expect_equal(s$disk_used, 5000)                # no leak / no negative
  expect_equal(s$ram_used, 0)                    # never promoted
  expect_true(file.exists(flumen:::cache_disk_path(cache, "Z")))
})

test_that("O5 — cache_reset removes retained disk files", {
  cache <- flumen:::new_memory_cache(max_ram = 1000, max_disk = 1e9,
                                     dir = tempfile("cacheO5_"))
  flumen:::cache_set(cache, "Z", mk_result(5000))
  invisible(flumen:::cache_get(cache, "Z"))      # retained on disk by the fix
  fpath  <- flumen:::cache_disk_path(cache, "Z")
  diskdir <- cache$state$disk_dir
  expect_true(file.exists(fpath))
  flumen:::cache_reset(cache)
  expect_false(file.exists(fpath))
  expect_false(dir.exists(diskdir))
  expect_equal(cache$state$disk_used, 0)
})
