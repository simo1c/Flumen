## cache_set must subtract a pre-existing key's size and drop its old copy (RAM
## entry and/or disk file) before (re)inserting -- the mesh sets the same key
## again when re-caching a re-received holder output. Otherwise ram_used /
## disk_used drift upward on every overwrite, causing premature spill (RAM
## believed full while free) and a spurious "Disk cache limit exceeded" stop on
## a run whose disk is nearly empty.

mk <- function(nbytes) {
  v <- list(list(result = as.raw(rep(1L, nbytes)), data_type = "R_object",
                 is_arrow = FALSE, elem_name = NULL))
  class(v) <- "result_object"
  v
}

test_that("repeated cache_set on the same RAM key does not inflate ram_used", {
  cache <- flumen:::new_memory_cache(max_ram = 1e6, max_disk = 1e7, dir = tempfile("mbram_"))
  for (i in 1:3) flumen:::cache_set(cache, "k", mk(1000))
  expect_equal(cache$state$ram_used, 1000)
  expect_equal(length(cache$state$ram), 1L)
  expect_false(is.null(flumen:::cache_get(cache, "k")))
})

test_that("repeated cache_set on the same DISK key does not raise a spurious limit error", {
  ## max_ram=1 forces everything to disk; the real content is one 1000B entry,
  ## well under max_disk=2500. Without reconciliation the 3rd set would inflate
  ## disk_used to 3000 and throw "Disk cache limit exceeded".
  cache <- flumen:::new_memory_cache(max_ram = 1, max_disk = 2500, dir = tempfile("mbdisk_"))
  expect_error(for (i in 1:3) flumen:::cache_set(cache, "k", mk(1000)), NA)  # NA = no error
  expect_equal(cache$state$disk_used, 1000)
})

test_that("overwrite with a different size reflects only the new size", {
  cache <- flumen:::new_memory_cache(max_ram = 1e6, max_disk = 1e7, dir = tempfile("mbsz_"))
  flumen:::cache_set(cache, "k", mk(1000))
  flumen:::cache_set(cache, "k", mk(3000))
  expect_equal(cache$state$ram_used, 3000)
  expect_equal(length(flumen:::cache_get(cache, "k")[[1]]$result), 3000L)
})

test_that("re-setting a disk-resident key into RAM drops the stale disk copy", {
  cache <- flumen:::new_memory_cache(max_ram = 1500, max_disk = 1e7, dir = tempfile("mbxt_"))
  flumen:::cache_set(cache, "a", mk(1000))
  flumen:::cache_set(cache, "b", mk(1000))                   # a spills to disk
  expect_true("a" %in% names(cache$state$disk_sizes))
  flumen:::cache_set(cache, "a", mk(500))                    # a now fits RAM again
  expect_false("a" %in% names(cache$state$disk_sizes))   # stale disk entry gone
  expect_true("a" %in% names(cache$state$ram))
  expect_false(file.exists(flumen:::cache_disk_path(cache, "a")))  # stale file removed
})
