## Feature: capability `tags` inside requires/resources, matched with AND
## (subset) semantics, so a task can be pinned to workers advertising a
## capability (e.g. tags="cellranger" -> only Linux workers) without abusing the
## numeric cpu/mem/gpu schema.
##
## Scope: tags travel via a worker's declared resources. For a LAUNCHED worker
## those are launcher$resources, which stay authoritative. A local worker (no
## launcher) declares its own through new_worker(resources = ...) -- that came
## later than this file and is covered by test-worker-resources.R; the local
## worker mocked below deliberately declares none, which is why it matches no
## tag at all. That is still the mechanism keeping a Linux-only tool off a Mac
## worker: not the absence of the ability to advertise, but the absence of the
## advertised tag.
##
## requires/resources are scheduler-side only (never serialized to workers),
## so this touches validation + matching + human-readable formatting only.

## minimal mock worker advertising resources via its launcher
mkw <- function(...) list(launcher = list(resources = list(...)))
## local worker (no launcher -> advertises no resources)
loc <- list(launcher = NULL)

test_that("tags are accepted by validation and enforced (not silently ignored) by matching", {
  ## 'tags' is a valid capability field, not an unknown field
  expect_error(flumen:::validate_resources(list(tags = "linux")), NA)
  ## a worker that offers resources but NOT the required capability is rejected
  expect_false(flumen:::worker_satisfies_requires(mkw(cpu = 8), list(tags = "cellranger")))
})

test_that("FIX: tags are validated and matched with AND (subset) semantics", {
  ## validation accepts a character-vector tags field (on requires and resources)
  expect_error(flumen:::validate_resources(list(tags = c("linux", "cellranger"))), NA)
  expect_error(flumen:::validate_resources(list(cpu = 8, tags = "cellranger")),     NA)

  ## AND-subset matching: worker must offer every required tag
  wc <- mkw(tags = c("linux", "cellranger"))
  expect_true (flumen:::worker_satisfies_requires(wc, list(tags = "cellranger")))
  expect_true (flumen:::worker_satisfies_requires(wc, list(tags = c("linux", "cellranger"))))
  expect_false(flumen:::worker_satisfies_requires(mkw(tags = "linux"), list(tags = c("linux", "cellranger"))))  # one missing
  expect_false(flumen:::worker_satisfies_requires(mkw(cpu = 8),        list(tags = "cellranger")))              # offers no tags
  expect_false(flumen:::worker_satisfies_requires(loc,                 list(tags = "cellranger")))              # local worker

  ## combined with numeric fields: both must be satisfied
  wct <- mkw(cpu = 8, tags = "cellranger")
  expect_true (flumen:::worker_satisfies_requires(wct, list(cpu = 4,  tags = "cellranger")))
  expect_false(flumen:::worker_satisfies_requires(wct, list(cpu = 16, tags = "cellranger")))  # cpu insufficient
  expect_false(flumen:::worker_satisfies_requires(wct, list(cpu = 4,  tags = "gpu")))         # tag missing

  ## backward compatibility: behaviour without tags is unchanged
  expect_true (flumen:::worker_satisfies_requires(loc,          NULL))            # no requires -> anywhere
  expect_true (flumen:::worker_satisfies_requires(mkw(cpu = 8), list(cpu = 4)))   # numeric-only unchanged
  expect_false(flumen:::worker_satisfies_requires(mkw(cpu = 8), list(cpu = 16)))

  ## flumen:::format_resources() must handle a vector-valued field (used in error messages)
  expect_error(flumen:::format_resources(list(tags = c("linux", "cellranger"))), NA)
  expect_match(flumen:::format_resources(list(tags = c("linux", "cellranger"))), "linux")
})
