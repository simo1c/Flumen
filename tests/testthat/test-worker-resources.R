## Feature: local workers can declare `resources` (incl. capability `tags`)
## directly via new_worker(resources=), so a task can be pinned to a LOCAL
## worker (e.g. a Mac master+worker). The launcher stays AUTHORITATIVE for
## launched (remote/SLURM) workers -- its resources describe what is actually
## provisioned -- so resources come from the launcher when present, and from
## new_worker(resources=) only for local workers (no launcher). Declaring
## resources in BOTH places is an error (no silent precedence).

test_that("new_worker() accepts worker-level resources for a local worker", {
  expect_error(new_worker(name = "mac", resources = list(tags = "mac")), NA)
  expect_equal(new_worker(name = "mac", resources = list(tags = "mac"))$resources,
               list(tags = "mac"))
})

test_that("local worker resources are used for matching; launcher stays authoritative", {
  ## a local worker's declared resources drive the match
  loc_mac <- list(launcher = NULL, resources = list(tags = "mac"))
  expect_true (flumen:::worker_satisfies_requires(loc_mac, list(tags = "mac")))
  expect_false(flumen:::worker_satisfies_requires(loc_mac, list(tags = "linux")))

  ## a local worker WITHOUT resources is unchanged (matches only requires=NULL)
  expect_false(flumen:::worker_satisfies_requires(list(launcher = NULL, resources = NULL), list(tags = "mac")))
  expect_true (flumen:::worker_satisfies_requires(list(launcher = NULL, resources = NULL), NULL))

  ## launcher is authoritative: for a launched worker the launcher's resources
  ## are used and any worker-level resources are ignored
  remote <- list(launcher = list(resources = list(tags = "linux")),
                 resources = list(tags = "mac"))
  expect_true (flumen:::worker_satisfies_requires(remote, list(tags = "linux")))   # from launcher
  expect_false(flumen:::worker_satisfies_requires(remote, list(tags = "mac")))     # worker-level ignored
})

test_that("declaring resources in BOTH new_worker() and the launcher is an error", {
  expect_error(
    new_worker(name = "x", resources = list(cpu = 8),
               launcher = ssh_launcher(host_user = "a@b", resources = list(cpu = 8))),
    "one place|launcher"
  )
})
