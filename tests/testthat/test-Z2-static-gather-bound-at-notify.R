## Stage 13b: a static gather over a producer is bound to its constituents
## at the notify of each producer instance, and the container is the one
## shape (output_req_list), one constituent per path in the order of the
## paths, placeholders (exe_id NA) for the producers not yet finished.

mk_out <- function(name, path) {
  p <- list(name = name, value = structure(path, class = c("file_object", "file_path", "flumen_path", "character", "vector")))
  class(p) <- c("R_out_par_object", "out_par_object", "par_object")
  p
}
mk_producer <- function(exe_id, path, extra = NULL) {
  outs <- list(mk_out("o", path))
  if (!is.null(extra)) outs <- c(outs, list(mk_out("o2", extra)))
  list(task_id = "P", exe_id = exe_id, par_list = outs,
       output_files = unlist(lapply(outs, function(p) as.character(p$value))))
}
mk_consumer <- function(paths, template) {
  agg <- structure(paths, class = c("aggregation", "file_object", "file_path", "flumen_path", "character", "vector"),
                   original_template = template)
  dep <- structure(list(task_id = "P", value = agg), class = "dep_object")
  list(task_id = "C", exe_id = "C1", par_list = list(list(name = "xs", value = dep)))
}
notify <- function(exe_id) list(exe_id = exe_id)

test_that("the first notify builds the container, each notify fills its own constituent, order is the paths' order", {
  wf <- list(task_list = list(mk_producer("P1", "out/s1.txt"), mk_producer("P2", "out/s2.txt"),
                              mk_consumer(c("out/s1.txt", "out/s2.txt"), "out/{s}.txt")))
  ## P2 finishes first
  wf <- flumen:::bind_static_gather_constituents(wf, op_to_expand = 3L, finished_task = wf$task_list[[2]],
                                                 res_notify = notify("P2"), socket_worker = "ipc://w2", uses_tcp_worker = FALSE)
  v <- wf$task_list[[3]]$par_list[[1]]$value
  expect_s3_class(v, "output_req_list")
  expect_length(v, 2L)
  expect_true(isTRUE(attr(v, "gather_marker")))
  expect_true(is.na(v[[1]]$exe_id))                      ## s1 still waiting
  expect_identical(v[[2]]$exe_id, "P2")
  expect_identical(v[[2]]$dyn_wildcard, "out/s2.txt")
  expect_identical(v[[2]]$consumer_pattern, "out/{s}.txt")
  expect_identical(v[[2]]$socket, "ipc://w2")
  expect_null(v[[2]]$output_path_sel)                    ## one declared output: no path selector
  ## P1 finishes
  wf <- flumen:::bind_static_gather_constituents(wf, op_to_expand = 3L, finished_task = wf$task_list[[1]],
                                                 res_notify = notify("P1"), socket_worker = "ipc://w1", uses_tcp_worker = TRUE)
  v <- wf$task_list[[3]]$par_list[[1]]$value
  expect_identical(vapply(v, `[[`, character(1), "exe_id"), c("P1", "P2"))
  expect_true(v[[1]]$uses_tcp)
  ## a third notify of an unrelated producer changes nothing
  wf2 <- flumen:::bind_static_gather_constituents(wf, op_to_expand = 3L, finished_task = mk_producer("P9", "elsewhere.txt"),
                                                  res_notify = notify("P9"), socket_worker = "ipc://w9", uses_tcp_worker = FALSE)
  expect_identical(wf2$task_list[[3]]$par_list[[1]]$value, v)
})

test_that("a producer with several outputs gives its constituent a path selector, backticks stripped", {
  wf <- list(task_list = list(mk_producer("P1", "out/s1.txt", extra = "out/s1.log"),
                              mk_consumer("`out/`s1.txt", "`out/`{s}.txt")))
  wf <- flumen:::bind_static_gather_constituents(wf, op_to_expand = 2L, finished_task = wf$task_list[[1]],
                                                 res_notify = notify("P1"), socket_worker = "ipc://w1", uses_tcp_worker = FALSE)
  v <- wf$task_list[[2]]$par_list[[1]]$value
  expect_identical(v[[1]]$exe_id, "P1")
  ## anchored at "^", not at a path boundary: a suffix match would also claim a
  ## file of a DEEPER declared output (R/scheduler_task_table.R)
  expect_identical(v[[1]]$output_path_sel, "^out/s1\\.txt$")
})

test_that("a DYNAMIC gather is left to the wildcard engine", {
  ## Seen on the first build of 13b: the dynamic template was bound as a
  ## static gather, and its placeholder constituent reached the supervisor
  ## with exe_id NA (six contract cells and nine delivery tests).
  wf <- list(task_list = list(mk_producer("P1", "parts/s1_{{n}}.txt"),
                              mk_consumer("parts/s1_{{n}}.txt", "parts/{s}_{{n}}.txt")))
  wf <- flumen:::bind_static_gather_constituents(wf, op_to_expand = 2L, finished_task = wf$task_list[[1]],
                                                 res_notify = notify("P1"), socket_worker = "ipc://w1", uses_tcp_worker = FALSE)
  expect_s3_class(wf$task_list[[2]]$par_list[[1]]$value, "dep_object")
})

test_that("a consumer of another task is left alone", {
  wf <- list(task_list = list(mk_producer("P1", "out/s1.txt"),
                              mk_consumer("out/s1.txt", "out/{s}.txt")))
  wf$task_list[[2]]$par_list[[1]]$value$task_id <- "OTHER"
  wf <- flumen:::bind_static_gather_constituents(wf, op_to_expand = 2L, finished_task = wf$task_list[[1]],
                                                 res_notify = notify("P1"), socket_worker = "ipc://w1", uses_tcp_worker = FALSE)
  expect_s3_class(wf$task_list[[2]]$par_list[[1]]$value, "dep_object")
})
