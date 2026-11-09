## Stage 11, group A: a constituent is read by the identity of its
## producer, never by position, and a constituent that would be delivered
## empty is refused instead of handed to the wrapper.

req <- function(exe_id, tmpl) flumen:::new_output_req(tmpl, exe_id, NULL, socket = "ipc://x")

test_that("output_req_for returns the constituent of that producer and nothing else", {
  lst <- flumen:::new_output_req_list(req("E1", "parts/s1_r1_{{n}}.txt"),
                                      req("E2", "parts/s1_r2_{{n}}.txt"))
  expect_identical(flumen:::output_req_for(lst, "E2")$dyn_wildcard, "parts/s1_r2_{{n}}.txt")
  expect_identical(flumen:::output_req_for(lst, "E1")$dyn_wildcard, "parts/s1_r1_{{n}}.txt")
  ## progressive resolution replaced E1's slot with a value: E2 is still found
  lst[[1]] <- list("parts/s1_r1_1.txt")
  expect_identical(flumen:::output_req_for(lst, "E2")$exe_id, "E2")
  ## no fallback to a sibling
  expect_null(flumen:::output_req_for(lst, "E9"))
  expect_null(flumen:::output_req_for(lst, "E1"))
  ## a single output_req answers only for its own producer
  one <- req("E1", "p/out.txt")
  expect_identical(flumen:::output_req_for(one, "E1")$exe_id, "E1")
  expect_null(flumen:::output_req_for(one, "E2"))
  ## the selectors read through the same door
  one$file_sel <- "^s1_"
  expect_identical(flumen:::output_req_selectors(one, "E1")$file_sel, "^s1_")
  expect_null(flumen:::output_req_selectors(one, "E2")$file_sel)
})

## In-memory reproduction of the X2 / C04a shape: the co-local resolver is
## handed a container whose OWN constituent for the producer carries the right
## template, next to a sibling carrying another. Before the stage the sibling's
## template was read and the delivery came out empty; now the delivery is
## whole, and a template that excludes everything is refused.
co_local_fixture <- function(own_template) {
  cache <- flumen:::new_memory_cache(dir = file.path(tempdir(), paste0("cache_", basename(tempfile()))))
  files <- list("parts/s1_r1_1.txt", "parts/s1_r1_2.txt")
  results <- flumen:::new_result(lapply(files, function(f)
    list(result = qs2::qs_serialize(f), data_type = "file", is_arrow = FALSE,
         elem_name = "", output_par_name = "part")))
  flumen:::cache_set(cache, "E1", results)
  container <- flumen:::new_output_req_list(req("E2", "parts/s1_r2_{{n}}.txt"),
                                            req("E1", own_template))
  exe <- list(task_id = "M", type = "R",
              par_list = list(parts = container), par_names = "parts", par_uuids = "u1",
              params_type = c(u1 = "R_object"), params_is_arrow = c(u1 = FALSE), params_form = c(u1 = "aggregate"),
              params_element_types = list(), params_element_is_arrow = list(),
              file_transfer = list())
  reg <- list(W = list(exe_object = exe, par_list_locality = c(parts = "parameter_request")))
  ws <- data.frame(Wrapper_ID = c("W", "W"), Op_ID = "M", Param_dependency = "parts",
                   Param_dependency_wrapper_ID = c("E2", "E1"), Supervisor_dependency = "ipc://x",
                   Status = "ready", stringsAsFactors = FALSE)
  flumen:::update_co_local_dependency(reg, "W", "parts", "E1", ws, cache)
}

test_that("the co-local resolver uses the constituent's own template, whatever the sibling says", {
  out <- co_local_fixture("parts/s1_r1_{{n}}.txt")
  expect_null(out$error)
  slot <- out$reg_tasks$W$exe_object$par_list$parts[[2]]
  expect_length(slot, 2L)
})

test_that("a constituent that would be delivered empty is refused, not handed over", {
  out <- co_local_fixture("parts/s1_r2_{{n}}.txt")   ## the sibling's template
  expect_true(is.character(out$error))
  expect_match(out$error, "matches 0 of the 2 file")
})

## Stage 14: the template is a CHECK, not a filter. A template that accepts
## only some of the files the selectors let through is refused whole, with the
## files it would have dropped named; it used to deliver the matching subset
## with a log note.
test_that("a constituent whose template drops some of the selected files is refused, not trimmed", {
  out <- co_local_fixture("parts/s1_r1_1{{n}}")      ## accepts parts/s1_r1_1.txt only
  expect_true(is.character(out$error))
  expect_match(out$error, "matches 1 of the 2 file")
  expect_match(out$error, "parts/s1_r1_2.txt")
})

## Stage 11, group C1: the launch decision reads the container, not only
## the rows. A container with one constituent still waiting is pending; one
## whose constituents have all been replaced by values is not.
test_that("container_pending lists exactly the constituents still waiting", {
  lst <- flumen:::new_output_req_list(req("E1", "a_{{n}}.txt"), req("E2", "b_{{n}}.txt"))
  exe <- list(task_id = "M", par_list = list(k = "x", parts = lst))
  p <- flumen:::container_pending(exe)
  expect_length(p, 2L)
  expect_setequal(vapply(p, `[[`, character(1), "exe_id"), c("E1", "E2"))
  lst[[1]] <- list("a_1.txt")                       ## E1 delivered
  exe$par_list$parts <- lst
  p <- flumen:::container_pending(exe)
  expect_length(p, 1L)
  expect_identical(p[[1]][["exe_id"]], "E2")
  expect_match(flumen:::pending_message(exe, "W", p), "waits for 1 constituent")
  lst[[2]] <- list("b_1.txt")                       ## E2 delivered
  exe$par_list$parts <- lst
  expect_length(flumen:::container_pending(exe), 0L)
  ## a single output_req is pending too
  exe$par_list$one <- req("E3", "c.txt")
  expect_length(flumen:::container_pending(exe), 1L)
})

test_that("declared_form reads the declared form by uuid and is NA when absent", {
  exe <- list(par_uuids = c("u1", "u2"), params_form = c(u1 = "aggregate", u2 = "bare"))
  expect_identical(flumen:::declared_form(exe, 1L), "aggregate")
  expect_identical(flumen:::declared_form(exe, 2L), "bare")
  exe$params_form <- NULL
  expect_true(is.na(flumen:::declared_form(exe, 1L)))
})

## Stage 11, 3c: the container rebuilt from the slots must hold as many
## constituents as the scheduler declared.
test_that("constituent_count_mismatch names every parameter whose container disagrees with the declaration", {
  lst <- flumen:::new_output_req_list(req("E1", "a_{{n}}.txt"), req("E2", "b_{{n}}.txt"))
  exe <- list(par_names = c("k", "parts", "one"), par_uuids = c("u0", "u1", "u2"),
              par_list = list(k = "x", parts = lst, one = req("E3", "c.txt")),
              params_n_constituents = c(u0 = 0L, u1 = 2L, u2 = 1L))
  expect_length(flumen:::constituent_count_mismatch(exe), 0L)
  exe$params_n_constituents[["u1"]] <- 3L                 ## declared three, holds two
  m <- flumen:::constituent_count_mismatch(exe)
  expect_length(m, 1L)
  expect_match(m, "'parts' declared 3 constituent\\(s\\), the container holds 2")
  ## Stage 14, the user's decision: one contract only. A message without the
  ## declaration comes from another build and is reported as such, instead of
  ## being accepted with nothing to check it against.
  exe$params_n_constituents <- NULL
  m0 <- flumen:::constituent_count_mismatch(exe)
  expect_length(m0, 1L)
  expect_match(m0, "different builds of flumen")
})

test_that("constituent_count_mismatch finds a container keyed by uuid (shell tasks)", {
  exe <- list(par_names = c("a"), par_uuids = c("u1"),
              par_list = list(u1 = req("E1", "p.txt")),
              params_n_constituents = c(u1 = 1L))
  expect_length(flumen:::constituent_count_mismatch(exe), 0L)
})

## Stage 12: a dependency parameter is a list of constituents whatever their
## number and whatever the siblings look like. The shape is a function of the
## parameter alone: a single producer gives a list of one, with or without a
## gathering sibling in the same message, and a plain value stays bare.
test_that("group_by_name builds a list of constituents for every dependency, a single producer included", {
  one <- req("E1", "e.txt"); two_a <- req("E2", "f_{{n}}.txt"); two_b <- req("E3", "f_{{n}}.txt")
  g <- flumen:::group_by_name(list(k = "x", obj = one, files = two_a, files = two_b),
                              c(k = "R_object", obj = "R_object", files = "R_object"))
  expect_s3_class(g$files, "output_req_list")
  expect_length(g$files, 2L)
  expect_s3_class(g$obj, "output_req_list")
  expect_length(g$obj, 1L)
  expect_identical(g$obj[[1]]$exe_id, "E1")
  expect_identical(g$k, "x")
  ## the same single dependency without any sibling: identical shape
  g1 <- flumen:::group_by_name(list(k = "x", obj = one), c(k = "R_object", obj = "R_object"))
  expect_identical(class(g1$obj), class(g$obj))
  expect_length(g1$obj, 1L)
  expect_identical(g1$k, "x")
})

## Stage 12: the resolvers fill ONE shape. A container that is not a list of
## constituents is the invariant being false, reported as such.
test_that("update_param_wildcard_with_value refuses a container that is not a list of constituents", {
  pl <- list(obj = req("E1", "e.txt"))
  expect_error(flumen:::update_param_wildcard_with_value(pl, list(as.raw(1:3)), 1L,
                                                          is_arrow = FALSE, data_type = "vector", exe_id = "E1"),
               "not a list of constituents")
  pl <- list(obj = flumen:::new_output_req_list(req("E1", "e.txt")))
  out <- flumen:::update_param_wildcard_with_value(pl, list(as.raw(1:3)), 1L,
                                                    is_arrow = TRUE, data_type = "vector", exe_id = "E1")
  expect_s3_class(out$obj, "output_req_list")
  expect_identical(attr(out$obj[[1]][[1]], "flumen_elem_type"), "vector")
  expect_true(attr(out$obj[[1]][[1]], "flumen_elem_is_arrow"))
  ## a native element (a path) is labelled too, "file" as the on-disk "file_path"
  out <- flumen:::update_param_wildcard_with_value(pl, list("a/b.txt"), 1L,
                                                    is_arrow = FALSE, data_type = "file", exe_id = "E1")
  expect_identical(attr(out$obj[[1]][[1]], "flumen_elem_type"), "file_path")
})
