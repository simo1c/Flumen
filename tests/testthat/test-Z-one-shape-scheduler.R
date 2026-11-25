## On the scheduler, as on the supervisor, a resolved dependency
## has ONE shape, a list of constituents (output_req_list), a single producer
## included. The expansion engine must carry that list through untouched.

req <- function(exe_id, tmpl, instance = NULL)
  flumen:::new_output_req(tmpl, exe_id, instance, socket = "ipc://x")

test_that("as_output_req_list gives the one shape whatever it is handed", {
  one <- req("E1", "a.txt")
  l1 <- flumen:::as_output_req_list(one)
  expect_s3_class(l1, "output_req_list")
  expect_length(l1, 1L)
  expect_identical(l1[[1]]$exe_id, "E1")
  ## already a list: unchanged
  l2 <- flumen:::new_output_req_list(req("E1", "a_{{n}}.txt"), req("E2", "a_{{n}}.txt"))
  expect_identical(flumen:::as_output_req_list(l2), l2)
  ## a plain list of constituents becomes the classed container
  l3 <- flumen:::as_output_req_list(list(req("E1", "a.txt"), req("E2", "b.txt")))
  expect_s3_class(l3, "output_req_list")
  expect_length(l3, 2L)
  ## anything else is the invariant being false
  expect_error(flumen:::as_output_req_list("a.txt"), "list of constituents|output_req")
})

test_that("the expansion engine keeps a one-constituent container as a container", {
  ## A static (no wildcard) parameter group goes through conditional_expand,
  ## which delivers a non-expanded list of ONE element as that element (sec.
  ## 7.5 of the DSL). A dependency's container of one producer is not such a
  ## wrapping: unboxed, its output_req came out wearing the container's class
  ## and every reader iterating the constituents would meet atomic fields
  ## ("$ operator is invalid for atomic vectors").
  pv <- list(k = "s1", ones = flumen:::new_output_req_list(req("E1", "one/s1.txt", instance = 2L)))
  out <- flumen:::expand_parameters(pv, flumen::new_wildcard_list(s = c("s1")),
                                    par_expansion = c(k = FALSE, ones = FALSE),
                                    gather = c(k = FALSE, ones = FALSE))
  expect_length(out$expanded_par_values, 1L)
  v <- out$expanded_par_values[[1]]$ones
  expect_s3_class(v, "output_req_list")
  expect_length(v, 1L)
  expect_s3_class(v[[1]], "output_req")
  expect_identical(v[[1]]$exe_id, "E1")
  expect_identical(v[[1]]$output_instance, 2L)
  ## the plain value of one element is still delivered bare
  expect_identical(out$expanded_par_values[[1]]$k, "s1")
  ## and the dependency ids are read off the container
  op <- list(par_list = list(list(value = v)))
  expect_identical(flumen:::get_dep_exe_ids_from_task(op), "E1")
})
