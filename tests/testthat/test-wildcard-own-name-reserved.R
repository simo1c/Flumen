## A wildcard value equal to the wildcard's own name ("cond" in
## cond = c("cond", "ctrl")), bare or in a pattern form, is the sentinel the
## expansion engine reads as "aggregate over this wildcard". Accepted, the
## instance bound to it is wired to every parent instance and B[cond] receives
## A[ctrl]'s result with exit 0, so new_workflow() refuses it
## (R/validation.R, rule 5e).

own_name_wf <- function(wl, wd = names(wl)[1]) {
  A <- new_R_task(
    task_id = "A",
    input   = new_input(x = new_object_exp(sprintf("{%s}", wd))),
    output  = new_output(o = new_object("{{oa}}")),
    exec    = new_script("f <- function(x) x")
  )
  B <- new_R_task(
    task_id = "B",
    input   = new_input(x = new_object_exp(sprintf("{%s}", wd)),
                        y = new_dep("A", new_object("{{oa}}"))),
    output  = new_output(o = new_object("{{ob}}")),
    exec    = new_script("f <- function(x, y) y")
  )
  tasks <- list(A, B)
  for (e in setdiff(names(wl), wd))
    tasks[[length(tasks) + 1L]] <- new_R_task(
      task_id = paste0("T_", e),
      input   = new_input(x = new_object_exp(sprintf("{%s}", e)),
                          y = new_dep("A", new_object("{{oa}}"))),
      output  = new_output(o = new_object(sprintf("{{o_%s}}", e))),
      exec    = new_script("f <- function(x, y) y")
    )
  new_workflow(workflow_id = "own_name", desc = "own name", task_list = tasks,
               wildcard_list = wl)
}

test_that("a value equal to the wildcard's own name is refused, in every form", {
  expect_error(own_name_wf(new_wildcard_list(cond = c("cond", "ctrl"))),
               regexp = "its\\s+own\\s+name")
  expect_error(own_name_wf(new_wildcard_list(sample = "sample")),
               regexp = "its\\s+own\\s+name")
  expect_error(own_name_wf(new_wildcard_list(cond = c("{cond}", "ctrl"))),
               regexp = "its\\s+own\\s+name")
  ## the key of an object wildcard
  expect_error(own_name_wf(new_wildcard_list(new_wildcard("data",
                 list(data = data.frame(a = 1), d2 = data.frame(a = 2))))),
               regexp = "its\\s+own\\s+name")
  ## the string value of a keyed wildcard
  expect_error(own_name_wf(new_wildcard_list(sample = list(S1 = "sample", S2 = "x"))),
               regexp = "its\\s+own\\s+name")
})

test_that("values that only resemble the name are accepted", {
  expect_s3_class(own_name_wf(new_wildcard_list(cond = c("cond1", "my_cond", "condition"))),
                  "workflow_object")
  expect_s3_class(own_name_wf(new_wildcard_list(cond = c("Cond", "ctrl"))),
                  "workflow_object")
  ## the name of ANOTHER wildcard is not a sentinel for this one
  expect_s3_class(suppressWarnings(own_name_wf(new_wildcard_list(cond = c("sample", "ctrl"),
                                                                 sample = c("s1", "s2")))),
                  "workflow_object")
  ## the cells of an object value are data, not identities
  expect_s3_class(own_name_wf(new_wildcard_list(new_wildcard("data",
                    list(d1 = data.frame(x = "data"), d2 = data.frame(x = "y"))))),
                  "workflow_object")
})
