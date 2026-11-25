## A plain input -- new_object()/new_file() without _exp, not a dependency --
## is a VECTOR BINDING: it receives all the values of a static wildcard the task
## does not expand (FLUMEN_DSL.md sec. 5.1, "1 instance, all values as vector").
## This holds for the bare token "{sample}" and for text around it
## ("id_{sample}", "ref/{ref}.fa") alike: both are resolved by the wildcard
## engine, one entry per KEY of the wildcard, the same way in every shape.

vb_values <- function(kind, vb, wl = new_wildcard_list(sample = c("s1", "s2"), ref = c("r1", "r2"))) {
  sc    <- new_script("f <- function(...) 1")
  A_exp <- new_R_task("A", input = new_input(x = new_object_exp("{sample}")),
                      output = new_output(o = new_object("{{a}}")), exec = sc)
  A_one <- new_R_task("A", input = new_input(x = 1L),
                      output = new_output(o = new_object("{{a}}")), exec = sc)
  tasks <- switch(kind,
    root      = list(new_R_task("R", input = new_input(v = vb, y = 1L), output = new_output(o = new_object("{{r}}")), exec = sc),
                     new_R_task("Z", input = new_input(d = new_dep("R", new_object("{{r}}"))), output = new_output(o = new_object("{{z}}")), exec = sc)),
    broadcast = list(A_one, new_R_task("R", input = new_input(v = vb, d = new_dep("A", new_object("{{a}}"))), output = new_output(o = new_object("{{r}}")), exec = sc)),
    cloned    = list(A_exp, new_R_task("R", input = new_input(v = vb, d = new_dep("A", new_object("{{a}}"))), output = new_output(o = new_object("{{r}}")), exec = sc)),
    gather    = list(A_exp, new_R_task("R", input = new_input(v = vb, d = new_dep("A", new_object_aggr("{{a}}"))), output = new_output(o = new_object("{{r}}")), exec = sc)),
    by        = list(new_R_task("A", input = new_input(x = new_object_exp("{sample}"), z = new_object_exp("{ref}")), output = new_output(o = new_object("{{a}}")), exec = sc),
                     new_R_task("R", input = new_input(s = new_object_exp("{ref}"), v = vb, d = new_dep("A", new_object_aggr("{{a}}", by = "ref"))), output = new_output(o = new_object("{{r}}")), exec = sc)),
    exp_other = list(A_one, new_R_task("R", input = new_input(r = new_object_exp("{ref}"), v = vb, d = new_dep("A", new_object("{{a}}"))), output = new_output(o = new_object("{{r}}")), exec = sc)))
  wf <- suppressWarnings(new_workflow("vb", "vector binding", tasks, wl))
  lapply(Filter(function(t) identical(t$task_id, "R"), wf$task_list), function(t)
    as.character(unlist(Filter(function(p) identical(p$name, "v"), t$par_list)[[1]]$value)))
}

test_that("a composite vector binding receives every value, in every task shape", {
  for (kind in c("root", "broadcast", "cloned", "gather", "by", "exp_other")) {
    for (v in vb_values(kind, new_object("id_{sample}")))
      expect_identical(v, c("id_s1", "id_s2"), info = kind)
    for (v in vb_values(kind, new_file("in/{sample}.fq")))
      expect_identical(v, c("in/s1.fq", "in/s2.fq"), info = kind)
    ## the bare token, unchanged
    for (v in vb_values(kind, new_object("{sample}")))
      expect_identical(v, c("s1", "s2"), info = kind)
  }
})

test_that("several wildcards in one text keep the order of a root task", {
  wl <- new_wildcard_list(sample = c("s1", "s2"), ref = c("r1", "r2", "r3"))
  expect_identical(vb_values("root", new_object("x_{sample}_{ref}"), wl)[[1]],
                   c("x_s1_r1", "x_s1_r2", "x_s1_r3", "x_s2_r1", "x_s2_r2", "x_s2_r3"))
  expect_identical(vb_values("root", new_file(c("d/{sample}_1.fq", "d/{sample}_2.fq")), wl)[[1]],
                   c("d/s1_1.fq", "d/s2_1.fq", "d/s1_2.fq", "d/s2_2.fq"))
})

test_that("text around the token receives the KEY of a named or object wildcard", {
  keyed <- new_wildcard_list(smp = list(S1 = "raw/S1.fq", S2 = "raw/S2.fq"))
  expect_identical(vb_values("root", new_file("out/{smp}.txt"), keyed)[[1]],
                   c("out/S1.txt", "out/S2.txt"))
  objs <- new_wildcard_list(new_wildcard("data", list(d1 = data.frame(a = 1), d2 = data.frame(a = 2))))
  expect_identical(vb_values("root", new_object("id_{data}"), objs)[[1]], c("id_d1", "id_d2"))
})
