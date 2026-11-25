## A dynamic `{{wc}}` pattern must be matched as a WHOLE path, at path
## component granularity, and the R matcher and its Python port must agree.
##
## A match against a path has exactly three degrees of freedom: where it may
## START, where it must END, and what a token may ABSORB. A defect of this family
## is one of the three left free: a search INSIDE the name leaves head and tail
## free, and `(.*)` crossing '/' leaves absorb free. The rule pinned here, in
## extract_multiple_wildcard_values():
##   head   anchored at a component boundary, (?:^|/), so a relative pattern
##          still matches an absolute path but never mid-name;
##   tail   anchored at the absolute end, \z, not $;
##   absorb `([^/]*)` unless the pattern is RECURSIVE by is_recursive_wildcard(),
##          the same predicate that chooses list.files(recursive = ...) for it,
##          so the matcher accepts exactly what the lister enumerates.
##
## The table below enumerates the forms, one per degree of freedom plus the
## behaviours that must be PRESERVED. Forms 1, 2, 4 are reachable end to end
## (accessory index delivered as the output; a sample claiming a sibling's
## file). Forms 3 and 9 cannot be reached end to end today, because a
## non-recursive lister never enumerates a sub-directory, so this table is the
## only place they can be seen to fail. Seen to fail on the tail-only build
## for forms 2, 3, 4, 9, and on the original build for all but 5.
##
## The Python port (inst/scripts/objects.py) is run on the SAME table through
## python3, so the two implementations cannot drift apart unnoticed.

forms <- list(
  list(id = "1 tail: index beside the bam",
       p = "out/{{n}}.bam",
       v = c("out/s1.bam", "out/s1.bam.bai", "out/s3.bamx"),
       ok = "out/s1.bam"),
  list(id = "2 head: name carrying another sample's prefix",
       p = "s2_{{n}}.txt",
       v = c("s2_a.txt", "s1_s2_b.txt", "s2_c.txt"),
       ok = c("s2_a.txt", "s2_c.txt")),
  list(id = "3 absorb: sub-directory under a NON-recursive pattern",
       p = "out/{{n}}.bam",
       v = c("out/s1.bam", "out/sub/x.bam"),
       ok = "out/s1.bam"),
  list(id = "4 head: sibling directory sharing a suffix",
       p = "out/{{n}}.bam",
       v = c("out/s1.bam", "other_out/s1.bam"),
       ok = "out/s1.bam"),
  list(id = "5 preserved: a recursive bundle keeps its sub-tree",
       p = "outs/{{any}}",
       v = c("outs/a", "outs/sub/deep"),
       ok = c("outs/a", "outs/sub/deep")),
  list(id = "6 preserved: relative pattern against an absolute path",
       p = "out/{{n}}.bam",
       v = c("/abs/run/out/s1.bam", "/abs/run/out/s1.bam.bai"),
       ok = "/abs/run/out/s1.bam"),
  list(id = "7 preserved: multi-extension prefix, dots inside the capture",
       p = "idx/idx_base.{{N}}.bt2",
       v = c("idx/idx_base.1.bt2", "idx/idx_base.rev.1.bt2", "idx/idx_base.1.bt2.old"),
       ok = c("idx/idx_base.1.bt2", "idx/idx_base.rev.1.bt2")),
  list(id = "8 tail: dynamic token in the middle of the name",
       p = "b/h_{{m}}_t.txt",
       v = c("b/h_v1_t.txt", "b/h_v1_t.txt.bak", "b/x/h_v1_t.txt"),
       ok = "b/h_v1_t.txt"),
  list(id = "9 absorb: two tokens, the lazy one must not cross '/'",
       p = "d/{{a}}_{{b}}.txt",
       v = c("d/x_y.txt", "d/x_y.txt.bak", "d/s/x_y.txt"),
       ok = "d/x_y.txt")
)

test_that("the R matcher pins head, tail and absorb on every form", {
  for (f in forms) {
    got <- flumen:::extract_multiple_wildcard_values(f$p, f$v)$matches
    expect(setequal(got, f$ok),
           sprintf("%s: got [%s], want [%s]", f$id,
                   paste(got, collapse = ","), paste(f$ok, collapse = ",")))
  }
})

test_that("the Python port agrees with R on every form", {
  py <- Sys.which("python3")
  skip_if(!nzchar(py), "python3 not on PATH")
  objects_py <- system.file("scripts", "objects.py", package = "flumen")
  expect_true(nzchar(objects_py), label = "inst/scripts/objects.py installed")
  driver <- paste(
    "import sys, json",
    "sys.path.insert(0, sys.argv[1])",
    "import objects",
    "p, v = json.loads(sys.argv[2]), json.loads(sys.argv[3])",
    "print(json.dumps(objects.extract_multiple_wildcard_values(p, v)['matches']))",
    sep = "; ")
  for (f in forms) {
    out <- system2(py, c("-c", shQuote(driver), shQuote(dirname(objects_py)),
                          shQuote(jsonlite::toJSON(f$p, auto_unbox = TRUE)),
                          shQuote(jsonlite::toJSON(f$v))),
                   stdout = TRUE, stderr = TRUE)
    got <- tryCatch(jsonlite::fromJSON(tail(out, 1)),
                    error = function(e) stop("python port failed on ", f$id, ": ",
                                             paste(out, collapse = "\n")))
    got <- as.character(got)
    expect(setequal(got, f$ok),
           sprintf("python, %s: got [%s], want [%s]", f$id,
                   paste(got, collapse = ","), paste(f$ok, collapse = ",")))
  }
})
