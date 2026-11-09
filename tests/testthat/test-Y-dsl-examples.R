## Group Y — the examples in docs/FLUMEN_DSL.md must still be true.
##
## That file exists so someone (or something) with no prior knowledge can write
## a workflow from it. Its failure mode is not being incomplete: it is being
## WRONG, because a reader then writes a broken workflow with confidence. Over
## one working session five statements in it turned out not to match the code --
## section 6.10 twice, the Python arrival shapes, the list-return contract.
## Every one of them would have been caught by building the snippet.
##
## Nothing is executed: run_workflow() and preview_workflow() are stubbed, so a
## documentation check never spawns a worker.
##
##   Y1  every ```r block parses
##   Y2  every self-contained example BUILDS (new_workflow / task constructors)
##   Y3  every block marked "REJECTED" is in fact rejected
##
## Blocks that are API signatures (they carry a "# R/<file>" pointer) or
## templates (a bare `...` where a value belongs) are parse-only by nature.

DSL <- normalizePath(file.path("..", "..", "docs", "FLUMEN_DSL.md"), mustWork = FALSE)

dsl_blocks <- function(path) {
  src <- readLines(path, warn = FALSE)
  out <- list(); cur <- NULL; st <- 0L; sec <- ""
  for (i in seq_along(src)) {
    l <- src[i]
    if (grepl("^#", l)) sec <- trimws(gsub("^#+", "", l))
    ## Accept every fence spelling a markdown editor may leave behind:
    ## ```r  /  ``` r  /  ```{r}  — RStudio's visual editor rewrites the first
    ## into the second on save, which silently emptied this whole group once.
    if (grepl("^\\s*```\\s*\\{?r\\b", l)) { cur <- character(0); st <- i; csec <- sec; next }
    if (!is.null(cur) && trimws(l) == "```") {
      out[[length(out) + 1L]] <- list(line = st, sec = csec,
                                      code = paste(cur, collapse = "\n"))
      cur <- NULL; next
    }
    if (!is.null(cur)) cur <- c(cur, l)
  }
  out
}

is_signature <- function(code) grepl("#\\s*R/", code)                  # points at the source
## A template is an example with a "..." standing in for what the reader supplies.
## Two spellings occur in the document and BOTH must be recognised, or the block is
## handed to Y2, which then fails on R's own "'...' used in an incorrect context"
## and reads as a documentation error when the document is right:
##   inline      new_R_task(task_id = ..., input = ...)
##   on its own line, as the omitted remaining arguments:
##                new_R_task(
##                  task_id = "preprocess",
##                  ...
##                )
## The second alternative is anchored to a whole line on purpose: a "..." sharing a
## line with other tokens is a real signature (`function(x, ...)`) inside a genuine
## example, and skipping that would silently shrink Y2's coverage.
is_template  <- function(code)
  grepl("[=(]\\s*\\.\\.\\.\\s*[,)\n]", code) ||
  grepl("(^|\n)[[:space:]]*\\.\\.\\.[[:space:]]*(,|\\))?[[:space:]]*($|\n)", code)
is_rejected  <- function(code) grepl("REJECTED", code, fixed = TRUE)
is_buildable <- function(code) grepl("new_workflow\\(|new_[A-Za-z]+_task\\(", code)

build_block <- function(code) {
  e <- new.env(parent = globalenv())
  ## Build only -- never run: the point is to check the documentation, not to
  ## start pipelines as a side effect.
  assign("run_workflow",     function(...) invisible(NULL), envir = e)
  assign("preview_workflow", function(...) invisible(NULL), envir = e)
  tryCatch({ eval(parse(text = code), envir = e); NA_character_ },
           error = function(err) conditionMessage(err))
}

blocks <- if (file.exists(DSL)) dsl_blocks(DSL) else list()

test_that("Y0 — the DSL document is present and holds code blocks", {
  skip_if(!file.exists(DSL), "docs/FLUMEN_DSL.md not reachable from the test dir")
  expect_gt(length(blocks), 20L)
})

test_that("Y1 — every code block parses", {
  skip_if(!length(blocks), "no blocks")
  for (b in blocks) {
    ok <- tryCatch({ parse(text = b$code); TRUE }, error = function(e) conditionMessage(e))
    expect_true(isTRUE(ok),
                label = sprintf("block at line %d [%s] parses -- %s",
                                b$line, substr(b$sec, 1, 30),
                                if (isTRUE(ok)) "" else substr(ok, 1, 90)))
  }
})

test_that("Y2 — every self-contained example builds", {
  skip_if(!length(blocks), "no blocks")
  targets <- Filter(function(b)
    is_buildable(b$code) && !is_signature(b$code) &&
    !is_template(b$code) && !is_rejected(b$code), blocks)
  expect_gt(length(targets), 8L)          # guard against the filter silently emptying
  for (b in targets) {
    msg <- build_block(b$code)
    expect_true(is.na(msg),
                label = sprintf("example at line %d [%s] builds -- %s",
                                b$line, substr(b$sec, 1, 30), substr(msg, 1, 110)))
  }
})

test_that("Y3 — examples marked REJECTED are really rejected", {
  skip_if(!length(blocks), "no blocks")
  targets <- Filter(function(b) is_rejected(b$code) && is_buildable(b$code), blocks)
  skip_if(!length(targets), "no REJECTED examples")
  for (b in targets) {
    msg <- build_block(b$code)
    expect_false(is.na(msg),
                 label = sprintf("counter-example at line %d [%s] must fail to build",
                                 b$line, substr(b$sec, 1, 30)))
  }
})
