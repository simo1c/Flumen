## User-facing diagnostics: one shape for every message flumen prints.
##
##   Flumen declaration rejected.
##     call      : new_workflow()
##     task      : align
##     error     : <what happened>
##     fix       : <what to do>
##
## The shape lives here, not at the call sites, so the ~150 messages stay uniform:
## a call site passes fields, never layout. Fields print in write order and an
## empty one is dropped, so a caller with no task or parameter context omits it.
##
## Values are wrapped to the terminal width with a hanging indent to the colon
## column: they are interpolated (task ids, paths, wildcard values), and a break
## chosen while writing the message lands wrong as soon as a real path is longer
## than the example one. `nowrap` exempts a field whose own line breaks carry
## meaning, such as stderr.
flumen_block <- function(header, fields, nowrap = character(0), width = NULL){
  keep   <- vapply(fields, function(v)
                   length(v) > 0L && any(nzchar(as.character(v))), logical(1))
  fields <- fields[keep]
  if(!length(fields)) return(header)

  field_w <- 10L
  w       <- if(is.null(width)) getOption("width", 80L) else width
  ## Floor so a narrow terminal still leaves room for the text, ceiling so a wide
  ## one does not stretch the block into unreadably long lines.
  w       <- max(60L, min(as.integer(w), 100L))
  ind     <- field_w + 4L                    ## 2 leading spaces + label + ": "

  out <- character(0)
  for(nm in names(fields)){
    txt  <- paste(as.character(fields[[nm]]), collapse = " ")
    head <- sprintf("  %-*s: ", field_w, nm)
    body <- if(nm %in% nowrap) unlist(strsplit(txt, "\n", fixed = TRUE))
            else               wrap_keeping_calls(txt, w - ind)
    if(!length(body)) body <- ""
    out <- c(out, paste0(head, body[1L]),
             if(length(body) > 1L) paste0(strrep(" ", ind), body[-1L]))
  }
  paste(c(header, out), collapse = "\n")
}

## Almost every `fix` ends in a call the user is meant to copy, and a wrap that
## breaks at every space splits it, leaving `new_input(x =` on one line and the
## rest on the next. One level of nesting covers the shapes that occur here, such
## as new_dep("a", value = new_file("p/{wc}.ext")). An `argument = ` prefix is
## taken with the call, so `task_list = list(my_task)` is one unit too: a break
## after the `=` reads as a truncated example.
CODE_CALL_PATTERN <- paste0("(?:[A-Za-z_.][A-Za-z0-9_.]*[ ]*=[ ]*)?",
                            "[A-Za-z_.][A-Za-z0-9_.]*\\((?:[^()]|\\([^()]*\\))*\\)")

## Wrap by measuring the text itself, rather than hiding the spaces inside a call
## behind a marker and passing it to strwrap(): strwrap does not count a control
## character towards the width, so each protected space steals a column and the
## line overflows the terminal. A call is one token, everything else splits on
## whitespace, and the line is filled greedily against the real width. A token
## longer than the width still overflows, which is the right trade: never cut a
## path or a call in half.
wrap_keeping_calls <- function(txt, width){
  chars <- strsplit(txt, "", fixed = TRUE)[[1]]
  if(!length(chars)) return("")

  ## Mark the whitespace that may NOT be broken, rather than cutting the string
  ## at the matches: cutting detaches whatever sits against the call, producing
  ## text such as `new_input() , but has class`.
  inside <- rep(FALSE, length(chars))
  m <- gregexpr(CODE_CALL_PATTERN, txt, perl = TRUE)[[1]]
  if(m[1L] != -1L){
    len <- attr(m, "match.length")
    for(k in seq_along(m)) inside[m[k]:(m[k] + len[k] - 1L)] <- TRUE
  }
  brk <- chars %in% c(" ", "\t", "\n") & !inside
  if(!any(!brk)) return("")

  ## A token is a maximal run of characters carrying no breaking whitespace, so
  ## a call keeps its own spaces and whatever is written against it.
  grp  <- cumsum(brk)
  toks <- vapply(split(chars[!brk], grp[!brk]), paste0, collapse = "", character(1))
  toks <- unname(toks[nzchar(toks)])
  if(!length(toks)) return("")

  lines <- character(0)
  cur   <- toks[1L]
  for(t in toks[-1L]){
    if(nchar(cur) + 1L + nchar(t) <= width) cur <- paste0(cur, " ", t)
    else { lines <- c(lines, cur); cur <- t }
  }
  c(lines, cur)
}

## The headers flumen raises. R prints its own "Error: " or "Warning message:"
## above them, but the header stays because log files carry no such prefix and
## there it is the only line saying what the block is.
##
## A message travelling to the master inside an error_notify is not built here: it
## arrives in the `error` field of flumen_fail()'s own block, so it stays one
## sentence ending in "Fix: ...", and a block would nest inside a block.
## The `call` field already says what was refused, so the header is derived from
## it: an invocation, a declaration, or an engine invariant (no `call` at all).
flumen_reject <- function(...){
  f <- list(...)
  invocations <- c("run_workflow()", "start_worker()", "init_sockets()")
  header <- if(is.null(f$call))             "Flumen internal error."
            else if(f$call %in% invocations) "Flumen run rejected."
            else                             "Flumen declaration rejected."
  flumen_block(header, f)
}

flumen_warn   <- function(...) flumen_block("Flumen workflow warning.",  list(...))

flumen_launch_fail <- function(...)
  flumen_block("Flumen worker launch failed.", list(...))

flumen_fail <- function(..., nowrap = character(0))
  flumen_block("Flumen workflow failed.", list(...), nowrap = nowrap)
