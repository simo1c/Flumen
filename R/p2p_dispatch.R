## Consumer-side state for cross-worker output_req resolution. Coalesces
## parameter_requests so that N wrappers asking for the same upstream output
## trigger a single P2P transfer; later requests are served from the on-disk
## cache.
##
## Every parameter_request must be registered with `p2p_register_or_check()`
## before it is sent, and every parameter_requested response must call
## `p2p_complete_request()`.

## Fresh isolated environment holding the pending P2P resolutions.
p2p_make_env <- function(){
  new.env(parent = emptyenv())
}

## Cache key for a (producer exe_id, output_instance, list-vs-scalar) triple
## plus the three narrowings.
p2p_make_key <- function(exe_id, out_instance, is_list_req,
                         output_sel = NULL, output_path_sel = NULL, file_sel = NULL){
  paste(exe_id,
        deparse(out_instance),
        as.integer(isTRUE(is_list_req)),
        ## All three narrowings enter the key. NULL contributes "", so a request
        ## narrowing nothing keys the same as one with no narrowings at all.
        sel_key_part(output_sel),
        sel_key_part(output_path_sel),
        sel_key_part(file_sel),
        sep = ":")
}

## A narrowing is active when it is one non-empty string. NULL, NA and "" all
## mean "do not narrow", which is the shape the request message uses on the wire.
sel_active <- function(s){
  !is.null(s) && length(s) == 1L && !is.na(s) && nzchar(as.character(s))
}

sel_key_part <- function(s) if(sel_active(s)) as.character(s) else ""

## The constituent of `par_val` produced by `exe_id`. Returns NULL when no
## constituent carries that exe_id; callers report that rather than falling back
## to a sibling.
output_req_for <- function(par_val, exe_id){
  if(inherits(par_val, "output_req_list")){
    for(.it in par_val)
      if(inherits(.it, "output_req") &&
         identical(as.character(.it$exe_id), as.character(exe_id))) return(.it)
    return(NULL)
  }
  if(inherits(par_val, "output_req") &&
     identical(as.character(par_val$exe_id), as.character(exe_id))) return(par_val)
  NULL
}

## The three narrowings a request can carry, read off the constituent that
## belongs to `exe_id`.
output_req_selectors <- function(par_val, exe_id){
  .req <- output_req_for(par_val, exe_id)
  list(output_sel      = if(is.null(.req)) NULL else .req$output_sel,
       output_path_sel = if(is.null(.req)) NULL else .req$output_path_sel,
       file_sel        = if(is.null(.req)) NULL else .req$file_sel)
}

## A resolved pattern as an anchored PATH regex: wildcard tokens become the
## capture, every other character is literal path text, escaped.
##   "a/{{k}}.txt" -> "^a/[^/]*\\.txt$"
##   "out/{{any}}" -> "^out/.*$"     (recursive: the capture crosses /)
##
## The capture crosses "/" only when the pattern is RECURSIVE, by the same
## predicate that chooses list.files(recursive =) for it.
##
## `at_basename` is inserted where the FILE NAME begins, which is what the
## sibling lookahead of p2p_derive_select() needs.
p2p_pattern_to_regex <- function(pattern, at_basename = ""){
  p   <- normalize_path_lexical(as.character(pattern)[1])
  cap <- if(is_recursive_wildcard(p)) ".*" else "[^/]*"
  ## "\001" cannot occur in a path, so it survives the escaping below and marks
  ## where the wildcards were.
  rx <- function(x){
    x <- gsub("[{][{][^}]+[}][}]", "\001", x)
    x <- gsub("[{][^}]+[}]",       "\001", x)
    gsub("\001", cap, p2p_escape_literal(x), fixed = TRUE)
  }
  d      <- dirname(p)
  dir_rx <- if(identical(d, ".")) "" else paste0(rx(d), "/")
  paste0("^", dir_rx, at_basename, rx(basename(p)), "$")
}

## The literal head of a resolved pattern's basename: everything before the
## first placeholder. "outs/A_{{any}}.txt" -> "A_".
p2p_pattern_literal_head <- function(pattern){
  b <- basename(strip_backticks(as.character(pattern)[1]))
  sub("[{].*$", "", b)
}

## Escape every regex metacharacter: the head is LITERAL text, not a glob.
p2p_escape_literal <- function(x){
  gsub("([.\\\\|()\\[\\]{}^$*+?])", "\\\\\\1", x, perl = TRUE)
}

## NULL (no filter) when the consumer pattern is NOT more specific than the
## producer's output pattern (their basename regexes coincide); else the
## consumer's basename regex, used by the producer to ship only matching files.
##
## `sibling_patterns`: the SAME parameter's resolved pattern in the other
## instances of this consumer task. Longest match wins: a file whose name starts
## with a MORE SPECIFIC sibling's literal head belongs to that sibling and is
## excluded with a negative lookahead. With no siblings the filter selects
## everything.
p2p_derive_select <- function(consumer_pattern, producer_pattern,
                              sibling_patterns = NULL){
  if(is.null(consumer_pattern) || is.null(producer_pattern)) return(NULL)
  cr <- p2p_pattern_to_regex(consumer_pattern)
  ## EVERY declared entry, not producer_pattern[1]: a bundle output declares
  ## several paths under one name. unique() so a bundle repeating one path does
  ## not turn into a narrowing that narrows nothing.
  prs <- unique(vapply(as.character(producer_pattern), p2p_pattern_to_regex,
                       character(1), USE.NAMES = FALSE))
  ## The ask is the whole offer only when the offer is ONE entry and it is that
  ## one. An EMPTY offer returns NULL too: the degenerate input fails towards
  ## "deliver everything".
  if(length(prs) == 0L ||
     (length(prs) == 1L && identical(cr, prs[[1]]))) return(NULL)

  la <- ""
  if(length(sibling_patterns)){
    mine <- p2p_pattern_literal_head(consumer_pattern)
    if(nzchar(mine)){
      heads <- unique(vapply(as.character(sibling_patterns),
                             p2p_pattern_literal_head, character(1)))
      ## strictly more specific: starts with mine, and is longer
      more_specific <- heads[nzchar(heads) & heads != mine &
                             startsWith(heads, mine)]
      if(length(more_specific))
        la <- paste0("(?!", p2p_escape_literal(more_specific), ")", collapse = "")
    }
  }
  ## The lookahead goes at the start of the FILE NAME, not spliced after "^":
  ## there it would sit at the start of the PATH and always be satisfied.
  if(nzchar(la)) p2p_pattern_to_regex(consumer_pattern, at_basename = la) else cr
}

## `output_sel` names a declared producer output. Objects have no path to match
## on, so the declared output name is the only stable identity a consumer can
## name.

## Filter a producer file list + its PARALLEL metadata vectors (data_type,
## is_arrow, elem_names, group_names) by the selectors, keeping them
## length-aligned. select NULL/"" -> identity.
##
## The three narrowings are applied IN SEQUENCE and they compose: each
## contributes a keep mask, the masks are ANDed, and the vectors are subset ONCE
## at the end. Both path narrowings are anchored PATH regexes
## (p2p_pattern_to_regex) matched against the FULL path.
p2p_filter_param <- function(param_req, data_type, result_is_arrow,
                             result_elem_names, result_group_names,
                             output_sel = NULL, output_path_sel = NULL,
                             file_sel = NULL){

  n <- length(param_req)
  keep <- rep(TRUE, n)

  if(sel_active(output_sel)){
    ## Select by declared OUTPUT NAME, matched against the label the PRODUCER
    ## states per result. A producer that states nothing cannot be resolved by
    ## name, so fail loudly rather than hand the consumer an empty parameter.
    want <- as.character(output_sel)
    gn   <- if(length(result_group_names) == n) as.character(result_group_names)
            else rep("", n)
    gn[is.na(gn)] <- ""
    result_group_names <- gn
    keep <- keep & (gn == want)
    if(!any(keep))
      stop(sprintf(paste0("output '%s' not among the producer's results (seen: %s). ",
                          "Fix: return a named list whose names are the declared outputs."),
                   want, paste(ifelse(nzchar(gn), gn, "<unnamed>"), collapse = ", ")),
           call. = FALSE)
  }

  if(sel_active(output_path_sel) || sel_active(file_sel)){
    ## Lexically normalised, so a top-level declared output delivered as
    ## "./x.txt" is compared against a pattern written "x.txt".
    full <- normalize_path_lexical(
              vapply(param_req, function(p) as.character(p)[1], character(1)))
    for(.rx in c(output_path_sel, file_sel)){
      if(!sel_active(.rx)) next
      keep <- keep & grepl(.rx, full, perl = TRUE)
    }
  }

  if(all(keep)){
    return(list(param_req = param_req, data_type = data_type,
                is_arrow = result_is_arrow, elem_names = result_elem_names,
                group_names = result_group_names))
  }

  ## Subset every vector by the SAME mask, and only those whose length says they
  ## are parallel to the elements.
  sub_par <- function(v) if(length(v) == n) v[keep] else v
  list(param_req = param_req[keep],
       data_type = sub_par(data_type),
       is_arrow  = sub_par(result_is_arrow),
       elem_names    = sub_par(result_elem_names),
       group_names   = sub_par(result_group_names))
}

## State machine entry point. One of three actions returned:
##   list(action = "first")    -> caller MUST emit the P2P parameter_request
##   list(action = "waiting")  -> caller MUST mark wrapper as wait_parameter
##   list(action = "cached", par_req=..., par_req_types=..., par_req_is_arrow=...,
##        is_multi_cross=..., is_from_list_cross=..., paths=...)
##                              -> caller MUST apply resolution from cache
##
## A "completed" entry whose on-disk paths have disappeared is treated as stale:
## the entry is reset to "in_transit" and "first" is returned so the caller
## refetches.
##
## `started`/`seen`/`quiet_checks` carry the progress bookkeeping that
## check_stalled_transfers() reads. They are written at the only two places an
## in_transit entry is born.
p2p_register_or_check <- function(env, key, waiter){
  if(!exists(key, envir = env, inherits = FALSE)){
    assign(key,
           list(status = "in_transit", waiters = list(waiter),
                started = Sys.time(), seen = 0L, quiet_checks = 0L),
           envir = env)
    return(list(action = "first"))
  }

  entry <- get(key, envir = env, inherits = FALSE)

  if(entry$status == "in_transit"){
    entry$waiters <- c(entry$waiters, list(waiter))
    assign(key, entry, envir = env)
    return(list(action = "waiting"))
  }

  ## "completed" is the only other status an entry can carry, so the two
  ## branches are exhaustive.
  paths_ok <- length(entry$paths) == 0L ||
              all(file.exists(unlist(entry$paths)))
  if(paths_ok){
    return(list(action            = "cached",
                par_req           = entry$par_req,
                par_req_types     = entry$par_req_types,
                par_req_is_arrow  = entry$par_req_is_arrow,
                par_req_elem_names = entry$par_req_elem_names,
                is_multi_cross    = entry$is_multi_cross,
                is_from_list_cross = entry$is_from_list_cross,
                paths             = entry$paths))
  }
  ## Stale: file removed externally between transfers. Reset and refetch.
  assign(key,
         list(status = "in_transit", waiters = list(waiter),
              started = Sys.time(), seen = 0L, quiet_checks = 0L),
         envir = env)
  return(list(action = "first"))
}

## Record which peer this fetch was sent to, once the caller has resolved the
## producer's transport. Read by the stalled-transfer report, which otherwise
## cannot name the machine that stopped answering.
p2p_note_peer <- function(env, key, peer){
  if(!exists(key, envir = env, inherits = FALSE)) return(invisible(NULL))
  entry <- get(key, envir = env, inherits = FALSE)
  entry$peer <- as.character(peer)[1]
  assign(key, entry, envir = env)
  invisible(NULL)
}

## Mark a P2P request completed and return the list of waiters to fan out to.
## Caller must invoke apply_p2p_resolution() on each waiter and then, if all of
## that wrapper's params are ready, prepare_code_and_parameters_for_wrapper() +
## send_message_by_chunks().
##
## A late or duplicate response (key absent / already completed) warns and
## returns empty waiters; it does NOT abort.
p2p_complete_request <- function(env, key, paths,
                                  par_req, par_req_types, par_req_is_arrow,
                                  is_multi_cross, is_from_list_cross,
                                  par_req_elem_names = NULL){
  if(!exists(key, envir = env, inherits = FALSE)){
    warning(flumen_warn(
      call    = "p2p_complete_request()",
      warning = sprintf("key '%s' is not registered, a P2P response with no prior request",
                        key)), call. = FALSE)
    return(list())
  }

  entry <- get(key, envir = env, inherits = FALSE)
  if(entry$status != "in_transit"){
    warning(flumen_warn(
      call    = "p2p_complete_request()",
      warning = sprintf("key '%s' is already in status '%s', a duplicate response",
                        key, entry$status)), call. = FALSE)
  }

  waiters <- entry$waiters

  assign(key,
         list(
           status              = "completed",
           paths               = paths,
           par_req             = par_req,
           par_req_types       = par_req_types,
           par_req_is_arrow    = par_req_is_arrow,
           par_req_elem_names  = par_req_elem_names,
           is_multi_cross      = is_multi_cross,
           is_from_list_cross  = is_from_list_cross
         ),
         envir = env)

  waiters
}

## Drop all entries. Called from terminate / error_notify cleanup paths.
p2p_reset_env <- function(env){
  rm(list = ls(envir = env), envir = env)
  invisible(NULL)
}
