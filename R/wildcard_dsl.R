## Wildcard user-facing DSL: new_wildcard / new_wildcard_list constructors,
## plus the runtime helpers that mutate par_list/par_names entries when a
## wildcard slot is resolved with a concrete value (used by the supervisor's
## co-local and P2P parameter-resolution paths).
##
## Note: the lower-level wildcard pattern matchers and expansion engine
## live in R/wildcard_engine.R; this file only contains the constructors and
## the post-resolution slot-update helpers.


## Object values must be named. An atomic value is its own key, so a character
## or numeric vector needs nothing (both constructors auto-key it above), but an
## object has no natural one and the only key left would be its POSITION.
##
## A position is not an identity: insert a value or reorder the list and the
## same key denotes a different dataset, in the output paths, in the instance ids
## and in the per-instance log.
## `was_atomic` says which form the caller wrote, because by the time the check
## runs both look the same: an atomic vector is turned into a named list whose
## names ARE its values, so a missing name means an empty VALUE, not a missing
## label. Without the flag the guard would tell someone who passed strings that
## their "object values" need naming, and offer a remedy that does not apply.
check_object_wildcard_names <- function(values, wc_name, was_atomic = FALSE){
  if(is.atomic(values) || !length(values)) return(invisible(TRUE))
  nms <- names(values)
  if(!is.null(nms) && all(nzchar(nms))) return(invisible(TRUE))

  if(was_atomic)
    stop(flumen_reject(
      call  = "new_wildcard()",
      error = sprintf(paste("wildcard '%s' has an empty value, and an atomic value is",
                            "its own key, so the instance it would create has no identity"),
                      wc_name),
      fix   = paste("every value must be a non-empty string. Check for a blank line",
                    "or a trailing separator in the source")), call. = FALSE)

  stop(flumen_reject(
    call  = "new_wildcard()",
    error = sprintf(paste("wildcard '%s' has %d object value(s) and %s; without names",
                          "the only key left is the position, and a position is not an identity"),
                    wc_name, length(values),
                    if(is.null(nms)) "none of them is named"
                    else "at least one of them has no name"),
    fix   = paste("name them, list(name1 = <obj>, name2 = <obj>); atomic values need no",
                  "names, there the value is its own key")), call. = FALSE)
}

## Two entries sharing a wildcard KEY are the same instance: expansion is keyed
## by the name, so the second collapses onto the first.
##
## That is intended, and relied upon, for the atomic form. `c("a","a","b")` is a
## sample sheet listing a sample twice, and there the name IS the value, so the
## discarded entry is identical to the kept one and nothing is lost: it expands
## to two instances (`k=a`, `k=b`).
##
## It is NOT intended when the values differ. `list(D1 = "X", D1 = "Y")` yields
## ONE instance holding "X", and "Y" never reaches any task.
##
## So reject only the case that loses data, and leave the identical-value
## collapse alone. Build-time, before anything runs.
check_duplicate_wildcard_keys <- function(values, wc_name){
  nms <- names(values)
  if(is.null(nms) || !any(duplicated(nms))) return(invisible(TRUE))

  for(k in unique(nms[duplicated(nms)])){
    idx   <- which(nms == k)
    first <- values[[idx[1]]]
    same  <- vapply(idx[-1], function(i) identical(values[[i]], first), logical(1))
    if(all(same)) next   ## same id declared twice with the same value: harmless

    stop(flumen_reject(
      call  = "new_wildcard()",
      error = sprintf(paste("wildcard '%s' has %d entries named '%s' carrying DIFFERENT",
                            "values, and expansion is keyed by the name, so only the",
                            "first would become a task instance"),
                      wc_name, length(idx), k),
      fix   = "give each entry its own name"), call. = FALSE)
  }
  invisible(TRUE)
}


#' Declare a wildcard
#'
#' Creates a named wildcard with a set of values. When referenced in task
#' inputs/outputs (e.g., `"{sample}"`), each value spawns a separate execution
#' instance.
#'
#' @param name Character. The wildcard name (referenced as `{name}`).
#' @param values Atomic vector or named list. Values the wildcard expands to.
#'
#' @return An object of class `wildcard_object`.
#'
#' @seealso [new_wildcard_list()]
#'
#' @examples
#' new_wildcard("sample", c("3019", "3022", "3023"))
#'
#' @export
new_wildcard <- function(name, values){

  was_atomic <- is.atomic(values)
  if(was_atomic){
    value_names <- values
    values <- as.list(values)
    names(values) <- value_names
  }

  ## The names ARE the identity, and register_op hashes them: that hash
  ## serialises, and serialisation tells a deferred string (what as.character()
  ## returns) from a materialised one holding the same characters, so two equal
  ## values would get two identities and escape the collapse. `[]` materialises
  ## them once here rather than at every registration. Names only: always a
  ## character vector, so never an S4 object, which `[]` would refuse.
  if(!is.null(names(values))) names(values) <- names(values)[]

  check_object_wildcard_names(values, name, was_atomic)
  check_duplicate_wildcard_keys(values, name)

  will <- list(values)
  names(will) <- name
  class(will) <- "wildcard_object"
  return(will)
}

#' Combine wildcards into a list
#'
#' Aggregates wildcard definitions passed to [new_workflow()]. Accepts either
#' [new_wildcard()] objects or named alias arguments
#' (e.g., `sample = c("a", "b")`).
#'
#' @param ... Named atomic vectors, named lists, or [new_wildcard()] objects.
#'
#' @return An object of class `wildcard_list_object`.
#'
#' @seealso [new_wildcard()], [new_workflow()]
#'
#' @examples
#' new_wildcard_list(
#'   ref    = "mmu",
#'   sample = c("3019", "3022", "3023")
#' )
#'
#' @export
new_wildcard_list <- function(...){
  args <- list(...)
  wildcards <- list()

  ## Checked BEFORE each assignment: `wildcards[[name]] <- ...` replaces an
  ## earlier wildcard of the same name, so a check made after the loop sees no
  ## duplicate and new_wildcard_list(s = c("a", "b"), s = "c") keeps s = "c"
  ## alone, silently.
  refuse_duplicate <- function(name){
    if(name %in% names(wildcards))
      stop(flumen_reject(
        call  = "new_wildcard_list()",
        error = sprintf("the wildcard '%s' is declared more than once", name),
        fix   = "give each wildcard its own name, or merge their values into one"),
        call. = FALSE)
  }

  for (i in seq_along(args)) {
    arg <- args[[i]]
    nm  <- if (!is.null(names(args))) names(args)[i] else ""

    if (inherits(arg, "wildcard_object")) {
      wc_name <- names(arg)[1]
      ## A hand-built wildcard_object reaches here without having passed
      ## through new_wildcard(), so its checks and its key materialisation are
      ## repeated here: see that function for why the keys carry the identity.
      check_object_wildcard_names(arg[[1]], wc_name)
      check_duplicate_wildcard_keys(arg[[1]], wc_name)
      .wc <- arg[[1]]
      if(!is.null(names(.wc))) names(.wc) <- names(.wc)[]
      refuse_duplicate(wc_name)
      wildcards[[wc_name]] <- .wc
    } else if (!is.null(nm) && nm != "") {
      was_atomic <- is.atomic(arg)
      if (was_atomic) {
        values <- as.list(arg)
        names(values) <- arg
      } else {
        values <- arg
      }
      if(!is.null(names(values))) names(values) <- names(values)[]
      check_object_wildcard_names(values, nm, was_atomic)
      check_duplicate_wildcard_keys(values, nm)
      refuse_duplicate(nm)
      wildcards[[nm]] <- values
    } else {
      stop(flumen_reject(
        call  = "new_wildcard_list()",
        error = "unnamed arguments that are not a wildcard_object are not supported",
        fix   = "pass wildcards built with new_wildcard(), or name the argument"),
        call. = FALSE)
    }
  }

  if(any(duplicated(names(wildcards)))){
    stop(flumen_reject(
      call  = "new_wildcard_list()",
      error = "the wildcards must have unique names",
      fix   = "give each wildcard its own name"), call. = FALSE)
  }

  class(wildcards) <- "wildcard_list_object"
  return(wildcards)
}


## -- runtime helpers used by the supervisor's parameter resolution -----------

update_param_wildcard_with_value <- function(par_list,
                                             value,
                                             index,
                                             is_arrow = FALSE,
                                             data_type = NULL,
                                             elem_names = NULL,
                                             exe_id) {

  target_element <- par_list[[index]]

  ## A dependency parameter is a list of constituents (group_by_name), one
  ## per producer, a single producer included: there is ONE shape to fill.
  ## Anything else here is the container having been replaced before its
  ## producer delivered, an engine invariant and not a workflow error.
  if (!inherits(target_element, "output_req_list")) {
    stop(flumen_fail(
      call  = "update_param_wildcard_with_value()",
      error = sprintf(paste("a value arrived from producer '%s' for parameter %d,",
                            "whose container is a '%s' and not a list of constituents"),
                      as.character(exe_id)[1], index, class(target_element)[1]),
      fix   = paste("this is an engine invariant, not a workflow error:",
                    "report the workflow and the producer id")), call. = FALSE)
  }

  if (!is.list(value)) value <- list(value)

  ## Per-element handling. The helper is reached from two callers with
  ## different payload shapes: apply_p2p_resolution (cross-worker) and
  ## update_co_local_dependency (colocal). A raw element is an encoded
  ## payload; a native element (a path string, a shell prefix) is a value.
  ##
  ## A raw constituent is NOT decoded: the producer's bytes are what the
  ## consumer's wrapper will decode, and decoding them here only to encode them
  ## again in prepare_code_and_parameters_for_wrapper is a buffer copy per
  ## constituent.
  ##
  ## The producer's (type, codec) is pinned ON the element rather than kept in
  ## a vector parallel to the container: resolution is progressive (one
  ## constituent at a time, from producers that finish in any order) and
  ## expand_wrapped_params flattens composite producers, so a parallel vector
  ## would have to stay aligned through both reorderings. Travelling with the
  ## payload, the metadata cannot fall out of step with it. The attributes are
  ## consumed and removed in prepare_code_and_parameters_for_wrapper; they
  ## never reach the wire. A native element gets the same label when the
  ## caller gives one: a path is "file_path" whatever its constituent count
  ## (a received "file" is on disk by now, hence the same label).
  ##
  ## One label is NOT forwarded: the bare "wrapped" sentinel, which is decoded
  ## here instead. It means the producer could not Arrow-encode this element,
  ## so the bytes are qs2 and carry no type of their own, while "wrapped:raw",
  ## one character apart, is the contract for the user's OWN bytes, which the
  ## consumer must hand over undecoded. Passthrough buys nothing for the
  ## sentinel anyway: a Python consumer is refused either way, and an R one
  ## needs the qs2 decode regardless of who does it.
  value <- lapply(seq_along(value), function(i){
    v_i <- value[[i]]
    ty  <- if (is.null(data_type) || i > length(data_type)) NA_character_
           else as.character(data_type[[i]])[1]
    if (!is.raw(v_i)) {
      if (!is.na(ty) && nzchar(sub("^wrapped:?", "", ty))) {
        attr(v_i, "flumen_elem_type")     <- if (identical(ty, "file")) "file_path" else ty
        attr(v_i, "flumen_elem_is_arrow") <- FALSE
        attr(v_i, "flumen_elem_wrapped")  <- TRUE
      }
      return(v_i)
    }
    if (is.na(ty) || !nzchar(sub("^wrapped:?", "", ty)))
      return(decode_message_data(v_i, data_type[[i]], isTRUE(is_arrow[[i]])))
    attr(v_i, "flumen_elem_type")     <- ty
    attr(v_i, "flumen_elem_is_arrow") <- isTRUE(is_arrow[[i]])
    attr(v_i, "flumen_elem_wrapped")  <- TRUE
    v_i
  })

  ## Preserve names of the producer's user-returned list across IPC.
  ## When the producer returned `list(a=..., b=...)`, the wrapper splits the
  ## list element-by-element into N raw blobs; names live in `elem_names`
  ## metadata. Re-attach them so consumers see `xs[[i]]$a` instead of the
  ## stripped positional `xs[[i]][[1]]`.
  if (!is.null(elem_names) && length(elem_names) == length(value) &&
      any(nzchar(elem_names))) {
    names(value) <- elem_names
  }

  ## The arriving value goes into the slot of ITS OWN producer, never into
  ## "the first still-pending slot". The container is read BY PRODUCER in three
  ## places (the P2P request builder, the response handler and the co-local
  ## resolver, all through output_req_selectors), and filling it by POSITION
  ## would make those reads unsound.
  ##
  ## Identity also makes the aggregate DETERMINISTIC: it arrives in declaration
  ## order rather than completion order, which the DSL describes as
  ## topology-dependent and unsafe as an identity.
  sub_indices <- which(vapply(target_element,
                              function(x) inherits(x, "output_req"), logical(1)))
  sub_indices <- sub_indices[vapply(target_element[sub_indices],
                                    function(x) identical(as.character(x$exe_id),
                                                          as.character(exe_id)),
                                    logical(1))]

  ## No slot is waiting for this producer. By construction this cannot
  ## happen: a wrapper announces itself once, each dependency row is walked
  ## once, and both delivery paths clear Param_dependency as they resolve, so
  ## a second delivery for the same producer is never requested. There is no
  ## retransmission either. Stated HERE, at the single point where a value
  ## enters the container: if it fires, the invariant is false and the run must
  ## not continue.
  if (length(sub_indices) == 0L) {
    stop(flumen_fail(
      call  = "update_param_wildcard_with_value()",
      error = sprintf(paste("a value arrived from producer '%s' for parameter %d,",
                            "and no constituent of that aggregation was waiting for it"),
                      as.character(exe_id)[1], index),
      fix   = paste("this is an engine invariant, not a workflow error:",
                    "report the workflow and the producer id")), call. = FALSE)
  }

  target_element[[sub_indices[1]]] <- value
  par_list[[index]] <- target_element

  return(par_list)
}

update_param_names_with_value <- function(par_names,
                                          value,
                                          index) {

  start <- if(index > 1)par_names[seq_len(index - 1)] else NULL

  len_par_names <- length(par_names)
  end <- if (index < len_par_names) par_names[seq(index + 1, len_par_names)] else NULL
  new_par_names <- c(start, value, end)

  return(new_par_names)
}

