
wildcard_expansion_and_alignment <- function(op, wildcard_list, sibling_patterns = NULL){
  UseMethod("wildcard_expansion_and_alignment")
}

from_task_object_to_exe_object <- function(task_object, exe_id){
  UseMethod("from_task_object_to_exe_object")
}

get_all_task_ids <- function(task_list){
  return(
    unique(sapply(task_list, get_object_id))
  )
}

#' @export
#' @noRd
wildcard_expansion_and_alignment.task_list_object <- function(op, wildcard_list, sibling_patterns = NULL){
  task_list <- op
  list_of_list_expanded_op <- lapply(task_list, function(task_object){
    expanded_list_of_task_object <- wildcard_expansion_and_alignment(task_object, wildcard_list, sibling_patterns)
  })

  return(list_of_list_expanded_op)
}

#' @export
#' @noRd
wildcard_expansion_and_alignment.task_object <- function(op, wildcard_list, sibling_patterns = NULL){
  task_object <- op

  par_list_object <- task_object$par_list
  
  par_values <- lapply(par_list_object, function(par){
    if("dep_object" %in% class(par$value)){
      par$value$value
    }else{
      par$value
    }
  })
  
  par_expansion <- sapply(par_list_object, function(par){
    
    if("dep_object" %in% class(par$value)){
      "expansion" %in% class(par$value$value)
    }else{
      "expansion" %in% class(par$value)
    }    
  })
  
  par_gather <- sapply(par_list_object, function(par){
    
    if("dep_object" %in% class(par$value)){
      "aggregation" %in% class(par$value$value)
    }else{
      "aggregation" %in% class(par$value)
    }    
  })
  
  par_expansion <- mapply(function(e, g) if (isTRUE(g)) FALSE else e, par_expansion, par_gather)

  ## Plain INPUTS: not a dependency, not an output, neither _exp nor _aggr. A
  ## static {x} in one of them is a VECTOR BINDING -- one instance receiving all
  ## the values -- resolved by the wildcard engine whatever the shape of the
  ## task (see process_expansion_item, section 3b).
  par_vector_input <- vapply(par_list_object, function(par){
    !inherits(par, "out_par_object") && !inherits(par$value, "dep_object") &&
      !inherits(par$value, "expansion") && !inherits(par$value, "aggregation")
  }, logical(1))
  
  names(par_values) <- sapply(task_object$par_list, function(par){
    par$par_uuid
  })
  
  names(par_expansion) <- names(par_gather) <- names(par_vector_input) <- names(par_values)
  
  ## A substituted value that is itself a request, replaced by another request,
  ## accumulates: results from several operations have to be aggregated, so the
  ## scheduler collects the requests into a list and passes that list to the wrapper.
  ##
  ## `by` is inferred when left out, so the explicit and the implicit spellings take
  ## the SAME path. A gather whose pattern names a wildcard the consumer is itself
  ## expanded over is a partial aggregation whether or not the user said so, and
  ## saying so is what lets expand_with_partial_aggregation() substitute the group's
  ## value before expansion.
  ##
  ## Deliberately NOT applied when the pattern also carries a dynamic `{{}}`: there
  ## the static wildcard is a per-instance FILTER, not a gather axis (see the
  ## bracket-sentinel note in R/wildcard_engine.R).
  .exp_wcs <- tryCatch(wildcards_expanding_task(task_object), error = function(e) character(0))
  if(length(.exp_wcs)){
    for(.i in seq_along(par_values)){
      if(!isTRUE(par_gather[[.i]])) next
      .v <- par_values[[.i]]
      if(!is.null(attr(.v, "aggr_by"))) next
      .w <- tryCatch(extract_wildcards_from_value(.v),
                     error = function(e) list(static = character(0), dynamic = character(0)))
      if(length(.w$dynamic)) next
      .by <- intersect(.exp_wcs, .w$static)
      if(!length(.by)) next
      attr(.v, "aggr_by") <- .by
      if(!("partial_aggregation" %in% class(.v)))
        class(.v) <- c("partial_aggregation", class(.v))
      par_values[[.i]] <- .v
    }
  }

  expanded_par_values <- expand_with_partial_aggregation(par_values, wildcard_list,
                                                          par_expansion, par_gather,
                                                          sibling_patterns,
                                                          vector_input = par_vector_input)
  expanded_par_values <- reorder_wildcard_keys(expanded_par_values, par_list_object)
  
  ## Record which params were originally dep_object, so the class survives expansion.
  ## wildcard_expansion_and_alignment unwraps dep_object$value for the substitution
  ## engine, and without re-wrapping a cross-task dep declared as
  ## new_dep(task, value = new_file(...)) collapses to a literal file_object, leaving
  ## the scheduler unable to tell that the file is produced by another task. The
  ## re-wrap below restores the dep_object class around the post-expansion concrete
  ## value, so the scheduler can resolve it into an output_req at notify time.
  was_dep_arr <- sapply(par_list_object, function(par) "dep_object" %in% class(par$value))
  dep_task_ids_arr <- lapply(par_list_object, function(par) {
    if ("dep_object" %in% class(par$value)) par$value$task_id else NULL
  })

  comb_list_of_par_list <- lapply(expanded_par_values$expanded_par_values, function(par_comb){

    par_objects_for_combination <- lapply(seq_along(par_list_object), function(i){

      new_par <- par_list_object[[i]]
      new_par$value <- par_comb[[i]]

      ## The assignment carries the value across with all its attributes, class
      ## included, so there is nothing to re-stamp here. Re-stamping would force R
      ## to duplicate a value shared by every other instance.
      
      ## Stamp the authoritative aggregation flag (par_gather) onto the par object so
      ## it survives expansion. The "aggregation" class on the value is reliable here,
      ## at the original declaration, but lost downstream for a DYNAMIC object gather
      ## (new_object_aggr("{{...}}")), whose rebuilt op value carries no class.
      ## OR-sticky: once a gather, always a gather across re-expansion passes, where
      ## par_gather may recompute FALSE from the class-stripped value. Collected onto
      ## the exe_object by from_task_object_to_exe_object and read at delivery prep
      ## (R/supervisor_lib.R) to drive the single-object-wrap strip.
      new_par$is_aggr <- isTRUE(new_par$is_aggr) || isTRUE(par_gather[[i]])

      if (isTRUE(was_dep_arr[i])) {
        new_par$value <- list(task_id = dep_task_ids_arr[[i]], value = new_par$value)
        class(new_par$value) <- "dep_object"
      }
      return(new_par)
    })
    return(new_par_list(par_objects_for_combination))
  })
    
  
  initial_par_ids <- sapply(par_list_object, function(par){par$par_id})
  
  expanded_task_list <- new_task_list(
    
    lapply(seq_along(comb_list_of_par_list), function(rep_index){
      
      output_files <- c()
      output_auto_mkdir <- logical(0)
      output_par_names <- character(0)
      output_object_par_names <- character(0)
      par_list <- comb_list_of_par_list[[rep_index]]
      par_ids <- sapply(par_list, function(par){par$par_id})
      order_indices <- match(initial_par_ids, par_ids)
      par_list_ordered <- par_list[order_indices[!is.na(order_indices)]]
      par_list <- lapply(seq_along(par_list_ordered), function(par_id){
        par <- par_list_ordered[[par_id]]
        return(par)
      })

      for(i in seq_along(par_list)){
        par <- par_list[[i]]
        if("out_par_object" %in% class(par)){
          if("file_path" %in% class(par$value)){
            output_files <- c(output_files, par$value)
            am  <- attr(par$value, "auto_mkdir")
            amv <- if(is.null(am)) TRUE else isTRUE(am)
            output_auto_mkdir <- c(output_auto_mkdir, rep(amv, length(par$value)))
            output_par_names      <- c(output_par_names, rep(par$name, length(par$value)))
          }else{
            ## One entry per declared OBJECT output, never rep()-ed over the
            ## value: an object output's value is its {{wildcard}} pattern,
            ## not a list of produced items, so the count here is the count of
            ## DECLARED outputs, which is what tells the two labelling rules
            ## apart in the wrapper.
            output_object_par_names <- c(output_object_par_names, par$name)
          }
        }
      }

      if(is.null(output_files)){
        output_files <- ""
        ## Length-1 placeholder so msg[[6]] is always non-empty; wrapper
        ## filters by output_files != "" so the value is never consulted.
        output_auto_mkdir <- TRUE
        output_par_names      <- ""
      }

      ## Its OWN condition, not the one above: a task declaring only object
      ## outputs has an empty output_files and a populated list here, and one
      ## declaring only file outputs is the mirror case. Tying this sentinel
      ## to output_files would blank a populated vector in the first case.
      ## An object output may legitimately be unnamed, in which case its name
      ## is "" as well, and both readings give an empty label, which is what
      ## an unnamed output can honestly claim.
      if(!length(output_object_par_names)){
        output_object_par_names <- ""
      }

      ## An exe_id is EITHER inherited OR generated, never built out of another
      ## one, which gives the two properties:
      ##
      ##   bounded -- no concatenation is left on this path, so an id cannot
      ##              accrete a suffix per re-expansion pass.
      ##   stable  -- an instance rebuilt by a later pass keeps the identity it
      ##              already had, instead of being renamed every time.
      ##
      ## The count tells the two situations apart: ONE combination is the same
      ## instance coming back (an aggregation consumer re-expanded per parent
      ## notify, or a task whose dep resolved further), so it inherits; MORE than
      ## one is genuinely new instances, so each gets its own id.

      .keep_stable <- length(comb_list_of_par_list) == 1L
      op <- new_task(
        task_id = get_object_id(task_object),
        exe_id = if(.keep_stable) task_object$exe_id else uuid::UUIDgenerate(),
        type = get_type_from_object(task_object),
        exec = get_cmd_from_object(task_object),
        par_list = par_list,
        ## Merge: keys NOT computed by expand_parameters in this call
        ## (e.g. concrete `sample="s1"` set by inherit_parent_wildcards in
        ## a prior build pass) survive when expand_parameters cannot
        ## recompute them for the current call (par_expansion=FALSE -> vfe
        ## yields placeholder).
        wildcards = local({
          old <- task_object$wildcards
          new <- expanded_par_values$wildcard_used[[rep_index]]
          ## A BARE placeholder must not overwrite the BRACKETED form of the
          ## same key. The brackets are the task's gather intent, read back by
          ## detect_op_modalities() as is_explicit_agg. An expansion triggered
          ## by a notify for a DIFFERENT parameter leaves the aggregating key
          ## untouched, so w_info_store still holds the bare "{{k}}".
          for (k in intersect(names(old), names(new))) {
            o_val <- old[[k]]; n_val <- new[[k]]
            if (is.character(o_val) && length(o_val) == 1L &&
                is.character(n_val) && length(n_val) == 1L &&
                o_val %in% c(paste0("[{{", k, "}}]"), paste0("[{", k, "}]")) &&
                n_val %in% c(paste0("{{", k, "}}"),   paste0("{", k, "}"))) {
              new[[k]] <- o_val
            }
          }
          c(old[setdiff(names(old), names(new))], new)
        }),
        output_files = output_files,
        output_auto_mkdir = output_auto_mkdir,
        output_par_names = output_par_names,
        output_object_par_names = output_object_par_names,
        requires = task_object$requires,
        cmd_interpreter   = task_object$cmd_interpreter,
        shell_interpreter = task_object$shell_interpreter
      )

      return(op)
    })
  )

  return(expanded_task_list)
}

get_object_type <- function(ob) {
  if (is.vector(ob) && !is.list(ob)) {
    return("vector")
  } else {
    return(class(ob))
  }
}

## Per-param logical: was this param DECLARED as an aggregation (gather)?
## Reads the `is_aggr` flag stamped during wildcard_expansion_and_alignment
## (see above), falling back to the value-level "aggregation" class for a par
## object built outside that path. Consumed at delivery-prep to keep a 1-element
## gather a list while a single non-aggregation object dep is delivered bare.
## uuid-named to stay aligned with params_type.
get_params_is_aggr <- function(par_list, params_type_names) {
  flags <- vapply(par_list, function(par_object){
    if(!is.null(par_object$is_aggr)) return(isTRUE(par_object$is_aggr))
    v <- par_object$value
    if("dep_object" %in% class(v)) "aggregation" %in% class(v$value)
    else "aggregation" %in% class(v)
  }, logical(1))
  names(flags) <- params_type_names
  flags
}

exe_object_from_task <- function(task_object, exe_id,
                                 name_source = c("name", "uuid"),
                                 deparse_function_exec = FALSE){

  name_source <- match.arg(name_source)

  cmd_interpreter   <- task_object$cmd_interpreter
  shell_interpreter <- task_object$shell_interpreter
  type              <- get_type_from_object(task_object)
  par_list          <- get_child_objects(task_object)
  user_code         <- task_object$exec

  if(deparse_function_exec && "function" %in% class(task_object$exec)){
    user_code <- deparse(task_object$exec)
  }

  par_value <- sapply(par_list, function(par_object){
    par_value <- list(get_value_from_object(par_object))
    names(par_value) <- if(name_source == "uuid") par_object$par_uuid
                        else get_name_from_object(par_object)
    return(par_value)
  })

  params_type <- sapply(par_list, function(par_object){
    ifelse("file_object" %in% class(par_object$value) & !"out_par_object" %in% class(par_object), "file", get_object_type(get_value_from_object(par_object)))
  })

  params_type_names <- sapply(par_list, function(par_object){
    par_object$par_uuid
  })

  names(params_type) <- params_type_names

  params_is_aggr <- get_params_is_aggr(par_list, params_type_names)

  new_exe_object(
    task_id           = task_object$task_id,
    exe_id            = exe_id,
    cmd               = cmd_interpreter,
    shell_interpreter = shell_interpreter,
    type              = type,
    user_code         = user_code,
    func_call         = "",
    par_list          = par_value,
    par_names         = task_object$par_names,
    par_uuids         = task_object$par_uuids,
    params_type       = params_type,
    params_is_aggr    = params_is_aggr,
    output_files      = task_object$output_files,
    output_auto_mkdir = task_object$output_auto_mkdir,
    output_par_names  = task_object$output_par_names,
    output_object_par_names = task_object$output_object_par_names,
    is_output_par     = task_object$is_output_par,
    dyn_wildcard      = task_object$dyn_wildcard
  )
}

#' @export
#' @noRd
from_task_object_to_exe_object.R_task_object <- function(task_object, exe_id){
  exe_object_from_task(task_object, exe_id,
                       name_source = "name", deparse_function_exec = TRUE)
}

#' @export
#' @noRd
from_task_object_to_exe_object.shell_task_object <- function(task_object, exe_id){
  exe_object_from_task(task_object, exe_id,
                       name_source = "uuid", deparse_function_exec = FALSE)
}

#' @export
#' @noRd
from_task_object_to_exe_object.python_task_object <- function(task_object, exe_id){
  exe_object_from_task(task_object, exe_id,
                       name_source = "name", deparse_function_exec = FALSE)
}

## Expand par_values across the cartesian product of wildcard substitutions,
## handling partial_aggregation parameters specially: when a parameter
## carries class "partial_aggregation" with attr("aggr_by") set, the aggr_by
## wildcards are expanded into one combination at a time and the result
## carries a fixed value for those keys. This keeps the partial-aggregation
## consumer cloned per (aggr_by) tuple while still scattering the remaining
## axes via expand_parameters.
##
## When no partial_aggregation parameters exist, falls through to a plain
## expand_parameters call. Pure helper of
## wildcard_expansion_and_alignment.task_object: no side effects.
expand_with_partial_aggregation <- function(par_values, wildcard_list,
                                             par_expansion, par_gather, sibling_patterns = NULL,
                                             vector_input = NULL){
  partial_agg_pars <- names(which(sapply(par_values, function(v) {
    "partial_aggregation" %in% class(v) && !is.null(attr(v, "aggr_by"))
  })))

  if (length(partial_agg_pars) == 0) {
    return(expand_parameters(par_values, wildcard_list, par_expansion, par_gather, sibling_patterns,
                             vector_input = vector_input))
  }

  by_wds <- unique(unlist(lapply(partial_agg_pars, function(p) attr(par_values[[p]], "aggr_by"))))
  by_wds <- intersect(by_wds, names(wildcard_list))

  if (length(by_wds) == 0) {
    return(expand_parameters(par_values, wildcard_list, par_expansion, par_gather, sibling_patterns,
                             vector_input = vector_input))
  }

  by_wc_vals <- lapply(by_wds, function(wd) names(wildcard_list[[wd]]))
  by_combos <- expand.grid(setNames(by_wc_vals, by_wds), stringsAsFactors = FALSE,
                           KEEP.OUT.ATTRS = FALSE)

  all_expanded <- lapply(seq_len(nrow(by_combos)), function(i) {
    combo_vals <- as.list(by_combos[i, , drop = FALSE])

    mod_par_values <- lapply(par_values, function(v) {
      if (!is.character(v)) return(v)
      s <- as.character(v)
      for (wd in by_wds) {
        pat <- paste0("(?<!\\{)\\{", wd, "\\}(?!\\})")
        s <- gsub(pat, combo_vals[[wd]], s, perl = TRUE)
      }
      cls <- class(v)
      cls <- cls[cls != "partial_aggregation"]
      class(s) <- cls
      for (an in setdiff(names(attributes(v)), c("class", "aggr_by"))) {
        attr(s, an) <- attr(v, an)
      }
      return(s)
    })

    ep <- expand_parameters(mod_par_values, wildcard_list, par_expansion, par_gather, sibling_patterns,
                          vector_input = vector_input)

    ep$wildcard_used <- lapply(ep$wildcard_used, function(wc) {
      for (wd in by_wds) wc[[wd]] <- combo_vals[[wd]]
      return(wc)
    })
    return(ep)
  })

  list(
    expanded_par_values = unlist(lapply(all_expanded, `[[`, "expanded_par_values"), recursive = FALSE),
    wildcard_used       = unlist(lapply(all_expanded, `[[`, "wildcard_used"),       recursive = FALSE)
  )
}

## Reorder the keys of each wildcard_used vector so that the wildcards
## referenced by the task's parameters come first, in the order they appear
## across par_list_object (output_req dyn_wildcard first, then character
## values' wildcards). Wildcards not referenced retain their original order
## at the tail. Pure helper of wildcard_expansion_and_alignment.task_object.
reorder_wildcard_keys <- function(expanded_par_values, par_list_object) {
  ref_wc_order <- character(0)
  for (par in par_list_object) {
    val <- if ("dep_object" %in% class(par$value)) par$value$value else par$value
    ## A resolved dependency is a list of constituents (as_output_req_list),
    ## and the constituents of one parameter share their dynamic key.
    if (inherits(val, "output_req_list")) {
      k <- extract_dyn_key(val[[1]]$dyn_wildcard)
      if (length(k) > 0 && nchar(k) > 0) ref_wc_order <- unique(c(ref_wc_order, k))
    } else if (is.character(val) || is.factor(val)) {
      ref_wc_order <- unique(c(ref_wc_order, names(get_wildcards(val))))
    }
  }
  if (length(ref_wc_order) > 0) {
    expanded_par_values$wildcard_used <- lapply(expanded_par_values$wildcard_used, function(wc) {
      wc[c(intersect(ref_wc_order, names(wc)), setdiff(names(wc), ref_wc_order))]
    })
  }
  expanded_par_values
}
