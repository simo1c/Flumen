## Internal regex constants for wildcard parsing. Centralised here to avoid
## drift across the codebase when the wildcard syntax is extended.
##
## - WILDCARD_PATTERN: matches both static {w} and dynamic {{w}}. Uses
##   PCRE lookarounds, so always pass perl = TRUE when consuming.
## - DYN_WILDCARD_PATTERN: matches only dynamic {{w}} (no capture group).
## - DYN_WILDCARD_CAPTURE_PATTERN: matches only dynamic {{w}}, with the
##   wildcard name captured as group 1.
## - STATIC_WILDCARD_PATTERN: matches only static {w} (single brace, not
##   inside a {{...}}); uses negative lookarounds so requires perl = TRUE.
## - STATIC_WILDCARD_CAPTURE_PATTERN: same as above with the wildcard name
##   captured as group 1.
WILDCARD_PATTERN                  <- "(?<!\\{)\\{[^{}]+\\}(?!\\})|\\{\\{[^{}]+\\}\\}"
DYN_WILDCARD_PATTERN              <- "\\{\\{[^{}]+\\}\\}"
DYN_WILDCARD_CAPTURE_PATTERN      <- "\\{\\{([^{}]+)\\}\\}"
STATIC_WILDCARD_PATTERN           <- "(?<!\\{)\\{[^{}]+\\}(?!\\})"
STATIC_WILDCARD_CAPTURE_PATTERN   <- "(?<!\\{)\\{([^{}]+)\\}(?!\\})"

## Extract the dynamic wildcard key (without braces) from a dyn_wildcard
## template. Composite-aware: for "{sample}_{{out}}" returns "out" (captures
## only the {{...}} portion); for plain "{{out}}" returns "out"; for plain
## "out" returns "out". Centralised here so object_func.R and the body of
## expand_parameters cannot disagree on composite handling.
extract_dyn_key <- function(dyn_wildcard) {
  ## An output value may be a BUNDLE, several paths in one declaration, and
  ## regexpr() then returns one match per entry. The entries share the same
  ## {{name}} by construction -- validate_workflow() refuses more than one
  ## DISTINCT dynamic wildcard in a parameter -- so unique() yields the one key,
  ## and the callers that use it as a name (to_expand[[k]], w_info[[k]]) stay valid.
  m <- regmatches(dyn_wildcard, regexpr(DYN_WILDCARD_CAPTURE_PATTERN, dyn_wildcard))
  if (length(m) > 0) unique(gsub("[{}]", "", m)) else unique(gsub("[{}]", "", dyn_wildcard))
}

check_no_wildcard_element <- function(params_list) {
  # Regex that captures both {key} and {{key}}
  pattern <- WILDCARD_PATTERN
  has_no_wc <- sapply(params_list, function(x) {
    if (is.character(x)) {
      return(!any(grepl(pattern, x, perl = TRUE)))
    } else {
      return(TRUE)
    }
  })
  return(any(has_no_wc))
}

find_dynamic_wildcards <- function(values) {
  # Match the double-brace dynamic-wildcard pattern {{...}}
  #
  # grep() coerces what it is given: called on a task's literal parameter it
  # turns the whole payload into strings for a pattern that requires braces and
  # can never match it. Test only the character/factor entries -- but map the hits back
  # to their ORIGINAL positions, because two callers use the return value to
  # index `values` itself (R/supervisor_deps.R), not just as a yes/no.
  # Decide on the CONTAINER first: an atomic vector is scannable, or not, as a
  # whole. Only a list can hold a mix, and a list of parameters is short.
  match_indices <- if (is.character(values) || is.factor(values)) {
    grep(DYN_WILDCARD_PATTERN, values, perl = TRUE)
  } else if (is.list(values)) {
    scannable <- vapply(values, function(v) is.character(v) || is.factor(v), logical(1))
    if (!any(scannable)) return(NULL)
    keep <- which(scannable)
    keep[grep(DYN_WILDCARD_PATTERN, values[keep], perl = TRUE)]
  } else {
    return(NULL)
  }

  if (length(match_indices) == 0) {
    return(NULL)
  } else {
    return(match_indices)
  }
}

## A bare dynamic-wildcard output pattern -- the wildcard with NO file extension
## after it, e.g. `dir/`{{any}} -- signals "whole-folder" intent: the producer
## globs the directory RECURSIVELY and transfers the entire sub-tree. With an
## extension (`dir/`{{any}}.ext) the user wants only matching files, kept flat
## (single-level). Returns TRUE iff `pattern` carries a dynamic wildcard and its
## basename ends with the wildcard token (nothing after the closing `}}`).
## Used by the wrappers to choose list.files(recursive=) at output collection.
is_recursive_wildcard <- function(pattern) {
  if (!is.character(pattern) || length(pattern) != 1L || !nzchar(pattern)) return(FALSE)
  base <- basename(strip_backticks(pattern))
  grepl(DYN_WILDCARD_PATTERN, base, perl = TRUE) && sub(".*\\}\\}", "", base) == ""
}

## The character content of `x`, and nothing else -- the only content in which a
## wildcard can possibly appear: every pattern above requires braces, which no
## number, logical or raw can produce once coerced.
##
## Exists because `unlist()` COERCES: applied to a list holding one string and one
## parameter payload it turns the whole payload into strings.
##
## Order is preserved among the character leaves, so callers that paste and then
## extract tags see the same tags in the same order; dropped leaves contribute
## none. Returns character(0) when there is nothing scannable, which every caller
## already handles (get_wildcards returns list() on empty, grepl gives logical(0)).
wildcard_text <- function(x) {
  if (is.character(x) || is.factor(x)) return(as.character(x))
  if (is.list(x)) return(unlist(lapply(x, wildcard_text), use.names = FALSE))
  character(0)
}

get_wildcards <- function(val, active_wildcards = NULL) {
  if (length(val) == 0 || (!is.character(val) && !is.factor(val))) {
    return(list())
  }
  
  txt <- paste(as.character(val), collapse = " ")
  pattern <- WILDCARD_PATTERN
  matches <- gregexpr(pattern, txt, perl = TRUE)
  extracted <- regmatches(txt, matches)[[1]]
  
  if (length(extracted) == 0) {
    return(list())
  }
  
  unique_tags <- unique(extracted)
  keys <- gsub("[{}]", "", unique_tags)
  
  res <- setNames(as.list(paste0("[", unique_tags, "]")), keys)
  
  # Filter: if a key is already "active" (in the expansion phase),
  # do not report it as static to avoid duplicates in wildcard_used
  if (!is.null(active_wildcards)) {
    res <- res[!(names(res) %in% active_wildcards)]
  }
  
  return(res)
}

extract_suffix_contents <- function(string_vector) {
  pattern <- "\\`(.+?)\\`" 
  match_indices <- grep(pattern, string_vector, perl = TRUE)
  
  indices <- NULL
  backtick_contents <- NULL
  
  if (length(match_indices) > 0) {
    indices <- match_indices
    matches <- regexec(pattern, string_vector, perl = TRUE)
    extracted_list <- regmatches(string_vector, matches)
    backtick_contents <- unlist(lapply(extracted_list[match_indices], function(x) {
      return(x[2])
    }))
  }
  
  return(list(
    indices = indices,
    values = backtick_contents
  ))
}

process_wildcard_vector_item <- function(name, value, par_expansion) {
  pattern <- WILDCARD_PATTERN
  
  if (is.character(value) && length(value) > 1) {
    if (any(grepl(pattern, value, perl = TRUE))) {
      all_matches <- regmatches(value, gregexpr(pattern, value, perl = TRUE))
      keys <- unique(unlist(sapply(all_matches, function(m) {
        gsub("[{}]", "", m)
      })))
      return(list(
        type = "vector_wildcard_to_expand",
        name = name,
        templates = value,
        wildcards = keys,
        expand_final = par_expansion[name]
      ))
    }
  } else if (is.character(value) && length(value) == 1) {
    m <- regmatches(value, gregexpr(pattern, value, perl = TRUE))[[1]]
    if (length(m) > 0) {
      return(list(
        type = "wildcard",
        name = name,
        wildcards = unique(gsub("[{}]", "", m)),
        template = value
      ))
    }
  }
  return(list(type = "static", name = name, value = value))
}

get_wildcard_substitution_values <- function(wildcard_list, expand) {
  v_exp <- list()
  o_sub <- list()

  for (name in names(wildcard_list)) {
    val <- wildcard_list[[name]]

    # Expansion is the default, and is possible only over a named list: the keys
    # are the values to expand over.
    should_expand <- if (name %in% names(expand)) expand[[name]] else TRUE
    if (!is.list(val)) {
      should_expand <- FALSE
    }

    if (should_expand && !is.null(names(val)) && length(val) > 0 && !is.atomic(val)) {
      v_exp[[name]] <- names(val)
      o_sub[[name]] <- val

    } else {
      if (is.list(val) && length(val) > 0 && inherits(val[[1]], "output_req")) {

        first_elem_name <- if (!is.null(names(val))) names(val)[1] else paste0(name, "_1")

        ## x["k"] <- list(NULL), not x[["k"]] <- NULL: the second DROPS the
        ## entry, and the readers downstream test the field, not its presence.
        modified_elem <- val[[1]]
        modified_elem["output_instance"] <- list(NULL)

        v_exp[[name]] <- first_elem_name
        o_sub[[name]] <- setNames(list(modified_elem), first_elem_name)

      } else {
        dummy_key <- paste0("{", name, "}")
        v_exp[[name]] <- dummy_key
        o_sub[[name]] <- setNames(list(val), dummy_key)
      }
    }
  }

  return(list(values_for_expansion = v_exp, objects_for_substitution = o_sub))
}

group_parameters_by_wildcard_simple_expand <- function(par_values, wildcard_list, par_expansion) {
  w_per_e <- list()
  static_e <- list()
  v_wc_e <- list()
  p_names <- names(par_values)
  
  if (!all(p_names %in% names(par_expansion))) {
    par_expansion[setdiff(p_names, names(par_expansion))] <- FALSE
  }
  
  for (i in seq_along(par_values)) {
    res <- process_wildcard_vector_item(p_names[i], par_values[[i]], par_expansion)
    if (res$type == "wildcard") {
      w_per_e[[p_names[i]]] <- res$wildcards
      par_values[[p_names[i]]] <- res$template
    } else if (res$type == "static") {
      static_e[[p_names[i]]] <- res$value
    } else if (res$type == "vector_wildcard_to_expand") {
      w_per_e[[p_names[i]]] <- res$wildcards
      v_wc_e[[p_names[i]]] <- res
    }
  }
  
  grouped <- list()
  remain <- names(w_per_e)
  
  while (length(remain) > 0) {
    curr_n <- remain[1]
    curr_g <- curr_n
    w_in_g <- w_per_e[[curr_n]]
    
    repeat {
      added <- FALSE
      for (n in setdiff(remain, curr_g)) {
        if (any(w_per_e[[n]] %in% w_in_g)) {
          curr_g <- c(curr_g, n)
          w_in_g <- unique(c(w_in_g, w_per_e[[n]]))
          added <- TRUE
        }
      }
      if (!added) {
        break
      }
    }
    
    content <- list()
    for (n in curr_g) {
      if (n %in% names(v_wc_e)) {
        d <- v_wc_e[[n]]
        if (isTRUE(d$expand_final)) {
          tmpl <- setNames(as.list(d$templates), paste0(n, "_template_", seq_along(d$templates)))
        } else {
          tmpl <- setNames(list(d$templates), paste0(n, "_atomic_template"))
        }
        content <- c(content, tmpl)
      } else {
        content[[n]] <- par_values[[n]]
      }
    }
    grouped <- c(grouped, list(content))
    remain <- setdiff(remain, curr_g)
  }
  
  if (length(static_e) > 0) {
    grouped <- c(grouped, list(static_e))
  }
  return(grouped)
}

conditional_expand <- function(input_list, expand) {
  pre <- mapply(function(item, exp) {
    if (!isTRUE(exp) || is.data.frame(item) || !is.character(item) || length(item) > 1) {
      return(list(item))
    } else {
      return(as.list(item))
    }
  }, input_list, expand, SIMPLIFY = FALSE)

  if (length(input_list) == 1 && isTRUE(expand[[1]])) {
    res_list <- lapply(pre[[1]], function(v) {
      return(setNames(list(v), names(input_list)[1]))
    })
  } else {
    prot <- lapply(pre, function(x) {
      if (is.list(x) && !all(sapply(x, is.atomic))) {
        return(I(x))
      } else {
        return(x)
      }
    })
    ## KEEP.OUT.ATTRS = FALSE: expand.grid's out.attrs is built by paste0()-ing
    ## every VALUE into a label, which formats a real parameter payload element
    ## by element. Nothing in flumen reads that attribute, and what flumen
    ## derives from the grid is identical without it.
    grid <- do.call(expand.grid, c(prot, stringsAsFactors = FALSE, check.names = FALSE,
                                   KEEP.OUT.ATTRS = FALSE))
    res_list <- apply(grid, 1, as.list, simplify = FALSE)

    if (!is.null(names(input_list))) {
      unbox <- mapply(function(item, exp) {
        return(is.data.frame(item) || !isTRUE(exp))
      }, input_list, expand)

      res_list <- lapply(res_list, function(sub) {
        if (ncol(grid) == length(input_list)) {
          names(sub) <- names(input_list)
        }
        ## A non-expanded value that is a list of ONE element is delivered as
        ## that element. A resolved dependency is excluded: its container is a
        ## list of constituents whose length is the number of producers, not a
        ## wrapping, and unboxing a single producer would hand out its
        ## output_req with the container's class.
        for (i in seq_along(sub)) {
          if (i <= length(unbox) && unbox[i]) {
            if (is.list(sub[[i]]) && length(sub[[i]]) == 1 && !is.data.frame(sub[[i]]) &&
                !inherits(sub[[i]], "output_req_list")) {
              sub[[i]] <- sub[[i]][[1]]
            }
          }
        }
        return(sub)
      })
    }
  }

  final_wrapped <- lapply(res_list, function(item) {
    return(list(
      params = item,
      wc = get_wildcards(wildcard_text(item))
    ))
  })
  return(final_wrapped)
}

block_cartesian_product_N <- function(...) {
  input <- list(...)
  if (all(sapply(input, length) == 0)) {
    return(list())
  }

  idx_seq <- lapply(input, seq_along)
  combos <- do.call(expand.grid, c(idx_seq, KEEP.OUT.ATTRS = FALSE))

  res <- apply(combos, 1, function(row) {
    p <- list()
    w <- list()
    for (j in seq_along(input)) {
      wrapper <- input[[j]][[row[j]]]
      p <- c(p, wrapper$params)
      w <- c(w, wrapper$wc)
    }
    w <- w[!duplicated(names(w))]
    return(list(params = p, wc = w))
  }, simplify = FALSE)

  return(unname(res))
}

expand_wildcard <- function(parameters, wildcard_list, wc_expansion_values) {
  # wildcard_text() recurses over the whole parameter structure, which for an
  # aggregating consumer is the entire accumulated set. It is pure and
  # `parameters` is not touched, so one evaluation serves both uses below.
  par_text <- wildcard_text(parameters)
  all_t <- paste(par_text, collapse = " ")
  all_w_names <- names(wildcard_list)
  
  used_w <- all_w_names[sapply(all_w_names, function(n) {
    pattern <- paste0("((?<!\\{)\\{", n, "\\}(?!\\})|\\{\\{", n, "\\}\\})")
    return(grepl(pattern, all_t, perl = TRUE))
  })]
  
  # Get static wildcards excluding those we are about to expand
  static_wc <- get_wildcards(par_text, active_wildcards = used_w)
  
  # A wildcard with no values in wildcard_list is turned by get_wildcards into
  # a static aggregator ([{{out}}]). The brackets are stripped here so the DAG
  # can scatter on it: "[{{var}}]" becomes "{{var}}", "[{var}]" becomes "{var}".
  static_wc <- lapply(static_wc, function(val) {
    if (is.character(val) && length(val) == 1) {
      return(sub("^\\[(.*)\\]$", "\\1", val))
    }
    return(val)
  })

  if (length(used_w) == 0) {
    return(list(list(params = parameters, wc = static_wc)))
  }
  
  combos <- expand.grid(wc_expansion_values[used_w], stringsAsFactors = FALSE,
                        KEEP.OUT.ATTRS = FALSE)
  exp_res <- lapply(parameters, function(tpl) {
    if (inherits(tpl, "output_req") || inherits(tpl, "output_req_list")) {
      return(lapply(seq_len(nrow(combos)), function(i) tpl))
    }
    apply(combos, 1, function(row) {
      s <- tpl
      for (n in used_w) {
        pattern <- paste0("((?<!\\{)\\{", n, "\\}(?!\\})|\\{\\{", n, "\\}\\})")
        if (any(grepl(pattern, s, perl = TRUE))) {
          s <- gsub(pattern, row[[n]], s, perl = TRUE)
        }
      }
      return(s)
    }, simplify = FALSE)
  })
  
  final_list <- lapply(seq_len(nrow(combos)), function(i) {
    current_params <- setNames(lapply(exp_res, `[[`, i), names(parameters))
    current_wc_used <- c(as.list(combos[i, , drop = FALSE]), static_wc)
    current_wc_used <- current_wc_used[!duplicated(names(current_wc_used))]
    return(list(params = current_params, wc = current_wc_used))
  })
  
  return(final_list)
}

expand_parameters <- function(par_values, wildcard_list, par_expansion = NULL, gather = NULL,
                              sibling_patterns = NULL, vector_input = NULL) {

  # --- A. INITIALIZATION ---
  if (is.null(par_expansion)) {
    par_expansion <- setNames(rep(FALSE, length(par_values)), names(par_values))
  }
  if (is.null(gather)) {
    gather <- setNames(rep(FALSE, length(par_values)), names(par_values))
  }
  
  # --- B. EXPANSION SETUP ---
  ## Whether a wildcard drives fan-out is decided by ANY parameter that is an
  ## expansion driver for it -- an OR, not the first declared. An aggregation
  ## parameter has par_expansion forced to FALSE upstream (it gathers, it does
  ## not fan out), so declaration order must not decide.
  to_expand <- list()
  for (i in seq_along(par_values)) {
    wcs <- names(get_wildcards(par_values[[i]]))
    for (wc in wcs)
      to_expand[[wc]] <- isTRUE(to_expand[[wc]]) || isTRUE(par_expansion[[i]])
  }
  
  ## A resolved dependency is a list of constituents (as_output_req_list) whose
  ## dynamic key is shared: the first constituent speaks for the parameter.
  for (p in names(par_values)) {
    v <- par_values[[p]]
    ref <- if (inherits(v, "output_req_list")) v[[1]] else NULL
    if (!is.null(ref)) {
      k <- extract_dyn_key(ref$dyn_wildcard)
      if (length(k) > 0 && nchar(k) > 0 && !(k %in% names(to_expand))) {
        to_expand[[k]] <- isTRUE(par_expansion[[p]])
      }
    }
  }
  
  wc_d <- get_wildcard_substitution_values(wildcard_list, to_expand)
  orig_order <- names(par_values)
  class_attr <- lapply(par_values, attributes)
  
  # --- C. GROUPING AND CARTESIAN PRODUCT ---
  groups <- group_parameters_by_wildcard_simple_expand(par_values, wildcard_list, par_expansion)
  wc_groups_raw <- groups
  no_wc_g <- list()
  last_i <- length(groups)
  
  if (last_i > 0) {
    pattern <- WILDCARD_PATTERN
    if (all(!grepl(pattern, wildcard_text(groups[[last_i]]), perl = TRUE))) {
      no_wc_g <- groups[[last_i]]
      wc_groups_raw <- groups[-last_i]
    }
  }
  
  no_wc_exp <- if (check_no_wildcard_element(par_values) && length(no_wc_g) > 0) {
    conditional_expand(no_wc_g, par_expansion[names(no_wc_g)])
  } else list()
  
  wc_gs <- wc_groups_raw
  singl <- wc_gs[sapply(wc_gs, length) == 1]
  wc_gs <- wc_gs[sapply(wc_gs, length) > 1]
  
  if (length(wc_gs) > 0) wc_gs <- lapply(wc_gs, expand_wildcard, wildcard_list, wc_d$values_for_expansion)
  if (length(singl) > 0) singl <- lapply(singl, expand_wildcard, wildcard_list, wc_d$values_for_expansion)

  input_l <- Filter(length, c(wc_gs, singl, if (length(no_wc_exp) > 0) list(no_wc_exp)))
  res <- if (length(input_l) > 0) do.call(block_cartesian_product_N, input_l) else list()
  
  # --- D. RECONSTRUCTION AND SIGNAL HANDLING ---
  ## Output collection via lapply + unlist(recursive = FALSE). Order is
  ## preserved (sequential lapply over res, sequential lapply over sub_sets)
  ## and side effects on `new` / `w_info_store` stay in the closure scope.
  all_results <- unlist(lapply(res, function(item) {
    process_expansion_item(item, orig_order, class_attr, par_values,
                           par_expansion, gather, wildcard_list, wc_d,
                           sibling_patterns, vector_input)
  }), recursive = FALSE)
  ## A per-file instance the per-sample filter excludes comes back as a marker.
  ## Some dropped: those files belong to other samples. ALL dropped: this sample
  ## has no file of its own, and an instance-less sample would leave any consumer
  ## grouped by it waiting for ever -- so it stops the run, by name, as
  ## new_file_aggr does on the same input.
  dropped <- vapply(all_results, inherits, logical(1), "flumen_dropped_instance")
  if (length(all_results) && all(dropped)) {
    pat <- all_results[[1]]$pattern
    ## A condition of its own class: raised while the scheduler re-expands the
    ## workflow, it is a property of the data, not an engine fault, and
    ## scheduler_fatal() reports it without the "internal error" prefix.
    stop(structure(class = c("flumen_user_error", "error", "condition"), list(
      message = sprintf(paste("no file produced by the upstream task matches '%s'.",
                              "Fix: check that the producer writes a file for every sample."),
                        pat),
      call = NULL)))
  }
  all_results <- all_results[!dropped]

  f_params <- lapply(all_results, `[[`, "params")
  f_wc     <- lapply(all_results, `[[`, "wc")

  return(list(expanded_par_values = f_params, wildcard_used = f_wc))
}

## Process one cartesian-product item from expand_parameters: explode any
## "_template_" sub-keys, then for each sub-set apply the per-parameter
## transformations (attribute restore, gather logic, composite output_req
## injection, output_req injection, complex object substitution) and emit
## one (params, wc) pair per sub-set. Returns a list of such pairs.
##
## Helper of expand_parameters; not S3, not user-facing. Every variable it needs
## is passed explicitly rather than captured from the enclosing frame.
## `sibling_patterns`: par_uuid -> the resolved pattern this same parameter has
## in the OTHER instances of this task. Used only to sharpen the per-sample
## filter (see p2p_derive_select); NULL leaves the parameter unfiltered.
process_expansion_item <- function(item, orig_order, class_attr, par_values,
                                   par_expansion, gather, wildcard_list, wc_d,
                                   sibling_patterns = NULL, vector_input = NULL) {
  conf <- item$params
  w_info <- item$wc

  t_split <- grep("_template_", names(conf), value = TRUE)
  t_atomic <- grep("_atomic_template", names(conf), value = TRUE)
  rest <- conf[setdiff(names(conf), c(t_split, t_atomic))]
  atom_p <- list()
  if (length(t_atomic) > 0) for (n in t_atomic) atom_p[[sub("_atomic_template", "", n)]] <- conf[[n]]

  sub_sets <- if (length(t_split) > 0) {
    orig_s <- unique(sub("_template_.*", "", t_split))
    vals <- lapply(orig_s, function(o) lapply(grep(paste0("^", o, "_template_"), names(conf), value = TRUE), function(t) unlist(conf[[t]], use.names = FALSE)))
    grid <- do.call(expand.grid, c(setNames(vals, orig_s), stringsAsFactors = FALSE,
                                   check.names = FALSE, KEEP.OUT.ATTRS = FALSE))
    lapply(seq_len(nrow(grid)), function(r) c(lapply(as.list(grid[r, , drop = FALSE]), function(v) as.vector(unlist(v, FALSE, FALSE))), atom_p, rest))
  } else list(c(atom_p, rest))

  lapply(sub_sets, function(raw) {
    new <- raw[intersect(orig_order, names(raw))][orig_order]
    w_info_store <- w_info

    for (p in names(new)) {
      # 1. Attribute restore
      if (!is.null(class_attr[[p]])) {
        a <- class_attr[[p]]
        exp <- isTRUE(par_expansion[[p]])
        if (!is.null(a$class)) class(new[[p]]) <- if (exp) unique(c(a$class, class(new[[p]]))) else a$class
        for (an in names(a)) {
          if (an != "class" && !(exp && an %in% c("names", "row.names", "dim", "dimnames"))) attr(new[[p]], an) <- a[[an]]
        }
      }

      v_chk <- par_values[[p]]

      # 2. Explicit Gather Logic (square brackets [])
      if (isTRUE(gather[[p]])) {
        raw_str <- if (inherits(v_chk, "output_req_list")) v_chk[[1]]$dyn_wildcard
        else if (is.character(v_chk)) v_chk[1] else NULL
        if (!is.null(raw_str)) {
          ## unlist(), not [[1]]: raw_str may be a bundle, and [[1]] would see
          ## only the tokens of its first entry.
          matches <- unique(unlist(regmatches(raw_str, gregexpr(WILDCARD_PATTERN, raw_str, perl = TRUE))))
          ## Per-sample filter: when the pattern carries a dynamic {{wc}} (the
          ## gather axis), a STATIC {wc} is the consumer's per-instance binding
          ## (a filename filter, e.g. "{sample}_{{any}}"), NOT a gather axis --
          ## do NOT bracket-sentinel it, or the parent-matcher reads it as the
          ## broadcast sentinel. Pure-static gathers (no {{}}) are sentinelled.
          ## any(): raw_str may be a bundle, and a logical of length > 1 stops
          ## the run inside the || below. The entries agree, see extract_dyn_key().
          raw_has_dyn <- any(grepl("\\{\\{[^{}]+\\}\\}", raw_str, perl = TRUE))
          for (m in matches) {
            k <- gsub("[{}]", "", m)
            m_is_dyn <- grepl("^\\{\\{", m)
            if (k %in% names(w_info_store) && !grepl("^\\[.*\\]$", w_info_store[[k]]) &&
                (m_is_dyn || !raw_has_dyn)) {
              w_info_store[[k]] <- if (m_is_dyn) paste0("[{{", k, "}}]") else paste0("[{", k, "}]")
            }
            # Static string template expansion for gather.
            # Skip when the template also carries a dynamic {{...}}: the
            # composite case is handled by the output_req injection at notify
            # time, and a static broadcast here would freeze par_xs as
            # character[N] and block that injection.
            if (!grepl("^\\{\\{", m) &&
                is.character(new[[p]]) &&
                !inherits(new[[p]], "output_req_list") &&
                k %in% names(wildcard_list) &&
                !any(grepl(DYN_WILDCARD_CAPTURE_PATTERN, new[[p]], perl = TRUE))) {
              ## A path carries the wildcard's KEY, never its value: unlist()
              ## returns the values, which for an object-valued wildcard
              ## (list(df1 = <data.frame>, ...)) flattens the objects themselves.
              ## new_wildcard_list() stores an atomic wildcard with names ==
              ## values, so the two coincide there.
              wc_keys <- names(wildcard_list[[k]])
              wc_vals <- if (!is.null(wc_keys)) wc_keys else unname(unlist(wildcard_list[[k]]))
              orig_cls <- class(new[[p]])
              ## Preserve the un-substituted template(s) as an attribute on the
              ## expanded result, so each expanded path can be reverse-matched to
              ## its producer exe_id ({sample}.tsv + "s2" -> rsem_quant_2).
              orig_template <- new[[p]]
              pat_static <- paste0("(?<!\\{)\\{", k, "\\}(?!\\})")
              new[[p]] <- unname(as.vector(sapply(new[[p]], function(current_val) {
                sapply(wc_vals, function(v) gsub(pat_static, v, current_val, perl = TRUE))
              })))
              class(new[[p]]) <- orig_cls
              attr(new[[p]], "original_template") <- orig_template
            }
          }
        }
      }

      # 2.5: Composite templates with {{w}} that map to output_req.
      # E.g. "gen_{{out}}.Rds" -- expand_wildcard replaces with the key name (exe_id);
      # here we reconstitute an output_req with dyn_wildcard = full template so that
      # update_param_wildcard_with_value can substitute the correct value at runtime.
      if (is.character(v_chk) && length(v_chk) == 1 &&
          is.character(new[[p]]) && length(new[[p]]) == 1) {
        m_dyn <- regmatches(v_chk, gregexpr(DYN_WILDCARD_CAPTURE_PATTERN, v_chk, perl = TRUE))[[1]]
        if (length(m_dyn) > 0 && trimws(v_chk) != m_dyn[1]) {
          k_dyn <- gsub("[{}]", "", m_dyn[1])
          if (k_dyn %in% names(wc_d$objects_for_substitution)) {
            obj_subs <- wc_d$objects_for_substitution[[k_dyn]]
            if (is.list(obj_subs) && length(obj_subs) > 0 &&
                inherits(obj_subs[[1]], "output_req") &&
                k_dyn %in% names(w_info) &&
                w_info[[k_dyn]] %in% names(obj_subs)) {
              modified_req <- obj_subs[[w_info[[k_dyn]]]]
              ## modified_req already carries the producer's own dyn_wildcard
              ## (res_notify$dyn_wildcard) -- per-instance, with static
              ## wildcards already resolved. Do NOT overwrite it with the
              ## consumer's template v_chk, which still carries unresolved
              ## static wildcards.
              ## Per-sample filter: v_chk is the consumer's RESOLVED dep pattern
              ## (static {wc} already substituted, e.g. "dir/85_{{any}}.txt");
              ## modified_req$dyn_wildcard is the producer's pattern. If the
              ## consumer is more specific, capture the basename regex so the
              ## resolver ships only the matching files.
              .sel <- p2p_derive_select(v_chk, modified_req$dyn_wildcard,
                                        sibling_patterns[[p]])
              if(!is.null(.sel)) modified_req$file_sel <- .sel
              ## A per-FILE consumer instance (_exp, not a gather) whose one
              ## file the per-sample filter excludes receives nothing: the
              ## cartesian product pairs every sample with every file, and the
              ## delivery, applying this same filter, would leave the instance an
              ## empty input and stop the supervisor. It is not created. The test
              ## is the delivery's own (p2p_filter_param), so an instance is
              ## dropped exactly when it could not have been fed. Unknown path or
              ## no filter, and nothing changes. The dropped instance comes back
              ## as a marker carrying the resolved pattern, so that
              ## expand_parameters can tell "some files belong to another sample"
              ## from "this sample has no file at all".
              if(!is.null(.sel) && isTRUE(par_expansion[[p]]) && !isTRUE(gather[[p]]) &&
                 !is.null(modified_req$result_path) &&
                 !grepl(.sel, normalize_path_lexical(modified_req$result_path), perl = TRUE))
                return(structure(list(pattern = as.character(v_chk)[1]),
                                 class = "flumen_dropped_instance"))
              ## The consumer's own resolved pattern travels beside the
              ## producer's: it is where a shell consumer's backtick prefix
              ## lives, and the producer's pattern does not carry it. Read at
              ## dispatch by declare_constituent() to state shell_prefix and
              ## the prefix text; the producer's dyn_wildcard is untouched.
              modified_req$consumer_pattern <- as.character(v_chk)[1]
              ## One shape for a resolved dependency: a list of constituents,
              ## here of one; a gather accumulates into it at the next notify.
              new[[p]] <- as_output_req_list(modified_req)
              # Preserve gather intent across notifies: par_xs becomes
              # output_req here, losing its "aggregation" class. At the next
              # notify gather[[p]] is FALSE, so the cumulate branch is entered
              # on the gather_marker attribute alone.
              if (isTRUE(gather[[p]])) attr(new[[p]], "gather_marker") <- TRUE
              if (!isTRUE(par_expansion[[p]])) {
                if (isTRUE(gather[[p]])) {
                  w_info_store[[k_dyn]] <- paste0("[{{", k_dyn, "}}]")
                } else {
                  w_info_store[[k_dyn]] <- paste0("{{", k_dyn, "}}")
                }
              }
            }
          }
        }
      }

      # 3. Output_req Logic (PURE INJECTION + SIGNAL PROTECTION)
      if (inherits(v_chk, "output_req_list") && isFALSE(par_expansion[[p]])) {
        ref_req <- v_chk[[1]]
        k <- extract_dyn_key(ref_req$dyn_wildcard)

        if (!is.null(k) && k %in% names(w_info) && w_info[[k]] %in% names(wc_d$objects_for_substitution[[k]])) {
          o <- wc_d$objects_for_substitution[[k]][[w_info[[k]]]]

          if (inherits(o, "output_req") || (is.list(o) && length(o) > 0 && inherits(o[[1]], "output_req"))) {
            # scatter: replace with the new output_req; gather: accumulate
            if (isTRUE(gather[[p]]) || isTRUE(attr(v_chk, "gather_marker"))) {
              ## The per-sample filter is a property of the GROUP: it is derived
              ## from the consumer's resolved pattern and its siblings (branch
              ## 2.5), never from the instance, so every constituent of the
              ## group carries the same one. The first received it in branch
              ## 2.5; the ones accumulated here arrive raw from the notify.
              if (inherits(o, "output_req") && is.null(o$file_sel) &&
                  !is.null(ref_req$file_sel)) {
                o$file_sel <- ref_req$file_sel
              }
              ## Same for the consumer's pattern: one per group.
              if (inherits(o, "output_req") && is.null(o$consumer_pattern) &&
                  !is.null(ref_req$consumer_pattern)) {
                o$consumer_pattern <- ref_req$consumer_pattern
              }
              ## gather: the constituent joins the list the parameter already is
              new[[p]] <- v_chk
              new[[p]][[length(new[[p]]) + 1]] <- o
              attr(new[[p]], "gather_marker") <- TRUE
            } else {
              ## scatter: this producer's constituent replaces the previous one
              new[[p]] <- as_output_req_list(o)
            }

            # SIGNAL PROTECTION (string metadata). The gather intent is what
            # was DECLARED (gather / gather_marker), never the container's
            # class: a scatter dependency is a list of one constituent too.
            ## any(): dyn_wildcard may be a bundle, and `if (is_double)` on a
            ## logical of length > 1 stops the run. The entries agree.
            is_double <- any(grepl(paste0("\\{\\{", k, "\\}\\}"), ref_req$dyn_wildcard))
            if (isTRUE(gather[[p]]) || isTRUE(attr(new[[p]], "gather_marker"))) {
              w_info_store[[k]] <- if (is_double) paste0("[{{", k, "}}]") else paste0("[{", k, "}]")
            } else {
              w_info_store[[k]] <- if (is_double) paste0("{{", k, "}}") else paste0("{", k, "}")
            }
          }
        }
      }

      # 3b. Vector binding of a COMPOSITE plain input ("id_{sample}",
      # "ref/{ref}.fa"). A plain input -- new_object()/new_file() without _exp,
      # not a dependency, not an output -- receives ALL the values of a static
      # wildcard the task does not expand. The bare token "{sample}" is resolved
      # to the whole wildcard by section 4 below. Text around the token must be
      # resolved HERE and not by the DAG resolution, which covers only some task
      # shapes and gives the token the semantics of a DEPENDENCY: literal under
      # _aggr or by=, one value per clone in a task cloned over its parents.
      # One entry per KEY -- text around the token receives the key -- wildcards
      # substituted in turn and each entry expanded over the next.
      if (isTRUE(vector_input[[p]]) && is.character(new[[p]]) && length(new[[p]]) > 0) {
        txt <- as.character(new[[p]])
        pure_token <- length(txt) == 1L && grepl("^\\{[^{}]+\\}$", trimws(txt))
        toks <- unique(gsub("[{}]", "", unlist(regmatches(
          txt, gregexpr("(?<!\\{)\\{[^{}]+\\}(?!\\})", txt, perl = TRUE)))))
        toks <- toks[toks %in% names(wildcard_list)]
        toks <- toks[vapply(toks, function(k)
          identical(wc_d$values_for_expansion[[k]], paste0("{", k, "}")), logical(1))]
        if (!pure_token && length(toks) > 0) {
          cls <- class(new[[p]])
          for (k in toks) {
            keys <- names(wildcard_list[[k]])
            if (is.null(keys)) keys <- as.character(unlist(wildcard_list[[k]], use.names = FALSE))
            pat  <- paste0("(?<!\\{)\\{", k, "\\}(?!\\})")
            txt  <- unlist(lapply(txt, function(s)
              if (grepl(pat, s, perl = TRUE)) vapply(keys, function(x) gsub(pat, x, s, perl = TRUE), "")
              else s), use.names = FALSE)
          }
          new[[p]] <- txt
          class(new[[p]]) <- cls
        }
      }

      # 4. Complex Objects Substitution
      if (length(v_chk) == 0 || (!is.character(v_chk) && !is.factor(v_chk)) || is.na(as.character(v_chk)[1])) next
      pattern <- WILDCARD_PATTERN
      m <- regmatches(as.character(v_chk)[1], gregexpr(pattern, as.character(v_chk)[1], perl = TRUE))[[1]]
      if (length(m) > 0) {
        k <- gsub("[{}]", "", m[1])
        if (k %in% names(wc_d$objects_for_substitution)) {
          val <- new[[p]]; cls <- class(val)
          if (any(c("flumen_object", "file_path") %in% cls)) {
            is_pure <- (trimws(as.character(v_chk)[1]) == m[1])
            if (length(val) > 1 && all(val %in% names(wc_d$objects_for_substitution[[k]]))) {
              o_list <- lapply(val, function(n) wc_d$objects_for_substitution[[k]][[n]])
              if (is_pure) { new[[p]] <- o_list; attr(new[[p]], "class") <- unique(c(setdiff(cls, c("character", "vector")), "list")) }
            } else if (length(val) == 1 && val %in% names(wc_d$objects_for_substitution[[k]])) {
              o <- wc_d$objects_for_substitution[[k]][[val]]
              if (is_pure) {
                ## A bare token bound to a producer's output_req resolves to a
                ## dependency, which has one shape: a list of constituents.
                new[[p]] <- if (inherits(o, "output_req")) as_output_req_list(o) else o
                ## `cls` is the PLACEHOLDER's class, and a placeholder is a
                ## string, so it carries "character"/"vector". Those describe the
                ## text that has just been replaced, not the value replacing it,
                ## and are dropped here as the sibling branch above already does.
                ## An S3 value (a data.frame) is untouched by the guard.
                if (!is.object(o))
                  class(new[[p]]) <- unique(c(setdiff(cls, c("character", "vector")), class(o)))
                if (isTRUE(gather[[p]])) attr(new[[p]], "gather_marker") <- TRUE
                ## SIGNAL PROTECTION, as in branches 2.5 and 3: a plain (not
                ## _exp, not gather) dependency on a dynamic output receives the
                ## WHOLE result, so the instance is bound to every value of the
                ## key, not to the first one whose name was used as the
                ## substitution key. Left as that name, the parent matcher links
                ## the instance to the FIRST instance only of a sibling expanded
                ## over the same key, and an aggregation of that sibling is
                ## dispatched after that one instance.
                if (inherits(o, "output_req") && !isTRUE(par_expansion[[p]]) &&
                    !isTRUE(gather[[p]]) && grepl("^\\{\\{", m[1]))
                  w_info_store[[k]] <- paste0("{{", k, "}}")
              }
            }
          }
        }
      }
    }
    list(params = new, wc = w_info_store)
  })
}

extract_multiple_wildcard_values <- function(pattern_vector, values_vector) {
  all_w_res <- list()
  all_match <- character(0)
  for (model in pattern_vector) {
    w_names_br <- unique(unlist(regmatches(model, gregexpr("\\{\\{.*?\\}\\}", model, perl = TRUE))))
    w_names <- gsub("[{}]", "", w_names_br)
    if (length(w_names) == 0) {
      next
    }
    reg_mod <- gsub("([.\\|\\(\\)\\[\\]\\$^\\*\\+?])", "\\\\\\1", model, perl = TRUE)
    ## A match of a path against a pattern has three degrees of freedom: where
    ## it may START, where it must END, and what a token may ABSORB. All three
    ## are constrained below.
    ##
    ## ABSORB: the capture may cross '/' only when the pattern is RECURSIVE by
    ## is_recursive_wildcard(), the same predicate that chooses
    ## list.files(recursive = ...) for it. One decision, shared by the lister and
    ## the matcher, so the matcher accepts exactly what the lister enumerates.
    ## The capture stays free to contain dots, so a multi-extension prefix
    ## still matches (idx_base.rev.1.bt2 against idx_base.{{N}}.bt2).
    rec        <- is_recursive_wildcard(model)
    cap_lazy   <- if (rec) "(.*?)" else "([^/]*?)"
    cap_greedy <- if (rec) "(.*)"  else "([^/]*)"
    for (i in seq_along(w_names_br)) {
      esc <- gsub("\\{", "\\\\{", w_names_br[i], fixed = TRUE)
      esc <- gsub("\\}", "\\\\}", esc, fixed = TRUE)
      reg_mod <- if (i < length(w_names_br)) sub(esc, cap_lazy, reg_mod, fixed = TRUE) else sub(esc, cap_greedy, reg_mod, fixed = TRUE)
    }
    ## START: anchored at a path-component boundary, "(?:^|/)", not at "^". A
    ## delivered value may be an absolute path while the pattern is relative,
    ## so the pattern must be allowed to begin after a '/', but never mid-name.
    ## END: anchored at the absolute end, "\\z", not "$", because "$" also
    ## matches before a trailing newline and a file name may carry one.
    ##
    ## inst/scripts/objects.py holds the Python port of this function and must
    ## carry the same three rules: a change here is a change there.
    reg_anch <- paste0("(?:^|/)(?:", reg_mod, ")\\z")
    cur_match <- grep(reg_anch, values_vector, perl = TRUE, value = TRUE)
    if (length(cur_match) == 0) {
      next
    }
    all_match <- c(all_match, cur_match)
    caps <- regmatches(cur_match, regexec(reg_anch, cur_match, perl = TRUE))
    cap_mat <- do.call(rbind, caps)[, -1, drop = FALSE] 
    for (i in seq_along(w_names)) {
      all_w_res[[w_names[i]]] <- unique(c(all_w_res[[w_names[i]]], cap_mat[, i]))
    }
  }
  return(list(wildcards = all_w_res, matches = unique(all_match)))
}