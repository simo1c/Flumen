
## par_uuid -> the resolved pattern that parameter holds in the OTHER instances
## of `op`'s task. Feeds the per-sample filter (p2p_derive_select), which needs
## to know that a more specific sibling exists before it can refuse that
## sibling's files. Sibling instances are the entries of task_list sharing
## op's task_id; the parameter is matched by par_uuid, which is assigned per
## template parameter and therefore identical across the instances.
## Returns an empty list when there are no siblings, so the filter has nothing
## to narrow and the parameter is delivered whole.
collect_sibling_par_patterns <- function(task_list, op){
  tid <- as.character(op$task_id)[1]
  if (is.na(tid) || !nzchar(tid)) return(list())

  out <- list()
  for (other in task_list) {
    if (!identical(as.character(other$task_id)[1], tid)) next
    if (identical(as.character(other$exe_id)[1], as.character(op$exe_id)[1])) next
    for (par in other$par_list) {
      u <- par$par_uuid
      if (is.null(u) || !nzchar(u)) next
      v <- par$value
      inner <- if (inherits(v, "dep_object")) v$value else v
      if (!is.character(inner) || !length(inner)) next
      out[[u]] <- unique(c(out[[u]], as.character(inner)))
    }
  }
  out
}


create_DAG <- function(task_list){
  
  task_ids <- get_all_task_ids(task_list)
  
  if(length(task_ids) == 1){
    DAG <- igraph::make_empty_graph(n = 1)
    DAG <- igraph::set_vertex_attr(DAG, "name", value = task_ids)
  }else{
    
    adj_list <- NULL
    
    for(i in seq_along(task_list)){

      op <- task_list[[i]]
      task_id <- op$task_id
      par_list <- op$par_list

      for(j in seq_along(par_list)){
        
        par <- par_list[[j]]
        par_uuid <- par$par_uuid
        value <- par$value
        
        if("dep_object" %in% class(value)){

          parent_op <- value$task_id

          if(is.null(adj_list)){
            adj_list <- data.frame(from = parent_op, to = task_id, par_uuid = par_uuid)
          }else{
            edge <- c(parent_op, task_id, par_uuid)
            adj_list <- rbind(adj_list, edge)
          }
        }
      }
    }
    
    DAG <- NULL
    
    if(!is.null(adj_list)){
      DAG <- igraph::graph_from_data_frame(adj_list)
      not_connected <- setdiff(task_ids, as.character(unlist(adj_list)))
      n_not_connected <- length(not_connected)
      
      if(n_not_connected){
        DAG <- add_vertices(DAG, n_not_connected, name = not_connected)
      }
    }
  }
  
  return(DAG)
}

substitute_static_wildcards_in_clone <- function(task_clone, parent_exe_ids, lineage_idx, wildcard_list = NULL) {
  static_wds <- names(task_clone$wildcards)[sapply(names(task_clone$wildcards), function(wd) {
    val <- task_clone$wildcards[[wd]]
    is.character(val) && length(val) == 1 && val == paste0("{", wd, "}")
  })]

  if (length(static_wds) == 0) return(task_clone)

  wd_vals     <- list()          # concrete values for 1:1 substitution
  wd_expand_all <- character(0)  # wildcards to expand over all values (fallback)

  for (wd in static_wds) {
    concrete_found <- FALSE
    for (p_id in parent_exe_ids) {
      if (exists(p_id, envir = lineage_idx)) {
        p_op <- lineage_idx[[p_id]]
        p_val <- p_op$wildcards[[wd]]
        # accept only concrete values, not the sentinel "{wd}"
        if (!is.null(p_val) && is.character(p_val) && length(p_val) == 1 &&
            p_val != paste0("{", wd, "}")) {
          wd_vals[[wd]] <- p_val
          concrete_found <- TRUE
          break
        }
      }
    }
    if (!concrete_found && !is.null(wildcard_list) && wd %in% names(wildcard_list)) {
      wl <- wildcard_list[[wd]]
      if (is.list(wl) && length(wl) > 0 && is.atomic(wl[[1]])) {
        wd_expand_all <- c(wd_expand_all, wd)
      }
    }
  }

  if (length(wd_vals) == 0 && length(wd_expand_all) == 0) return(task_clone)

  ## Apply 1:1 substitution and fallback expansion to a character value,
  ## preserving its class (e.g. file_path, object). Used both for bare
  ## character par$value AND for the inner par$value$value of a dep_object,
  ## so that deps written as new_dep(task, value=new_file("...{wd}...")) get
  ## their static wildcard resolved per clone. A dep_object is re-wrapped after
  ## expansion, so substituting the bare character value alone does not reach it.
  do_subst <- function(v) {
    orig_cls <- class(v)
    for (wd in names(wd_vals)) {
      pat <- paste0("(?<!\\{)\\{", wd, "\\}(?!\\})")
      v <- gsub(pat, wd_vals[[wd]], v, perl = TRUE)
    }
    for (wd in wd_expand_all) {
      pat <- paste0("(?<!\\{)\\{", wd, "\\}(?!\\})")
      if (any(grepl(pat, v, perl = TRUE))) {
        ## unlist() yields the wildcard's VALUES, while a binding-driven
        ## resolution uses its KEYS. The two agree here: this branch runs only
        ## when no parent carries a concrete binding for `wd`, i.e. when the
        ## producer is not expanded over it and resolves its own path by this
        ## same expression. The two conditions exclude each other.
        wc_vals <- unname(unlist(wildcard_list[[wd]]))
        v <- unname(as.vector(sapply(v, function(s) {
          sapply(wc_vals, function(wl_v) gsub(pat, wl_v, s, perl = TRUE))
        })))
      }
    }
    class(v) <- orig_cls
    v
  }

  task_clone$par_list <- lapply(task_clone$par_list, function(par) {
    if (is.character(par$value)) {
      par$value <- do_subst(par$value)
    } else if (inherits(par$value, "dep_object") && is.character(par$value$value)) {
      par$value$value <- do_subst(par$value$value)
    }
    par
  })

  ## Resolve the same 1:1 static wildcards in output_files. output_files was
  ## built by wildcard_expansion_and_alignment from the (then still-template)
  ## output par$value; the par_list substitution above updates par$value but not
  ## the separately-stored output_files vector, which would be left holding a
  ## literal "{wc}". The (?<!\{)\{wd\}(?!\}) pattern (as in do_subst) skips
  ## {{dynamic}} entries. Only the 1:1 wd_vals substitutions apply -- an output
  ## must not fan out over the wd_expand_all fallback.
  if (is.character(task_clone$output_files) && length(wd_vals)) {
    for (wd in names(wd_vals)) {
      pat <- paste0("(?<!\\{)\\{", wd, "\\}(?!\\})")
      task_clone$output_files <- gsub(pat, wd_vals[[wd]], task_clone$output_files, perl = TRUE)
    }
  }

  # update wildcards only for concrete substitutions (1:1)
  for (wd in names(wd_vals)) {
    task_clone$wildcards[[wd]] <- wd_vals[[wd]]
  }
  # for wd_expand_all: leave wildcards[[wd]] = "{wd}" (signal unchanged)

  return(task_clone)
}

## Detect op modalities (gather / dynamic scatter / static align).
## Pure function: reads only `op`. Returns flags driving the cloning
## decision in dependency_exp_and_DAG_resolution.
detect_op_modalities <- function(op) {
  is_explicit_agg <- FALSE
  is_dynamic      <- FALSE
  is_static_align <- FALSE

  for (wd in names(op$wildcards)) {
    val <- op$wildcards[[wd]]
    if (!is.character(val) || length(val) != 1L || is.na(val)) next

    pat_static           <- paste0("{",  wd, "}")
    pat_dynamic          <- paste0("{{", wd, "}}")
    pat_explicit_static  <- paste0("[{",  wd, "}]")
    pat_explicit_dynamic <- paste0("[{{", wd, "}}]")

    if (val == wd || val == pat_explicit_static || val == pat_explicit_dynamic) {
      is_explicit_agg <- TRUE
    }
    if (val == pat_dynamic || val == pat_explicit_dynamic) {
      is_dynamic <- TRUE
    }
    if (val == pat_static) {
      is_static_align <- TRUE
    }
  }

  list(
    is_explicit_agg = is_explicit_agg,
    is_dynamic      = is_dynamic,
    is_static_align = is_static_align,
    needs_cloning   = (is_dynamic || is_static_align) && !is_explicit_agg
  )
}

## Match compatible physical parents for an op. Read-only traversal of the
## scheduler indices; returns the per-logical-parent grid and the flat list
## of all matched physical parents. Helper of
## dependency_exp_and_DAG_resolution: takes the indices it needs as args
## (lineage_idx, resolved_physicals are envs; dep_map, replace_op,
## cumulative_replace_op are lists).
match_compatible_parents <- function(op, parent_ids, orig_exe_id,
                                      lineage_idx, resolved_physicals,
                                      dep_map, replace_op,
                                      cumulative_replace_op) {
  all_matched_parents <- character(0)
  matched_by_logical  <- list()

  op_wcs              <- op$wildcards
  op_wd_names         <- names(op_wcs)

  ## Pre-clean op-side wildcard values + literal patterns, invariant for this
  ## call, rather than recomputing them inside the inner sapply.
  op_clean    <- list()
  op_pat_dyn  <- list()
  op_pat_stat <- list()
  for (wd in op_wd_names) {
    val_f <- op_wcs[[wd]]
    op_clean[[wd]]    <- if (is.character(val_f)) sub("^\\[(.*)\\]$", "\\1", val_f) else val_f
    op_pat_dyn[[wd]]  <- paste0("{{", wd, "}}")
    op_pat_stat[[wd]] <- paste0("{",  wd, "}")
  }

  ## The "allowed" set, computed once instead of per iteration: the dep_map ids
  ## plus any post-rename target from the current `replace_op` and from prior
  ## notifies' `cumulative_replace_op`. The pre-rename ids are kept alongside
  ## the renames, because a same-named parent may still be present.
  has_dep_filter <- !is.null(dep_map) && !is.null(dep_map[[orig_exe_id]])
  allowed_set <- NULL
  if (has_dep_filter) {
    allowed_base <- dep_map[[orig_exe_id]]
    extra <- unlist(lapply(allowed_base, function(p) {
      r1 <- if (p %in% names(replace_op)) replace_op[[p]] else NULL
      r2 <- if (!is.null(cumulative_replace_op) && p %in% names(cumulative_replace_op))
              cumulative_replace_op[[p]] else NULL
      c(r1, r2)
    }))
    allowed_set <- unique(c(allowed_base, extra))
  }

  for (p_id in parent_ids) {
    all_p_physicals <- if (exists(p_id, envir = resolved_physicals)) {
      get(p_id, envir = resolved_physicals)
    } else character(0)

    ## Cheap set-based pre-filter: it drops the physicals the inner sapply
    ## would reject at its early `return(FALSE)` guards anyway. intersect()
    ## keeps the order of its first argument, which `valid_p` downstream
    ## depends on.
    ##
    ## The scheduler's dep_map is the only record of who depends on whom; the
    ## task carries no second copy of its parent set.
    if (has_dep_filter) {
      all_p_physicals <- intersect(all_p_physicals, allowed_set)
    }

    if (length(all_p_physicals) == 0L) {
      matched_by_logical[[p_id]] <- character(0)
      next
    }

    matching_parents <- vapply(all_p_physicals, function(p_exe_id) {
      p_op       <- lineage_idx[[p_exe_id]]
      p_wcs      <- p_op$wildcards
      common_wds <- intersect(op_wd_names, names(p_wcs))
      if (length(common_wds) == 0) return(TRUE)

      for (wd in common_wds) {
        clean_f <- op_clean[[wd]]
        val_p   <- p_wcs[[wd]]
        clean_p <- if (is.character(val_p)) sub("^\\[(.*)\\]$", "\\1", val_p) else val_p
        ## clean_p == op_pat_stat[[wd]] : parent-side bare wildcard sentinel.
        ## A producer that references `{wd}` without _exp stays single-instance
        ## and carries the sentinel forever; this clause makes it match any
        ## concrete consumer value (broadcast semantics), symmetric to the
        ## consumer-side bare match.
        ##
        ## clean_p == op_pat_dyn[[wd]] : parent-side UNRESOLVED dynamic sentinel
        ## `{{wd}}`. When a producer has >1 dynamic-output parent, each parent's
        ## RESULT re-expands the producer along that parent's axis only, leaving
        ## the OTHER dynamic axes still `{{wd}}` on the fresh sibling instance,
        ## while a downstream consumer may have inherited the transiently
        ## resolved value of that axis (inherit_parent_wildcards). Treating an
        ## unresolved producer axis as broadcast-compatible lets the consumer
        ## re-link to the new sibling.
        if (!(clean_f == wd || clean_f == op_pat_dyn[[wd]] ||
              clean_f == op_pat_stat[[wd]] || clean_f == clean_p ||
              clean_p == op_pat_stat[[wd]] || clean_p == op_pat_dyn[[wd]])) {
          return(FALSE)
        }
      }
      TRUE
    }, logical(1))

    valid_p <- all_p_physicals[matching_parents]
    matched_by_logical[[p_id]] <- valid_p
    all_matched_parents        <- c(all_matched_parents, valid_p)
  }

  list(all_matched_parents = all_matched_parents,
       matched_by_logical  = matched_by_logical)
}

dependency_exp_and_DAG_resolution <- function(task_list, dag, dep_map = NULL, wildcard_list = NULL,
                                              cumulative_replace_op = NULL) {

  topo_order <- as.character(igraph::V(dag)$name[igraph::topo_sort(dag)])
  ops_by_id <- split(task_list, sapply(task_list, function(x) x$task_id))

  ## Hashed environments: O(1) [[name]] lookup vs O(N) on named lists.
  ## Mutated by the inner `register_op` closure via `<-` (env passes by
  ## reference, no `<<-` needed).
  final_workflow <- new.env(hash = TRUE, parent = emptyenv())
  final_dep_map  <- new.env(hash = TRUE, parent = emptyenv())
  replace_op     <- new.env(hash = TRUE, parent = emptyenv())

  ## as.list() on a hashed environment returns its entries in HASH order, so the
  ## order of the task list would follow the exe_id strings rather than the order
  ## the instances were created, and with exe_ids minted as uuids the same build
  ## yields a different order every time. Creation order is recorded here and
  ## replayed at the return, which makes the order independent of the ids.
  registration_order <- new.env(hash = FALSE, parent = emptyenv())
  registration_order$ids <- character(0)

  lineage_idx <- new.env(hash = TRUE, parent = emptyenv())
  global_lineage_registry <- new.env(hash = TRUE, parent = emptyenv())
  resolved_physicals <- new.env(hash = TRUE, parent = emptyenv())

  # --- HELPER: inherit parents' concrete wildcards onto a child clone ---
  # Propagates an axis (e.g. {ref}) from upstream to downstream when the
  # child does not produce that wildcard itself but its parents agree on a
  # single concrete value. Without this, a multi-stage DAG where an axis is
  # introduced upstream and consumed downstream (e.g. C aggr by="ref" over a
  # scalar consumer B of A) loses the axis at B; match_compatible_parents
  # then collapses C clones to a single global aggregate via lineage dedup.
  inherit_parent_wildcards <- function(task_clone, p_exe_ids) {
    if (length(p_exe_ids) == 0) return(task_clone)
    ## Aggregation consumers gather over every wildcard EXCEPT their `by=` keys.
    ## Inheriting a gathered axis here -- pinning a single value agreed on by one
    ## already-matched parent -- would make match_compatible_parents reject the
    ## sibling producer instances the aggregation must collect. So inheritance is
    ## restricted to the `by=` keys when the op carries an aggregation dependency
    ## (a full aggregation, by=NULL, inherits nothing). Non-aggregation clones
    ## are unaffected.
    agg_by_keys     <- NULL
    is_agg_consumer <- FALSE
    for (p in task_clone$par_list) {
      v   <- p$value
      agg <- if (inherits(v, "aggregation")) v
             else if (inherits(v, "dep_object") && inherits(v$value, "aggregation")) v$value
             else if (inherits(v, "dep_list")) {
               hit <- NULL
               for (d in v) if (inherits(d$value, "aggregation")) { hit <- d$value; break }
               hit
             } else NULL
      if (!is.null(agg)) {
        is_agg_consumer <- TRUE
        agg_by_keys     <- c(agg_by_keys, attr(agg, "aggr_by"))
      }
    }
    agg_by_keys <- unique(agg_by_keys)
    parent_wcs <- lapply(p_exe_ids, function(p_id) {
      if (exists(p_id, envir = lineage_idx)) lineage_idx[[p_id]]$wildcards else list()
    })
    all_keys <- unique(unlist(lapply(parent_wcs, names)))
    for (wd in all_keys) {
      if (wd %in% names(task_clone$wildcards)) next
      if (is_agg_consumer && !(wd %in% agg_by_keys)) next
      vals <- unique(unlist(lapply(parent_wcs, function(wc) wc[[wd]])))
      vals <- vals[!is.null(vals)]
      if (length(vals) == 1L && is.character(vals) &&
          vals != paste0("{", wd, "}") && vals != paste0("{{", wd, "}}")) {
        task_clone$wildcards[[wd]] <- vals
      }
    }
    task_clone
  }

  # --- HELPER: Registration and Deduplication (Lineage) ---
  register_op <- function(current_op, p_exe_ids, node_id, orig_id = NULL) {
    curr_id <- as.character(current_op$exe_id)
    if (length(curr_id) == 0 || curr_id == "") return(NULL)

    p_exe_ids <- unique(as.character(p_exe_ids))
    p_exe_ids <- p_exe_ids[p_exe_ids != ""]

    p_lineage_hashes <- sort(unlist(lapply(p_exe_ids, function(p_id) {
      if (exists(p_id, envir = lineage_idx)) lineage_idx[[p_id]]$lineage_colour else NULL
    })))

    ## A parentless op is identified by its wildcard BINDING as well as by its
    ## parameters. `colour` is hash(par_list) (R/task.R) and the wildcard KEY
    ## never reaches par_list -- new_object_exp("{sample}") substitutes the VALUE
    ## -- so two entries holding equal values would hash equal and the second be
    ## dropped by the registry hit below.
    ##
    ## In the ATOMIC form the key IS the value, so c("a","a","b") still collapses
    ## to two.
    ##
    ## A parented op is identified by its parameters and its parents, which is
    ## what collapses an aggregation consumer's clones into one node, AND by the
    ## CONCRETE keys it is bound to. Without them the same hazard holds one level
    ## down: rep = list(rep1 = cfg, rep2 = cfg, rep3 = cfg) under one producer
    ## gives three clones with equal parameters and equal parents, and rep2 and
    ## rep3 are dropped, with exit 0 -- one replicate instead of three. Only
    ## concrete keys enter: a gathered or not-yet-resolved axis is bound to a
    ## sentinel ("[{s}]", "{{n}}"), equal across the clones it must collapse, and
    ## an inherited key is the parents' own, already in p_lineage_hashes. Sorted
    ## by name, so the order the bindings were added in cannot split a node.
    new_lineage <- if(length(p_exe_ids) == 0) {
      rlang::hash(list(current_op$task_id, current_op$colour, current_op$wildcards))
    } else {
      wcs  <- current_op$wildcards
      keys <- character(0)
      for (wd in sort(names(wcs))) {
        v <- wcs[[wd]]
        if (is.character(v) && length(v) == 1L && !is.na(v) &&
            !grepl("{", v, fixed = TRUE) && !identical(v, wd))
          keys[[wd]] <- v
      }
      rlang::hash(list(current_op$colour, p_lineage_hashes, keys))
    }

    if (exists(new_lineage, envir = global_lineage_registry)) {
      existing_id <- get(new_lineage, envir = global_lineage_registry)
      if (existing_id != curr_id) {
        replace_op[[curr_id]] <- unique(c(replace_op[[curr_id]], existing_id))
      }
      if (!is.null(orig_id) && existing_id != orig_id) {
        replace_op[[orig_id]] <- unique(c(replace_op[[orig_id]], existing_id))
      }
      current_resolved <- if(exists(node_id, envir = resolved_physicals)) get(node_id, envir = resolved_physicals) else character(0)
      assign(node_id, unique(c(current_resolved, existing_id)), envir = resolved_physicals)
      return(NULL)
    }

    ## An exe_id is MINTED, never assembled. Five sites write one: the three task
    ## constructors, the wildcard expansion and the clone above. It is the key
    ## final_workflow, exe_id_to_idx, the dependency maps and the task table are
    ## all indexed by.
    current_op$lineage_colour <- new_lineage
    final_workflow[[curr_id]] <- current_op
    if (!(curr_id %in% registration_order$ids))
      registration_order$ids <- c(registration_order$ids, curr_id)
    final_dep_map[[curr_id]]  <- p_exe_ids
    lineage_idx[[curr_id]]    <- current_op
    assign(new_lineage, curr_id, envir = global_lineage_registry)

    if (!is.null(orig_id) && curr_id != orig_id) {
      replace_op[[orig_id]] <- unique(c(replace_op[[orig_id]], curr_id))
    }
    
    current_resolved <- if(exists(node_id, envir = resolved_physicals)) get(node_id, envir = resolved_physicals) else character(0)
    assign(node_id, unique(c(current_resolved, curr_id)), envir = resolved_physicals)
  }
  
  # --- HELPER: Process clone-or-aggregate branch ---
  # Builds the cartesian grid of compatible parents and registers one op per
  # combination (clone) or aggregates when the grid collapses. Side-effect:
  # calls `register_op` (outer closure) which mutates the registry envs
  # (final_workflow, final_dep_map, replace_op) by reference.
  process_clone_or_aggregate <- function(op, modalities, matched_by_logical,
                                         all_matched_parents, node_id, orig_exe_id) {
    grid <- do.call(expand.grid, c(matched_by_logical, stringsAsFactors = FALSE,
                                   KEEP.OUT.ATTRS = FALSE))

    ## Multi-parent zip alignment: when an op depends on >1 logical parent
    ## (e.g. diamond D <- B,C), the Cartesian product over their physical
    ## instances produces |B| x |C| rows. Restrict to combos where parents
    ## agree on every wildcard they all carry as a concrete value (natural
    ## join on the shared axes). Without this, D would clone over every
    ## parent pair instead of zip-aligning by the shared wildcards.
    if (nrow(grid) > 1 && ncol(grid) > 1) {
      keep <- vapply(seq_len(nrow(grid)), function(i) {
        p_ids <- as.character(unlist(grid[i, ]))
        wcs <- lapply(p_ids, function(p_id) {
          if (exists(p_id, envir = lineage_idx)) lineage_idx[[p_id]]$wildcards else list()
        })
        is_concrete <- function(v) {
          is.character(v) && length(v) == 1L &&
            !grepl("^\\[?\\{+[^{}]+\\}+\\]?$", v)
        }
        all_keys <- unique(unlist(lapply(wcs, names)))
        for (wd in all_keys) {
          vals <- lapply(wcs, function(w) w[[wd]])
          concrete <- Filter(is_concrete, vals)
          if (length(unique(unlist(concrete))) > 1L) return(FALSE)
        }
        TRUE
      }, logical(1))
      if (any(keep)) grid <- grid[keep, , drop = FALSE]
    }

    if (nrow(grid) > 1) {
      for (i in seq_len(nrow(grid))) {
        clone_p_exe_ids <- as.character(unlist(grid[i, ]))
        task_clone <- op
        ## A clone is a NEW instance, so it gets a new id, the same rule
        ## object_func.R applies when an expansion fans out. register_op()
        ## records the lineage through `orig_id`, a name-keyed map, so the id
        ## never has to spell out its parent. The parents of this clone are
        ## declared to register_op() below and recorded in the dependency map;
        ## they are NOT also written onto the task.
        task_clone$exe_id <- uuid::UUIDgenerate()
        if (modalities$is_static_align) {
          task_clone <- substitute_static_wildcards_in_clone(task_clone, clone_p_exe_ids, lineage_idx, wildcard_list)
        }
        task_clone <- inherit_parent_wildcards(task_clone, clone_p_exe_ids)
        register_op(task_clone, clone_p_exe_ids, node_id, orig_id = orig_exe_id)
      }
    } else if (nrow(grid) == 1) {
      clone_p_exe_ids <- as.character(unlist(grid[1, ]))
      if (modalities$is_static_align) {
        op <- substitute_static_wildcards_in_clone(op, clone_p_exe_ids, lineage_idx, wildcard_list)
      }
      op <- inherit_parent_wildcards(op, clone_p_exe_ids)
      register_op(op, clone_p_exe_ids, node_id, orig_id = orig_exe_id)
    } else {
      op <- inherit_parent_wildcards(op, all_matched_parents)
      register_op(op, all_matched_parents, node_id)
    }
  }

  # --- TOPOLOGICAL LOOP OVER THE GRAPH ---
  for (node_id in topo_order) {
    current_defs <- ops_by_id[[node_id]]
    if (is.null(current_defs)) next

    parent_ids <- as.character(names(igraph::neighbors(dag, node_id, mode = "in")))

    for (op in current_defs) {
      orig_exe_id <- as.character(op$exe_id)
      modalities  <- detect_op_modalities(op)

      if (length(parent_ids) > 0) {
        match_result <- match_compatible_parents(op, parent_ids, orig_exe_id,
                                                  lineage_idx, resolved_physicals,
                                                  dep_map, replace_op,
                                                  cumulative_replace_op)

        if (modalities$needs_cloning) {
          process_clone_or_aggregate(op, modalities,
                                     match_result$matched_by_logical,
                                     match_result$all_matched_parents,
                                     node_id, orig_exe_id)
        } else {
          # GATHER: funnel triggered by brackets
          op <- inherit_parent_wildcards(op, match_result$all_matched_parents)
          register_op(op, match_result$all_matched_parents, node_id)
        }
      } else {
        # Root
        if (modalities$is_static_align && !is.null(wildcard_list)) {
          op <- substitute_static_wildcards_in_clone(op, character(0), lineage_idx, wildcard_list)
        }
        register_op(op, character(0), node_id)
      }
    }
  }

  ## Back to lists: prepare_workflow_execution and the consumers downstream of
  ## it all expect named lists.
  replace_op_list <- as.list(replace_op)
  if (length(replace_op_list) == 0) replace_op_list <- NULL

  ## Materialise in CREATION order (see registration_order above), not in the
  ## hash order as.list() happens to give. Entries that were registered and later
  ## removed are skipped, and anything that reached the env by another route is
  ## appended in a sorted, therefore reproducible, order.
  ##
  ## `workflow` and `dependency_map` are two environments holding the SAME keys,
  ## and downstream code pairs them BY POSITION, not by name. Reordering one
  ## alone mis-pairs every instance with someone else's dependencies, so the two
  ## vectors below are built from one list of ids and must stay in step.
  ordered_ids <- Filter(function(i) exists(i, envir = final_workflow, inherits = FALSE),
                        registration_order$ids)
  ordered_ids <- c(ordered_ids, sort(setdiff(ls(final_workflow), ordered_ids)))
  dep_ids     <- ordered_ids[ordered_ids %in% ls(final_dep_map)]
  dep_ids     <- c(dep_ids, sort(setdiff(ls(final_dep_map), dep_ids)))

  result <- list(
    workflow       = unname(mget(ordered_ids, envir = final_workflow)),
    dependency_map = mget(dep_ids, envir = final_dep_map),
    replace_op     = replace_op_list
  )
  return(result)
}

prepare_workflow_execution <- function(task_list,
                                       DAG,
                                       op_to_expand,
                                       wildcard_list,
                                       dep_map = NULL,
                                       cumulative_replace_op = NULL){

  op_ready_to_execute <- c()
  expanded_tasks <- c()
  required_shapes <- list()
  replace_op <- list()

  ## wildcard substitution, expansion and alignment
  for(i in seq_along(task_list)){

    op <- task_list[[i]]
    exe_id <- op$exe_id
    expanded_list_of_task_object <- new_task_list(task_list = list(op))

    if(i %in% op_to_expand){
      if(length(op$par_list) > 0){
        ## Per-sample filter: collect, per parameter, the pattern that SAME
        ## parameter carries in the sibling instances of this task, so the filter
        ## is not built looking at one sample in isolation. Keyed by par_uuid,
        ## which is stable across instances of a task. Empty list -> no narrowing.
        .sibs <- collect_sibling_par_patterns(task_list, op)
        expanded_list_of_task_object <- wildcard_expansion_and_alignment(op, wildcard_list, .sibs)
      }
    }

    op_ready_to_execute <- c(op_ready_to_execute, expanded_list_of_task_object)

    replace <- sapply(expanded_list_of_task_object, function(op){
      op$exe_id
    })

    replace <- list(replace)
    names(replace) <- exe_id
    replace_op <- c(replace_op, replace)

  }

  if (!is.null(dep_map)) {
    dep_map <- lapply(dep_map, function(parents) {
      unique(unlist(lapply(parents, function(p) {
        if (p %in% names(replace_op)) replace_op[[p]] else p
      })))
    })
  }

  workflow_expanded <- dependency_exp_and_DAG_resolution(op_ready_to_execute, DAG, dep_map, wildcard_list,
                                                         cumulative_replace_op = cumulative_replace_op)

  if (!is.null(workflow_expanded$replace_op)) {
    # Save original values to exclude any intermediate identifiers
    orig_vals <- unlist(replace_op)

    # 1. Update existing mappings, resolving chained substitutions where an id
    #    was replaced and its replacement was replaced in turn
    for (key in names(replace_op)) {
      current_vals <- replace_op[[key]]
      new_vals <- character(0)

      for (val in current_vals) {
        if (val %in% names(workflow_expanded$replace_op)) {
          new_vals <- c(new_vals, workflow_expanded$replace_op[[val]])
        } else {
          new_vals <- c(new_vals, val)
        }
      }
      replace_op[[key]] <- new_vals
    }

    # 2. Add only new keys that do NOT derive from intermediate steps of the first pass
    for (key in names(workflow_expanded$replace_op)) {
      if (!(key %in% names(replace_op)) && !(key %in% orig_vals)) {
        replace_op[[key]] <- workflow_expanded$replace_op[[key]]
      }
    }
  }

  workflow_exe <- list(
    task_list = workflow_expanded$workflow,
    required_shapes = workflow_expanded$dependency_map,
    replace_op = replace_op
  )

  return(workflow_exe)
}

