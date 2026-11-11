## Workflow-template validation and value-tree introspection helpers.
##
## extract_wildcards_from_value walks a par value (including dep_object and
## dep_list wrappers) and returns the static/dynamic wildcard names it finds.
## normalize_path_lexical canonicalises path patterns for pre-runtime equality
## checks (does not touch the filesystem). validate_workflow runs the full
## pre-expansion validation pass on user-provided task_list templates.


## Extract static and dynamic wildcards from a parameter value, recursively
## navigating dep_object / dep_list. Character values (with class, e.g.
## file_path, expansion, aggregation) are handled by the is.character() branch.
extract_wildcards_from_value <- function(v){
  if(is.null(v)) return(list(static = character(0), dynamic = character(0)))
  if(inherits(v, "dep_object")) return(extract_wildcards_from_value(v$value))
  if(inherits(v, "dep_list") && length(v) > 0){
    res <- lapply(v, function(d) extract_wildcards_from_value(d$value))
    return(list(
      static  = unique(unlist(lapply(res, `[[`, "static"))),
      dynamic = unique(unlist(lapply(res, `[[`, "dynamic")))
    ))
  }
  if(is.character(v)){
    s <- paste(as.character(v), collapse = "\n")
    static_m <- regmatches(s, gregexpr(STATIC_WILDCARD_CAPTURE_PATTERN, s, perl = TRUE))[[1]]
    dyn_m    <- regmatches(s, gregexpr(DYN_WILDCARD_CAPTURE_PATTERN,    s, perl = TRUE))[[1]]
    return(list(
      static  = unique(gsub("[{}]", "", static_m)),
      dynamic = unique(gsub("[{}]", "", dyn_m))
    ))
  }
  return(list(static = character(0), dynamic = character(0)))
}

## TRUE for an ambiguous "bare folder" output declaration: a single-segment path
## with no extension, no wildcard (static or dynamic) and no backtick -- or any
## path ending in '/'.
is_ambiguous_folder_path <- function(p) {
  if (!is.character(p) || length(p) == 0L || !nzchar(p)) return(FALSE)
  if (grepl("/$", p)) return(TRUE)
  has_slash    <- grepl("/", p, fixed = TRUE)
  has_dot      <- grepl("\\.", basename(p))
  has_dyn_wc   <- grepl("\\{\\{", p, fixed = TRUE)
  has_static_wc <- grepl(STATIC_WILDCARD_PATTERN, p, perl = TRUE)
  has_backtick <- grepl("`", p, fixed = TRUE)
  ## A backtick prefix is only meaningful around a wildcard placeholder. A
  ## backtick path with no wildcard at all (e.g. `dir/`) is ambiguous.
  ## The regex here, not `has_dyn_wc`: that one is fixed=TRUE on an escaped
  ## pattern and never matches a real "{{...}}", so the sanctioned
  ## `dir/`{{any}} form would be rejected by this clause.
  if (has_backtick && !grepl(DYN_WILDCARD_PATTERN, p, perl = TRUE) && !has_static_wc) return(TRUE)
  return(!has_slash && !has_dot && !has_dyn_wc && !has_static_wc && !has_backtick)
}

## Lexical path normalization (does NOT touch the filesystem). Used to make
## consumer/producer dep value patterns comparable irrespective of cosmetic
## differences (leading "./", repeated slashes, mid-path "/./", trailing "/",
## flumen-specific backtick literals). Wildcards `{name}` and `{{name}}` are
## preserved untouched -- they are pattern markers, not path syntax.
normalize_path_lexical <- function(p) {
  if (is.null(p) || length(p) == 0) return(p)
  p <- as.character(p)
  p <- strip_backticks(p)
  p <- gsub("^\\./", "", p)
  p <- gsub("/+", "/", p)
  p <- gsub("/\\./", "/", p)
  p <- sub("/$", "", p)
  return(p)
}


## Collision key for the output-uniqueness guards: case-folded, so two outputs
## whose paths differ only in case count as ONE file. Folded on every platform,
## because on macOS and Windows `out/s1.txt` and `out/S1.txt` are one file and a
## name must not rely on case anywhere. Kept apart from normalize_path_lexical(),
## which also MATCHES a dependency against its producer's output pattern, where
## case-folding would change which producer a consumer binds to.
output_collision_key <- function(p) {
  tolower(normalize_path_lexical(p))
}

## Workflow validation on task_list templates. Fails only on clearly broken
## cases, each labelled below with the number it carries in the body:
##   (1)  empty task_list
##   (2)  duplicate task_ids across templates, case-insensitively
##   (3)  new_dep(task_id = "X") with X not present among templates
##   (3a) a task depending on itself
##   (3b) object aggregation on a static {wc} pattern, which matches no producer
##   (3c) new_dep(task_id="X", value=<file>) where value pattern does not match
##        any output of producer X (catches typos and producer rename mismatch
##        at construction instead of silent runtime fail), and (3c-type) where
##        it matches but a file dep names an object output, or the reverse
##   (3d) _exp / _aggr declared on an output parameter
##   (4)  multiple dynamic wildcards {{...}} in the same parameter
##   (4b) the same dynamic wildcard in more than one parameter of a task
##   (4c) two or more pure-dynamic outputs (no static discriminator) in one task
##        -- mutually unselectable by a consumer (see the ambiguity rejection)
##   (5)  static wildcard {name} used in task_list but not declared in wildcard_list
##   (5b-5e) a declared wildcard with no values, an empty or NA value, a
##        backslash, or its own name as a value
##   (6)  warning, not stop: wildcard declared in wildcard_list but unused
## Plus the unnumbered checks inlined in the loop (duplicate parameter names, a
## vector output repeating a dynamic pattern) and the validate_* functions
## called at the end.
## Must be called BEFORE prepare_workflow_execution(): expansion keeps the
## task_id of every instance and tells them apart by exe_id, so afterwards a
## task expanded into N instances reads as N duplicate task_ids.
validate_workflow <- function(task_list, wildcard_list = NULL) {

  ## A task_object is itself a list, so a forgotten list() (task_list = my_task)
  ## has non-zero length and reaches the vapply below with an opaque error.
  ## Catch that and a list holding non-tasks here.
  if(inherits(task_list, "task_object")){
    stop(flumen_reject(
      call  = "new_workflow()",
      error = "'task_list' must be a list of tasks, not a single task",
      fix   = "task_list = list(my_task)"), call. = FALSE)
  }

  if(length(task_list) == 0){
    stop(flumen_reject(
      call  = "new_workflow()",
      error = "'task_list' must contain at least one operation",
      fix   = "task_list = list(task_a, task_b)"), call. = FALSE)
  }

  not_task <- !vapply(task_list, function(op) inherits(op, "task_object"), logical(1))
  if(any(not_task)){
    stop(flumen_reject(
      call  = "new_workflow()",
      error = sprintf("'task_list' element(s) %s have class %s",
                      paste(which(not_task), collapse = ", "),
                      paste(shQuote(vapply(task_list[not_task],
                                           function(x) paste(class(x), collapse = "/"),
                                           character(1))), collapse = ", ")),
      fix   = "use new_R_task(), new_python_task() or new_shell_task()"), call. = FALSE)
  }

  task_ids <- vapply(task_list, function(op) as.character(op$task_id), character(1))

  dups <- unique(task_ids[duplicated(task_ids)])
  if(length(dups) > 0){
    stop(flumen_reject(
      call  = "new_workflow()",
      error = sprintf("duplicate task_id(s): %s", paste(shQuote(dups), collapse = ", ")),
      fix   = "give each task a unique task_id"), call. = FALSE)
  }

  ## ...and not merely unique, but distinguishable without relying on case.
  ## The task_id names a task on every user-facing surface (the Op_ID column,
  ## the `task` line of a failure block, the scheduler log), none of which is
  ## read case by case. Rejected on every platform, like the output-path twins.
  ci <- tolower(task_ids)
  ci_dups <- unique(ci[duplicated(ci)])
  if(length(ci_dups) > 0){
    twins <- task_ids[ci %in% ci_dups]
    stop(flumen_reject(
      call  = "new_workflow()",
      error = sprintf("task_id(s) %s differ only in case",
                      paste(shQuote(unique(twins)), collapse = ", ")),
      fix   = "make the names differ by more than case"), call. = FALSE)
  }

  all_static_used <- character(0)
  for(op in task_list){
    if(is.null(op$par_list)) next

    ## Parameter names within a task must be unique. A duplicate (two inputs
    ## named 'x', or an input and output sharing a name) collides in the wrapper's
    ## named par_list and its per-parameter length bookkeeping, and the supervisor
    ## dies at runtime on "invalid 'times' argument". Unnamed parameters are
    ## exempt: in a shell task they are the POSITIONAL arguments (`mv src dst`),
    ## and naming them would push the name itself into argv.
    if(!is.null(op$par_names)){
      named_pn <- op$par_names[nzchar(op$par_names)]
      dup_pn <- unique(named_pn[duplicated(named_pn)])
      if(length(dup_pn) > 0){
        stop(flumen_reject(
          call  = "new_workflow()",
          task  = op$task_id,
          error = sprintf("duplicate parameter name(s): %s",
                          paste(shQuote(dup_pn), collapse = ", ")),
          fix   = "give each input and output parameter a unique name"), call. = FALSE)
      }
    }

    ## Track which parameter(s) use each DYNAMIC wildcard name within THIS task:
    ## reused across parameters, its resolved values pool into one shared bucket.
    ## Static {name} correlation wildcards are exempt (sharing them is the point).
    task_dyn_use <- list()
    ## Track OUTPUT parameters that carry no static discriminator (a pure
    ## dynamic {{name}} with no literal text and no static {wc}). Such outputs
    ## are NOT individually addressable by a consumer. A single one is fine (the
    ## canonical `new_object("{{out}}")` sole output); TWO OR MORE make the
    ## producer's outputs mutually unselectable (see the ambiguity rejection
    ## after this loop).
    nonaddr_out <- integer(0)
    for(j in seq_along(op$par_list)){
      par <- op$par_list[[j]]
      if(is.null(par$value)) next

      ## (3) new_dep reference check
      dep_op <- NULL
      dep_value_for_check <- NULL
      if("dep_object" %in% class(par$value)){
        dep_op <- par$value$task_id
        dep_value_for_check <- par$value$value
      } else if("dep_list" %in% class(par$value) && length(par$value) > 0){
        dep_op <- par$value[[1]]$task_id
        dep_value_for_check <- par$value[[1]]$value
      }
      if(!is.null(dep_op) && nchar(dep_op) > 0 && !(dep_op %in% task_ids)){
        stop(flumen_reject(
          call  = "new_workflow()",
          task  = op$task_id,
          error = sprintf("new_dep(task_id = '%s') names a task that is not in task_list",
                          dep_op),
          fix   = sprintf("use one of the known task_ids: %s",
                          paste(shQuote(task_ids), collapse = ", "))), call. = FALSE)
      }

      ## (3a) self-dependency: a task cannot consume its own output. create_DAG
      ## drops the self-edge, so the dep never enters required_shapes, and the
      ## A<->B cycle check does not catch a length-1 self-loop.
      if(!is.null(dep_op) && nchar(dep_op) > 0 && identical(dep_op, op$task_id)){
        stop(flumen_reject(
          call  = "new_workflow()",
          task  = op$task_id,
          error = sprintf("new_dep(task_id = '%s') points at the task itself", dep_op),
          fix   = "remove the self-dependency, or split the task in two"), call. = FALSE)
      }

      ## (3b) object aggregation must reference a DYNAMIC {{name}} wildcard.
      ## Objects aggregate by dynamic name -- new_object_aggr("{{name}}") matching
      ## the producer's new_object("{{name}}"). A static {wc} pattern is a
      ## file-only construct and matches no object producer. File aggregations
      ## carry the "file_object" class and are exempt.
      if(inherits(dep_value_for_check, "aggregation") &&
         inherits(dep_value_for_check, "flumen_object") &&
         !inherits(dep_value_for_check, "file_object") &&
         is.null(find_dynamic_wildcards(as.character(dep_value_for_check)))){
        by_attr <- attr(dep_value_for_check, "aggr_by")
        by_txt  <- if(!is.null(by_attr))
                     sprintf(", by = '%s'", paste(by_attr, collapse = "', '")) else ""
        stop(flumen_reject(
          call  = "new_workflow()",
          task  = op$task_id,
          error = sprintf("new_object_aggr('%s'%s) uses a static wildcard pattern",
                          as.character(dep_value_for_check), by_txt),
          fix   = sprintf(paste("use new_object_aggr('{{name}}'%s), or new_file_aggr()",
                                "for a static {wc}"), by_txt)), call. = FALSE)
      }

      ## (3c) dep value-vs-producer-output pattern check.
      ## When the user pre-specifies a path in new_dep(value=...), the pattern
      ## must match (after lexical normalization) at least one output of the
      ## referenced producer. Catches typos and producer-side renames at
      ## construction time, instead of relying on a silent runtime mismatch
      ## in the scheduler dispatch path.
      ## any(nzchar(...)) rather than nchar(x) > 0: a dep declared as a BUNDLE,
      ## new_file(c("a.txt","b.txt")), gives one value per path, which `&&` refuses.
      if(!is.null(dep_op) && !is.null(dep_value_for_check) &&
         is.character(dep_value_for_check) && any(nzchar(dep_value_for_check)) &&
         dep_op %in% task_ids){
        producer <- task_list[[which(task_ids == dep_op)[1]]]
        producer_output_patterns <- character(0)
        producer_output_is_file  <- logical(0)
        if(!is.null(producer$par_list)){
          for(po in producer$par_list){
            if("out_par_object" %in% class(po) && !is.null(po$value)){
              pat <- as.character(unlist(po$value))
              producer_output_patterns <- c(producer_output_patterns, pat)
              producer_output_is_file  <- c(producer_output_is_file,
                rep(inherits(po$value, "file_path") || inherits(po$value, "file_object"),
                    length(pat)))
            }
          }
        }
        if(length(producer_output_patterns) > 0){
          dep_norm <- normalize_path_lexical(dep_value_for_check)
          out_norm <- normalize_path_lexical(producer_output_patterns)
          ## Per-sample filter: accept a consumer dep that SPECIALIZES a producer
          ## output -- same dir + same {{dynamic}} token + same suffix after it,
          ## with an extra static {wc} on the consumer side acting as a per-file
          ## filter (e.g. consumer "dir/{sample}_{{any}}.fastq.gz" vs producer
          ## "dir/{{any}}.fastq.gz"). Correlation stays broadcast; the static
          ## prefix only narrows which producer files reach this consumer.
          .specializes <- function(dep, outs){
            dep <- strip_backticks(dep)   ## backticks are shell markers
            db  <- basename(dep); dd <- normalize_path_lexical(dirname(dep))
            dyn <- regmatches(db, regexpr("[{][{][^}]+[}][}]", db))
            if(length(dyn) == 0) return(FALSE)
            dsuf <- sub(".*[}][}]", "", db)
            dpre <- sub("[{][{][^}]+[}][}].*", "", db)
            for(o in outs){
              o  <- strip_backticks(o)
              ob <- basename(o); od <- normalize_path_lexical(dirname(o))
              odyn <- regmatches(ob, regexpr("[{][{][^}]+[}][}]", ob))
              if(length(odyn) == 0) next
              osuf <- sub(".*[}][}]", "", ob)
              if(identical(dd, od) && identical(dyn, odyn) && identical(dsuf, osuf) &&
                 grepl("[{][^{}]+[}]", dpre)) return(TRUE)
            }
            FALSE
          }
          if(!any(dep_norm %in% out_norm) &&
             !.specializes(dep_value_for_check, producer_output_patterns)){
            stop(flumen_reject(
              call      = "new_workflow()",
              task      = op$task_id,
              parameter = sprintf("#%d", j),
              error     = sprintf("new_dep(task_id = '%s', value = '%s') matches no output of '%s'",
                                  dep_op, dep_value_for_check, dep_op),
              fix       = sprintf("use one of its outputs: %s",
                                  paste(shQuote(producer_output_patterns), collapse = ", "))),
              call. = FALSE)
          } else {
            ## (3c-type) The pattern matched, but the dependency TYPE (file vs
            ## object) must also match the producer's output type: a new_file()
            ## dep on a new_object() producer matches the pattern yet delivers
            ## the wrong kind of value. Require at least one SAME-TYPE producer
            ## output to match.
            dep_is_file <- inherits(dep_value_for_check, "file_path") ||
                           inherits(dep_value_for_check, "file_object")
            st_pat <- producer_output_patterns[producer_output_is_file == dep_is_file]
            if(!(any(dep_norm %in% normalize_path_lexical(st_pat)) ||
                 (length(st_pat) > 0 && .specializes(dep_value_for_check, st_pat)))){
              dep_kw    <- if(dep_is_file) "new_file"   else "new_object"
              dep_kind  <- if(dep_is_file) "file"       else "in-memory object"
              prod_kw   <- if(dep_is_file) "new_object"  else "new_file"
              prod_kind <- if(dep_is_file) "in-memory object" else "file"
              stop(flumen_reject(
                call      = "new_workflow()",
                task      = op$task_id,
                parameter = sprintf("#%d", j),
                error     = sprintf("new_dep('%s', %s('%s')) declares a %s but '%s' produces a %s",
                                    dep_op, dep_kw, dep_value_for_check, dep_kind,
                                    dep_op, prod_kind),
                fix       = sprintf("use %s() to match the producer", prod_kw)),
                call. = FALSE)
            }
          }
        }
      }

      ## (3d) _exp / _aggr forbidden in output parameters.
      ## - "expansion" (new_*_exp) encodes INPUT fan-out: output expansion
      ##   follows naturally from input expansion, declaring it on an output
      ##   is semantically incorrect.
      ## - "aggregation" (new_*_aggr) is a CONSUMER-side gather mechanism
      ##   used inside new_dep(value=...); it has no meaning on a producer output.
      if("out_par_object" %in% class(par)){
        ## An output cannot be a dependency: new_dep() declares a CONSUMED input,
        ## not a PRODUCED output.
        if(inherits(par$value, "dep_object") || inherits(par$value, "dep_list")){
          stop(flumen_reject(
            call      = "new_workflow()",
            task      = op$task_id,
            parameter = sprintf("output #%d", j),
            error     = "new_dep() declares a consumed input, not a produced output",
            fix       = "declare the output with new_file() or new_object()"), call. = FALSE)
        }
        bad_cls <- intersect(c("expansion", "aggregation"), class(par$value))
        if(length(bad_cls) > 0){
          if(bad_cls[1] == "expansion"){
            ctor_msg <- "new_object_exp() / new_file_exp()"
            fix_msg  <- "use new_object() / new_file(); the output expands with the input"
          } else {
            ctor_msg <- "new_object_aggr() / new_file_aggr()"
            fix_msg  <- paste("declare the output with new_object() / new_file();",
                              "aggregation is a consumer-side gather, used inside",
                              "new_dep(value = ...)")
          }
          stop(flumen_reject(
            call      = "new_workflow()",
            task      = op$task_id,
            parameter = sprintf("output #%d", j),
            error     = sprintf("%s is not allowed for outputs", ctor_msg),
            fix       = fix_msg), call. = FALSE)
        }
        ## A vector output must not repeat a DYNAMIC pattern. The vector form
        ## declares a BUNDLE -- several paths under one name -- and two identical
        ## entries declare the same reach twice, which leaves the outcome
        ## unspecified. The engine reads those entries to decide how to narrow a
        ## dependency on this output, and a repeated entry yields a narrowing
        ## that narrows nothing while still marking the copy PARTIAL.
        .vals <- as.character(par$value)
        if(length(.vals) > 1L){
          .dup <- unique(.vals[duplicated(.vals)])
          .dup <- .dup[grepl(DYN_WILDCARD_PATTERN, .dup, perl = TRUE)]
          if(length(.dup)){
            stop(flumen_reject(
              call      = "new_workflow()",
              task      = op$task_id,
              parameter = sprintf("output #%d", j),
              error     = sprintf("the declared value repeats the dynamic pattern %s",
                                  paste(shQuote(.dup), collapse = ", ")),
              fix       = "keep one entry per distinct pattern"), call. = FALSE)
          }
        }
      }

      ## (4) and (5) wildcard checks
      wcs <- extract_wildcards_from_value(par$value)
      if(length(wcs$dynamic) > 1){
        stop(flumen_reject(
          call      = "new_workflow()",
          task      = op$task_id,
          parameter = sprintf("#%d", j),
          error     = sprintf("at most one dynamic wildcard per parameter, found %d: %s",
                              length(wcs$dynamic),
                              paste(shQuote(wcs$dynamic), collapse = ", ")),
          fix       = "keep one, and move the other to its own parameter"), call. = FALSE)
      }
      all_static_used <- union(all_static_used, wcs$static)
      for(dn in wcs$dynamic) task_dyn_use[[dn]] <- union(task_dyn_use[[dn]], j)

      ## An OUTPUT is addressable only via STATIC content: a static {wc} or
      ## literal path text a consumer can name. If, after stripping the dynamic
      ## {{name}} token(s), nothing static remains (empty for an object output
      ## like new_object("{{d}}"), or a pathless new_file("{{d}}")), the output
      ## is pure-dynamic and cannot be selected on its own.
      if("out_par_object" %in% class(par)){
        s_out    <- paste(as.character(par$value), collapse = "\n")
        residual <- gsub(DYN_WILDCARD_CAPTURE_PATTERN,    "", s_out,    perl = TRUE)
        residual <- gsub(STATIC_WILDCARD_CAPTURE_PATTERN, "", residual, perl = TRUE)
        residual <- gsub("[`/[:space:]]", "", residual)
        addressable <- (length(wcs$static) > 0L) || nzchar(residual)
        if(!addressable) nonaddr_out <- c(nonaddr_out, j)
      }
    }
    ## (4b) the same dynamic wildcard {{name}} must not appear in more than one
    ## parameter of a task; reusing it pools the two parameters' values.
    dup_dyn <- names(task_dyn_use)[vapply(task_dyn_use, length, integer(1)) > 1L]
    if(length(dup_dyn)){
      dn <- dup_dyn[1]
      stop(flumen_reject(
        call  = "new_workflow()",
        task  = op$task_id,
        error = sprintf("the dynamic wildcard '{{%s}}' is used in %d parameters (#%s)",
                        dn, length(task_dyn_use[[dn]]),
                        paste(task_dyn_use[[dn]], collapse = ", ")),
        fix   = sprintf("rename one occurrence, e.g. '{{%s}}' -> '{{%s2}}'", dn, dn)),
        call. = FALSE)
    }

    ## (4c) two or more pure-dynamic outputs (no static discriminator) are
    ## mutually unselectable. A consumer addresses an output by its STATIC
    ## content; with several outputs distinguished only by a dynamic {{name}},
    ## no new_dep() can pick one.
    if(length(nonaddr_out) >= 2L){
      stop(flumen_reject(
        call  = "new_workflow()",
        task  = op$task_id,
        error = sprintf("outputs #%s are pure dynamic wildcards with no static discriminator",
                        paste(nonaddr_out, collapse = ", ")),
        fix   = "give each output a static discriminator, new_file(\"dir_a/{{x}}\")"),
        call. = FALSE)
    }
  }

  ## (5) static used but not declared; (6) declared but not used
  declared <- if(is.null(wildcard_list)) character(0) else names(wildcard_list)
  missing_wc <- setdiff(all_static_used, declared)
  if(length(missing_wc) > 0){
    stop(flumen_reject(
      call  = "new_workflow()",
      error = sprintf("wildcard(s) %s used in task_list but not declared in wildcard_list",
                      paste(shQuote(missing_wc), collapse = ", ")),
      fix   = "declare them, new_wildcard_list(sample = c(\"a\", \"b\"))"), call. = FALSE)
  }

  ## (5b-5e) reject malformed wildcard values: no values, an empty or NA value,
  ## a backslash, the wildcard's own name.
  if(!is.null(wildcard_list)){
    for(wd in names(wildcard_list)){
      vals_list <- wildcard_list[[wd]]
      ## (5b) a declared wildcard must carry at least one value; with none, its
      ## {wd} is never substituted and survives literally into every path/param.
      if(length(vals_list) == 0){
        stop(flumen_reject(
          call  = "new_workflow()",
          error = sprintf("wildcard '%s' is declared with no values", wd),
          fix   = sprintf("give it at least one, new_wildcard_list(%s = c(\"a\", \"b\"))",
                          wd)), call. = FALSE)
      }
      vals <- unique(c(names(vals_list),
                       as.character(unlist(vals_list, use.names = FALSE))))
      ## (5c) empty-string / NA values become empty/NA list names in the expansion
      ## engine; the name-based substitution lookup then returns NULL and the engine
      ## aborts with "attempt to set an attribute on NULL".
      if(any(is.na(vals)) || any(!nzchar(vals))){
        stop(flumen_reject(
          call  = "new_workflow()",
          error = sprintf("wildcard '%s' has an empty or NA value", wd),
          fix   = "every value must be a non-empty string"), call. = FALSE)
      }
      ## (5d) backslash: used as a gsub() REPLACEMENT during expansion, an
      ## unescaped backslash is read as a backreference (\1, \U, ...).
      bad  <- vals[grepl("\\", vals, fixed = TRUE)]
      if(length(bad) > 0){
        stop(flumen_reject(
          call  = "new_workflow()",
          error = sprintf("wildcard '%s' has value(s) containing a backslash: %s",
                          wd, paste(shQuote(bad), collapse = ", ")),
          fix   = "remove the backslash"), call. = FALSE)
      }
      ## (5e) the wildcard's own name, bare or in a pattern form, is the sentinel
      ## the expansion engine reads as "aggregate over this wildcard"
      ## (detect_op_modalities and the parent matching in workflow_prepare.R).
      ## An instance bound to such a value is taken for an aggregation and wired
      ## to EVERY parent instance: with cond = c("cond", "ctrl"), B[cond]
      ## received A[ctrl]'s result, with exit 0. Keys are the identity; a string
      ## value is checked too, the cells of an object value are not.
      str_vals <- unlist(lapply(vals_list, function(v)
        if(is.character(v) && length(v) == 1L) v else NULL), use.names = FALSE)
      sentinels <- c(wd, paste0("{", wd, "}"), paste0("{{", wd, "}}"),
                     paste0("[{", wd, "}]"), paste0("[{{", wd, "}}]"))
      bad <- intersect(unique(c(names(vals_list), str_vals)), sentinels)
      if(length(bad) > 0){
        stop(flumen_reject(
          call  = "new_workflow()",
          error = sprintf("wildcard '%s' has the value %s, which is its own name and is reserved",
                          wd, paste(shQuote(bad), collapse = ", ")),
          fix   = "rename the value or the wildcard"), call. = FALSE)
      }
    }
  }

  unused_wc <- setdiff(declared, all_static_used)
  if(length(unused_wc) > 0){
    warning(flumen_warn(
      call    = "new_workflow()",
      warning = sprintf(paste("wildcard(s) %s declared in wildcard_list but never used",
                              "in task_list"),
                        paste(shQuote(unused_wc), collapse = ", ")),
      fix     = "remove them, or use them in a task parameter"), call. = FALSE)
  }

  validate_input_file_existence(task_list)
  validate_output_folder_paths(task_list)
  validate_no_empty_constructor_values(task_list)
  validate_balanced_backticks(task_list)
  validate_backtick_has_static_content(task_list)
  validate_no_backticks_in_non_shell_task(task_list)
  validate_no_bare_wildcard(task_list)
  validate_no_aggr_outside_dep(task_list)
  validate_no_mixed_expansion_drivers(task_list)
  validate_aggr_by_wildcards(task_list, wildcard_list)
  validate_arrow_object_literals(task_list)
  validate_python_literal_arrow_types(task_list)
  validate_no_duplicate_output_paths(task_list)
  validate_no_colliding_expanded_outputs(task_list)
  validate_no_shared_dynamic_namespace(task_list, wildcard_list)
  validate_no_mixed_output_kinds(task_list)

  invisible(TRUE)
}

## Reject a task declaring BOTH a file output and an object output.
##
## The two are captured by mutually exclusive mechanisms: for a file output the
## wrapper enumerates the files produced and IGNORES the return value; for an
## object output the return value IS the result. Declaring both asks the wrapper
## to do both, and the file branch wins -- the returned object is dropped and the
## object slot is filled with the file paths instead.
validate_no_mixed_output_kinds <- function(task_list){
  for(op in task_list){
    if(is.null(op$par_list)) next

    file_outs <- character(0)
    obj_outs  <- character(0)
    for(po in op$par_list){
      if(!("out_par_object" %in% class(po)) || is.null(po$value)) next
      nm <- if(!is.null(po$name) && nzchar(po$name)) po$name else "<unnamed>"
      ## The two constructor families are disjoint by class: new_file*() carries
      ## "file_object", new_object*() carries "object" and never "file_object".
      if(inherits(po$value, "file_object") || inherits(po$value, "file_path")){
        file_outs <- c(file_outs, nm)
      } else if(inherits(po$value, "flumen_object")){
        obj_outs <- c(obj_outs, nm)
      }
    }

    if(length(file_outs) > 0 && length(obj_outs) > 0){
      stop(flumen_reject(
        call  = "new_workflow()",
        task  = op$task_id,
        error = sprintf("declares both file output(s) (%s) and object output(s) (%s)",
                        paste(shQuote(file_outs), collapse = ", "),
                        paste(shQuote(obj_outs), collapse = ", ")),
        fix   = "move the object to a downstream task, or write it to a file as well"),
        call. = FALSE)
    }
  }
  invisible(TRUE)
}

## Reject two DIFFERENT tasks declaring the SAME file output path pattern: at
## runtime both write the same file, and the forward path->producer index the
## scheduler builds becomes ambiguous.
## Object outputs are exempt -- they are keyed per producing instance in the
## result cache, not by a shared disk path, so a shared object name does not
## collide. Same-task pattern reuse across wildcard instances is fine (each
## instance resolves {wc} to a distinct concrete path); only a collision between
## DISTINCT task_ids is rejected.
validate_no_duplicate_output_paths <- function(task_list){
  seen <- list()   ## normalized pattern -> first task_id that declared it
  for(op in task_list){
    if(is.null(op$par_list)) next
    pats <- character(0)
    for(po in op$par_list){
      if("out_par_object" %in% class(po) && !is.null(po$value) &&
         (inherits(po$value, "file_path") || inherits(po$value, "file_object"))){
        pats <- c(pats, as.character(unlist(po$value)))
      }
    }
    pats <- unique(pats[nzchar(pats)])
    for(p in pats){
      key <- output_collision_key(p)
      if(!is.null(seen[[key]]) && !identical(seen[[key]]$task, op$task_id)){
        if(identical(seen[[key]]$path, normalize_path_lexical(p))){
          stop(flumen_reject(
            call  = "new_workflow()",
            tasks = sprintf("%s, %s", seen[[key]]$task, op$task_id),
            error = sprintf("both declare the file output '%s'", p),
            fix   = "give each task a distinct output path"), call. = FALSE)
        }
        stop(flumen_reject(
          call  = "new_workflow()",
          tasks = sprintf("%s, %s", seen[[key]]$task, op$task_id),
          error = sprintf("'%s' and '%s' differ only in case",
                          seen[[key]]$path, normalize_path_lexical(p)),
          fix   = "give them distinct names"), call. = FALSE)
      }
      if(is.null(seen[[key]]))
        seen[[key]] <- list(task = op$task_id, path = normalize_path_lexical(p))
    }
  }
  invisible(TRUE)
}

## Reject a fanned-out task whose file output carries no wildcard: every instance
## resolves to the SAME path, so the instances overwrite each other on disk and
## only the last writer survives. The run still reports success, and a downstream
## aggregation still receives N paths, all identical, so it reads the same file
## N times.
##
## What counts as fanning out: an input carrying the "expansion" class AND
## referencing a static wildcard. The class alone is not enough, since it is the
## wildcard's value set that multiplies the task, not the constructor.
##
## An output escapes the collision when it carries any wildcard: a static `{wc}`
## resolves to a different value per instance, and a dynamic `{{name}}` is bound
## at run time from the task's own outputs. Object outputs are excluded, being
## kept per instance in the worker cache rather than written to a shared path.
validate_no_colliding_expanded_outputs <- function(task_list){
  for(op in task_list){
    if(is.null(op$par_list)) next

    expands <- FALSE
    for(par in op$par_list){
      if("out_par_object" %in% class(par)) next
      if(is.null(par$value)) next
      val   <- par$value
      inner <- if(inherits(val, "dep_object")) val$value
               else if(inherits(val, "dep_list") && length(val) > 0L) val[[1L]]$value
               else val
      if(!("expansion" %in% class(inner))) next
      wc <- tryCatch(extract_wildcards_from_value(val), error = function(e) NULL)
      if(!is.null(wc) && length(wc$static) > 0L){ expands <- TRUE; break }
    }
    if(!expands) next

    for(par in op$par_list){
      if(!"out_par_object" %in% class(par)) next
      val <- par$value
      if(is.null(val)) next
      if(!(inherits(val, "file_path") || inherits(val, "file_object"))) next
      wc <- tryCatch(extract_wildcards_from_value(val), error = function(e) NULL)
      if(is.null(wc) || length(unlist(wc)) > 0L) next

      stop(flumen_reject(
        call  = "new_workflow()",
        task  = op$task_id,
        error = sprintf("the task fans out but its file output '%s' contains no wildcard",
                        as.character(unlist(val))[1]),
        fix   = "add the wildcard to the path, 'out_{sample}.txt'"),
        call. = FALSE)
    }
  }
  invisible(TRUE)
}

## The same concern as the two checks above, decided on the RESOLVED paths
## instead of on the pattern. Runs AFTER wildcard expansion, when every instance
## already carries the concrete path it will write (op$output_files), so it also
## catches what the pattern-level proxies miss: a task expanded over {s} and {r}
## whose output names only {s}, and two combinations that collide once
## substituted ({a}_{b} with a in {x, x_y}, b in {y_z, z} gives "x_y_z" twice).
##
## Duplicates WITHIN one instance are deliberately ignored (a pattern repeating a
## wildcard can legitimately emit the same path twice during expansion); only a
## collision BETWEEN instances is an overwrite. Dynamic outputs are skipped:
## their `{{name}}` is bound at run time.
##
## `context` and `remedy` exist because the same policy is applied at two
## moments: at build, over the instances a STATIC wildcard produced, and in the
## scheduler after a RUN TIME expansion, over instances whose number comes from a
## producer's result. The advice has to differ, because "add the missing
## wildcard" is not open to instances whose run time axis has no wildcard value.
validate_no_colliding_resolved_outputs <- function(task_list,
                                                   context = "new_workflow()",
                                                   remedy  = NULL){
  if(is.null(remedy))
    remedy <- paste0("make the path unique; add the missing wildcard, or change the ",
                     "separator when two wildcard values can join into the same name ",
                     "('{a}_{b}' with a='x', b='y_z' and a='x_y', b='z')")
  ## The build raises this on its own, so it gets the labelled block. The run time
  ## caller hands the text to record_error_notify(), where it is printed inside the
  ## `error` field of flumen_fail()'s block -- one sentence there, never a block
  ## nested in a block.
  say <- function(who1, who2, err, fix){
    if(identical(context, "new_workflow()"))
      flumen_reject(call = context, instances = sprintf("%s, %s", who1, who2),
                    error = err, fix = fix)
    else
      sprintf("%s: %s and %s %s. Fix: %s.", context, who1, who2, err, fix)
  }
  seen <- list()   ## resolved path -> "task_id (exe_id)" that claimed it first
  for(op in task_list){
    outs <- op$output_files
    if(is.null(outs)) next
    outs <- unique(as.character(outs))
    outs <- outs[nzchar(outs) & !grepl("{{", outs, fixed = TRUE)]
    if(!length(outs)) next
    who <- sprintf("%s (%s)", op$task_id, op$exe_id)
    for(p in outs){
      key <- output_collision_key(p)
      if(!is.null(seen[[key]]) && !identical(seen[[key]]$who, who)){
        if(identical(seen[[key]]$path, normalize_path_lexical(p))){
          ## Classed, and carrying the offending exe_id: the scheduler reports
          ## this through an envelope with its own "task" and "binding" fields,
          ## and without the id it fills them from the first task in the list --
          ## naming the producer for a collision between two consumers, and the
          ## innocent binding when only one of two collided. The build path
          ## ignores the extra fields and prints the plain sentence.
          stop(structure(class = c("flumen_output_collision", "error", "condition"),
                         list(call = NULL,
                              exe_id  = op$exe_id,
                              task_id = op$task_id,
                              message = say(
            seen[[key]]$who, who,
            sprintf("both resolve to the file output '%s'", p),
            remedy))))
        }
        stop(say(seen[[key]]$who, who,
                 sprintf("resolve to '%s' and '%s', which differ only in case",
                         seen[[key]]$path, normalize_path_lexical(p)),
                 "make the wildcard values differ by more than case"),
             call. = FALSE)
      }
      if(is.null(seen[[key]]))
        seen[[key]] <- list(who = who, path = normalize_path_lexical(p))
    }
  }
  invisible(TRUE)
}

## A fanned-out task whose DYNAMIC file output carries no static wildcard writes
## into a namespace it shares with its own siblings. The wrapper decides which
## files an instance produced by globbing the output pattern in its directory
## before and after the body runs, so with the pattern unresolved each instance
## claims every file that appeared while it ran.
##
## TWO conditions:
##   1. the producer really fans out (the wildcard has more than one value);
##   2. its dynamic FILE output carries no static wildcard. An OBJECT output is
##      exempt, never touching a shared directory, and a static wildcard
##      anywhere in the path is enough, because the glob is matched against the
##      resolved pattern.
##
## Who CONSUMES the output is deliberately not a condition: the ambiguity is the
## producer's.
wildcards_expanding_task <- function(op){
  out <- character(0)
  if(is.null(op$par_list)) return(out)
  for(par in op$par_list){
    if("out_par_object" %in% class(par)) next
    val <- par$value
    if(is.null(val)) next
    via_dep <- inherits(val, "dep_object") || inherits(val, "dep_list")
    inner <- if(inherits(val, "dep_object")) val$value
             else if(inherits(val, "dep_list") && length(val) > 0L) val[[1L]]$value
             else val
    ## An _exp constructor is not the only thing that fans a task out. A dep whose
    ## value is a PLAIN new_file()/new_object() carrying a static wildcard expands
    ## the consumer just as much, because it correlates with the producer's
    ## instances, and it suffers the same shared-namespace defect. An _aggr inside
    ## a dep is the opposite: it collapses the consumer to ONE instance, so it
    ## must not count.
    if(!("expansion" %in% class(inner)) &&
       !(via_dep && !inherits(inner, "aggregation"))) next
    wc <- tryCatch(extract_wildcards_from_value(val), error = function(e) NULL)
    if(!is.null(wc)) out <- unique(c(out, wc$static))
  }
  out
}

## The task's DYNAMIC file output patterns, as they stand on this object.
##
## Two callers ask two different questions of the same list, because a pattern
## means different things before and after expansion:
##   - at build time (validate_no_shared_dynamic_namespace, below) the static
##     wildcards are still unresolved, so "carries no static {wc}" marks an
##     output that cannot separate one instance from another;
##   - at the scheduler (R/scheduler_handlers.R) the instances are already
##     resolved, '{sample}' having BECOME 't1', so the question there is whether
##     two siblings ended up with the SAME pattern.
##
## Object outputs are excluded on purpose: one lives in the worker's result cache
## under its exe_id and never touches a shared directory.
dynamic_file_output_patterns <- function(op){
  out <- character(0)
  if(is.null(op$par_list)) return(out)
  for(par in op$par_list){
    if(!("out_par_object" %in% class(par))) next
    val <- par$value
    if(is.null(val)) next
    if(!(inherits(val, "file_path") || inherits(val, "file_object"))) next
    for(p in as.character(unlist(val))){
      if(!nzchar(p)) next
      if(grepl(DYN_WILDCARD_PATTERN, basename(p), perl = TRUE) ||
         grepl("[{][{][^}]+[}][}]", p)) out <- c(out, p)
    }
  }
  unique(out)
}

validate_no_shared_dynamic_namespace <- function(task_list, wildcard_list = NULL){
  if(is.null(wildcard_list)) return(invisible(TRUE))
  multi <- names(wildcard_list)[vapply(wildcard_list, function(v) length(unlist(v)) > 1L, logical(1))]
  if(!length(multi)) return(invisible(TRUE))

  for(op in task_list){
    exp_wc <- intersect(wildcards_expanding_task(op), multi)
    if(!length(exp_wc)) next

    ## Who consumes it does not matter: the ambiguity is in the PRODUCER, whose
    ## instances cannot tell their files apart, so a plain gather receives the
    ## same file several times -- claimed by every instance running when it appeared.
    for(par in op$par_list){
      if(!"out_par_object" %in% class(par)) next
      val <- par$value
      if(is.null(val)) next
      if(!(inherits(val, "file_path") || inherits(val, "file_object"))) next
      wc <- tryCatch(extract_wildcards_from_value(val), error = function(e) NULL)
      if(is.null(wc) || length(wc$dynamic) == 0L || length(wc$static) > 0L) next

      stop(flumen_reject(
        call  = "new_workflow()",
        task  = op$task_id,
        error = sprintf("runs once per '{%s}' but dynamic output '%s' has no static wildcard",
                        exp_wc[1], as.character(unlist(val))[1]),
        fix   = sprintf("put the wildcard in the path, '%s/{%s}/{{name}}.ext'",
                        dirname(as.character(unlist(val))[1]), exp_wc[1])), call. = FALSE)
    }
  }
  invisible(TRUE)
}

## Two tasks that may run AT THE SAME TIME must not declare file outputs whose
## names can overlap in one directory.
##
## A dynamic output is collected by globbing its pattern in its directory after
## the body ran, minus a snapshot taken before. Files written meanwhile by
## ANOTHER task land in that window too, and if they also match the glob the
## first task claims them.
##
## Ordering is what makes it safe, and it is respected here: if the DAG puts one
## task after the other, the later one starts with the earlier one's files
## already in its snapshot and correctly ignores them. Only pairs the scheduler
## is free to run together are rejected.

## A glob as an anchored regex, where "*" is the ONLY metacharacter: every other
## character is literal path text and is escaped. Not glob2rx(), which leaves
## "+ | ^ $" live and so both over-matches and yields unsatisfiable regexes.
glob_pattern_regex <- function(g)
  paste0("^", gsub("\001", ".*",
                   p2p_escape_literal(gsub("*", "\001", g, fixed = TRUE)),
                   fixed = TRUE), "$")

patterns_overlap <- function(g1, g2){
  ## Witness substitution: two globs overlap if either accepts a name generated
  ## from the other. Conservative -- it can miss exotic multi-star cases, which
  ## keeps it from rejecting anything it cannot prove.
  w1 <- gsub("*", "flumenWITNESS", g1, fixed = TRUE)
  w2 <- gsub("*", "flumenWITNESS", g2, fixed = TRUE)
  grepl(glob_pattern_regex(g1), w2) || grepl(glob_pattern_regex(g2), w1)
}

output_globs <- function(op){
  out <- list()
  if(is.null(op$par_list)) return(out)
  for(par in op$par_list){
    if(!"out_par_object" %in% class(par)) next
    val <- par$value
    if(is.null(val)) next
    if(!(inherits(val, "file_path") || inherits(val, "file_object"))) next
    for(p in as.character(unlist(val))){
      if(!nzchar(p)) next
      clean <- strip_backticks(p)
      out[[length(out)+1]] <- list(
        pat = p,
        dir = dirname(clean),
        dyn = grepl("{{", p, fixed = TRUE),
        ## A dynamic output with NO extension after the wildcard (`dir/`{{any}})
        ## is the "whole bundle" form: the wrapper globs its directory
        ## RECURSIVELY. It therefore claims the whole sub-tree, not just one
        ## level -- which is why the directory comparison below cannot be a
        ## plain equality.
        rec = is_recursive_wildcard(clean),
        glob = gsub("[{][{][^}]+[}][}]", "*", basename(clean)))
    }
  }
  out
}

validate_no_overlapping_output_globs <- function(task_list, DAG = NULL){
  ordered <- function(a, b){
    if(is.null(DAG)) return(FALSE)
    nm <- igraph::V(DAG)$name
    if(!(a %in% nm) || !(b %in% nm)) return(FALSE)
    is.finite(igraph::distances(DAG, v = a, to = b, mode = "out")[1, 1]) ||
      is.finite(igraph::distances(DAG, v = b, to = a, mode = "out")[1, 1])
  }
  n <- length(task_list)
  if(n < 2L) return(invisible(TRUE))
  globs <- lapply(task_list, output_globs)

  ## Enumerated by GROUP of task_id, not over every pair of instances: the body
  ## below refuses to look at two instances of the SAME task, so a pairwise scan
  ## builds and discards almost every pair it makes. The pairs removed are exactly
  ## the ones such a scan would skip. split() orders group names alphabetically,
  ## so a collision can name its two tasks in the other order.
  ids    <- vapply(task_list, function(op) as.character(op$task_id)[1], character(1))
  keepix <- which(lengths(globs) > 0L)
  if(length(keepix) < 2L) return(invisible(TRUE))
  groups <- split(keepix, ids[keepix])
  gn     <- names(groups)
  if(length(gn) < 2L) return(invisible(TRUE))

  for(gi in seq_len(length(gn) - 1L)) for(gj in seq(gi + 1L, length(gn))){
    ti <- gn[gi]; tj <- gn[gj]
    if(ordered(ti, tj)) next
    for(i in groups[[gi]]) for(j in groups[[gj]]){
    for(a in globs[[i]]) for(b in globs[[j]]){
      ## Same directory is the plain case. But a BUNDLE output (dynamic
      ## wildcard with no extension, e.g. `out/`{{any}}) is collected
      ## recursively by the wrappers, so it claims the whole sub-tree: a task
      ## writing into out/sub/ has its files swept up by it, and the equality
      ## test below never fires because "out" is not "out/sub".
      ## The trailing "/" keeps a mere name prefix apart: "output" is NOT
      ## inside "out".
      under <- function(child, parent)
        nzchar(parent) && nzchar(child) &&
        startsWith(child, paste0(parent, "/"))
      a_covers <- isTRUE(a$rec) && under(b$dir, a$dir)
      b_covers <- isTRUE(b$rec) && under(a$dir, b$dir)
      if(!identical(a$dir, b$dir) && !a_covers && !b_covers) next
      if(!a$dyn && !b$dyn) next          ## two static paths: covered elsewhere
      ## A bundle's glob is "*", which patterns_overlap() already reports as
      ## overlapping with anything, so the nested case needs no special case here.
      if(!patterns_overlap(a$glob, b$glob)) next
      if(a_covers || b_covers){
        deep <- if(a_covers) list(who = ti, pat = a$pat, dir = a$dir,
                                  other = tj, opat = b$pat, odir = b$dir)
                else         list(who = tj, pat = b$pat, dir = b$dir,
                                  other = ti, opat = a$pat, odir = a$dir)
        stop(flumen_reject(
          call  = "new_workflow()",
          tasks = sprintf("%s, %s", deep$who, deep$other),
          error = sprintf("'%s' collects '%s' recursively, claiming what '%s' writes into '%s'",
                          deep$who, deep$pat, deep$other, deep$odir),
          fix   = paste("add an extension after the wildcard, use separate folders,",
                        "or order the tasks")), call. = FALSE)
      }
      stop(flumen_reject(
        call  = "new_workflow()",
        tasks = sprintf("%s, %s", ti, tj),
        error = sprintf("both write into '%s' with overlapping names ('%s' and '%s')",
                        a$dir, a$pat, b$pat),
        fix   = sprintf("give the patterns a distinct prefix, '%s/left_{{name}}.ext'",
                        a$dir)), call. = FALSE)
    }
  }
    }
  invisible(TRUE)
}

## Reject bare wildcards: a parameter value containing `{x}` (static, single
## brace) or `{{x}}` (dynamic, double brace) that is NOT wrapped in a flumen
## constructor (`new_object()` / `new_file()` / `new_*_exp()` / `new_*_aggr()`).
##
##   - Bare `{{x}}` (dynamic): nothing wraps it, so it carries no class marker
##     and no resolution path, and the literal string "{{x}}" reaches the user
##     function unchanged. The workflow runs to completion with wrong semantics.
##
##   - Bare `{x}` (static): a non-expansion driver, so it resolves as vector
##     binding, one task instance with the parameter bound to the vector of
##     wildcard values. Mixed with an `_exp` driver on the same axis it is also
##     order-dependent (see `validate_no_mixed_expansion_drivers`).
##
## STATIC_WILDCARD_PATTERN matches the single brace, its lookbehind and lookahead
## keeping the inner `{x}` of a `{{x}}` out.
validate_no_bare_wildcard <- function(task_list) {

  check_value <- function(val, op_id, par_name, is_output) {
    ## Wrapped values are fine: the constructor has attached a class marker.
    if (inherits(val, "flumen_object") || inherits(val, "file_object")) return(invisible())
    if (!is.character(val)) return(invisible())

    s <- paste(as.character(val), collapse = "\n")
    dyn_m    <- regmatches(s, regexpr(DYN_WILDCARD_PATTERN,    s, perl = TRUE))
    static_m <- regmatches(s, regexpr(STATIC_WILDCARD_PATTERN, s, perl = TRUE))
    dyn_m    <- dyn_m[nzchar(dyn_m)]
    static_m <- static_m[nzchar(static_m)]

    has_bare_dyn    <- length(dyn_m)    > 0L
    has_bare_static <- length(static_m) > 0L
    if (!has_bare_dyn && !has_bare_static) return(invisible())

    param_role <- if (isTRUE(is_output)) "output" else "input"

    if (has_bare_dyn) {
      offending <- dyn_m[1]
      stop(flumen_reject(
        call      = "new_workflow()",
        task      = op_id,
        parameter = sprintf("%s '%s'", param_role, par_name),
        error     = sprintf(paste("the bare dynamic wildcard '%s' is passed as a literal",
                                  "and never substituted"), offending),
        fix       = sprintf("wrap it, new_object(\"%s\") or new_file(\"path/%s.ext\")",
                            offending, offending)), call. = FALSE)
    } else {
      offending <- static_m[1]
      stop(flumen_reject(
        call      = "new_workflow()",
        task      = op_id,
        parameter = sprintf("%s '%s'", param_role, par_name),
        error     = sprintf("the bare static wildcard '%s' defaults to vector binding",
                            offending),
        fix       = sprintf("new_object_exp(\"%s\") per value, new_object(\"%s\") for a vector",
                            offending, offending)), call. = FALSE)
    }
  }

  for (op in task_list) {
    if (is.null(op$par_list)) next
    for (par in op$par_list) {
      if (is.null(par$value)) next
      val <- par$value
      is_output <- "out_par_object" %in% class(par)

      if (inherits(val, "dep_object")) {
        check_value(val$value, op$task_id, par$name, is_output)
      } else if (inherits(val, "dep_list")) {
        for (d in val) {
          if (!is.null(d$value)) {
            check_value(d$value, op$task_id, par$name, is_output)
          }
        }
      } else {
        check_value(val, op$task_id, par$name, is_output)
      }
    }
  }

  invisible(TRUE)
}

## Reject mixed expansion / non-expansion drivers on the same static wildcard
## axis within a task's INPUT parameters.
##
## The engine decides fan-out with an OR over every parameter naming the
## wildcard (the EXPANSION SETUP block in R/wildcard_engine.R), so a single
## `_exp` makes the axis expand and the non-`_exp` parameter on the same axis is
## overruled.
##
## Excluded from the check:
##   - OUTPUT parameters: an output legitimately references a static wildcard
##     expanded by inputs (e.g. `new_file("path/{sample}/out.txt")` while a
##     sibling input has `new_object_exp("{sample}")`). Output expansion is a
##     consequence of input expansion, not an independent driver.
##   - AGGREGATION drivers (`new_*_aggr`, direct or wrapped in new_dep):
##     aggregation is a gather mechanism, not fan-out, and mixing it with
##     non-expansion is a different concern.
validate_no_mixed_expansion_drivers <- function(task_list) {
  for (op in task_list) {
    if (is.null(op$par_list)) next

    ## Map: static wildcard name -> list of (param_name, kind = "expansion" | "static")
    wc_drivers <- list()

    for (par in op$par_list) {
      if ("out_par_object" %in% class(par)) next
      if (is.null(par$value)) next
      val <- par$value

      ## Inspect the inner value for dep_object / dep_list wrapping; the
      ## semantic class (expansion / aggregation) lives there.
      check_val <- if (inherits(val, "dep_object")) val$value
                   else if (inherits(val, "dep_list") && length(val) > 0L) val[[1L]]$value
                   else val

      is_exp <- "expansion"   %in% class(check_val)
      is_agg <- "aggregation" %in% class(check_val)

      if (is_agg) next

      driver_kind <- if (is_exp) "expansion" else "static"

      ## Recurses into a dep to reach the static wildcards it names.
      wcs <- extract_wildcards_from_value(val)$static

      for (wc in wcs) {
        wc_drivers[[wc]] <- c(wc_drivers[[wc]],
                              list(list(param_name = par$name,
                                        kind = driver_kind)))
      }
    }

    for (wc in names(wc_drivers)) {
      entries <- wc_drivers[[wc]]
      kinds   <- vapply(entries, function(e) e$kind, character(1))

      if (any(kinds == "expansion") && any(kinds == "static")) {
        exp_params     <- vapply(entries[kinds == "expansion"],
                                 function(e) e$param_name, character(1))
        non_exp_params <- vapply(entries[kinds == "static"],
                                 function(e) e$param_name, character(1))
        stop(flumen_reject(
          call  = "new_workflow()",
          task  = op$task_id,
          error = sprintf("conflicting expansion on wildcard '{%s}': %s use _exp, %s do not",
                          wc,
                          paste(shQuote(exp_params),     collapse = ", "),
                          paste(shQuote(non_exp_params), collapse = ", ")),
          fix   = "use the _exp forms on every parameter, or none"), call. = FALSE)
      }
    }
  }

  invisible(TRUE)
}

## A partial aggregation `by=` (new_object_aggr / new_file_aggr) must name a
## wildcard DECLARED in new_wildcard_list(): the grouping mechanism expands
## over names(wildcard_list[[wd]]) (object_func.R), so an undeclared name has
## no values. Unchecked, an unknown or mistyped `by=` is dropped by intersect()
## and the consumer collapses into a FULL aggregation with no error.
## Only partial_aggregation params carry attr("aggr_by") (by = NULL -> plain
## aggregation, skipped).
validate_aggr_by_wildcards <- function(task_list, wildcard_list = NULL) {
  declared <- names(wildcard_list)
  for (op in task_list) {
    if (is.null(op$par_list)) next
    for (par in op$par_list) {
      if (is.null(par$value)) next
      val <- par$value
      inner_vals <- if (inherits(val, "dep_object")) list(val$value)
                    else if (inherits(val, "dep_list")) lapply(val, function(d) d$value)
                    else list(val)
      for (cv in inner_vals) {
        if (!("partial_aggregation" %in% class(cv))) next
        by_wcs <- attr(cv, "aggr_by")
        if (is.null(by_wcs)) next
        unknown <- setdiff(as.character(by_wcs), declared)
        if (length(unknown) > 0L) {
          stop(flumen_reject(
            call      = "new_workflow()",
            task      = op$task_id,
            parameter = sprintf("'%s'", par$name),
            error     = sprintf("by = %s is not a declared wildcard, known: %s",
                                paste(shQuote(unknown), collapse = ", "),
                                if (length(declared)) paste(declared, collapse = ", ")
                                else "(none declared)"),
            fix       = sprintf("use one of the declared wildcards, or add %s to new_wildcard_list()",
                                paste(shQuote(unknown), collapse = ", "))), call. = FALSE)
        }
      }
    }
  }

  invisible(TRUE)
}

## A LITERAL parameter bound for a Python task must be of a type Arrow can
## carry. The scheduler encodes such a value in the consumer's codec at dispatch
## (R/scheduler_dispatch.R); a type Arrow cannot represent has nowhere to go, so
## unchecked it ships as qs2 and fails deep inside a supervisor, naming the
## CONSUMING task. Checked here, the refusal names the parameter and the type.
##
## Only LITERALS are examined:
##   - a dep_object / dep_list is produced at runtime, and its type cannot be
##     known here (it reads as "list", which would reject every dependency);
##   - a file travels as a path, never through Arrow;
##   - an output parameter is a declaration, not a payload.

## An arrow object written straight into new_input(). Checked for EVERY consumer,
## unlike the Python-only check below: it is an R6 handle over a C++ pointer, and
## a qs2 round trip nulls that pointer, so the object keeps its classes and every
## method then fails with "external pointer to null".
##
## A TABULAR one is fine and is not touched here: it is encoded to Arrow bytes by
## the scheduler before any of that happens.
validate_arrow_object_literals <- function(task_list) {
  for (op in task_list) {
    if (is.null(op$par_list)) next
    for (par in op$par_list) {
      if ("out_par_object" %in% class(par)) next
      val <- par$value
      if (is.null(val)) next
      if (!inherits(val, "ArrowObject") || inherits(val, "ArrowTabular")) next
      .m <- arrow_non_tabular_message(val)
      stop(flumen_reject(
        call      = "new_workflow()",
        task      = op$task_id,
        parameter = sprintf("'%s'", par$name),
        error     = .m$error,
        fix       = .m$fix), call. = FALSE)
    }
  }
  invisible(NULL)
}

validate_python_literal_arrow_types <- function(task_list) {
  for (op in task_list) {
    if (!identical(get_type_from_object(op), "python")) next
    if (is.null(op$par_list)) next
    for (par in op$par_list) {
      if ("out_par_object" %in% class(par)) next
      val <- par$value
      if (is.null(val)) next
      if (inherits(val, "dep_object") || inherits(val, "dep_list")) next
      if ("file_object" %in% class(val)) next
      ty <- tryCatch(get_R_type(val), error = function(e) NULL)
      if (is.null(ty) || isTRUE(is_arrow_compatible_type(ty))) next
      .m <- arrow_unsupported_message(ty)
      stop(flumen_reject(
        call      = "new_workflow()",
        task      = op$task_id,
        parameter = sprintf("'%s'", par$name),
        error     = .m$error,
        fix       = .m$fix), call. = FALSE)
    }
  }
  invisible(TRUE)
}

## Build-time existence check for transferred input files (the default: a static
## file input NOT wrapped with pre_staged()). If the literal path does not exist
## on the master, the dispatch fails when it reads the bytes. Wildcard paths are
## resolved later and the master never reads a pre-staged file, so both are
## skipped. Warns per missing file and does NOT stop the construction.
validate_input_file_existence <- function(task_list) {
  for (op in task_list) {
    if (is.null(op$par_list)) next
    for (par in op$par_list) {
      if ("out_par_object" %in% class(par)) next
      val <- par$value
      if (is.null(val)) next
      if (any(c("dep_object", "dep_list") %in% class(val))) next
      if (!any(c("file_object", "file_path") %in% class(val))) next
      to_transfer <- attr(val, "to_transfer")
      if (!is.null(to_transfer) && !isTRUE(to_transfer)) next
      val_chr <- as.character(val)
      if (any(grepl("\\{", val_chr))) next       # wildcard, resolved later
      for (p in val_chr) {
        if (!nzchar(p)) next
        if (!file.exists(p)) {
          warning(flumen_warn(
            call    = "new_workflow()",
            task    = op$task_id,
            warning = sprintf(paste("input '%s' is set to be transferred but does not",
                                    "exist on the master"), p),
            fix     = "create it on the master, or wrap it with pre_staged()"), call. = FALSE)
        }
      }
    }
  }
}

## Reject "bare folder" output declarations.
## The only way to declare a directory as output is a backtick prefix plus a
## dynamic wildcard:
##   new_file("`dir/`{{wildcard}}")
## A user writing new_file("output_dir") creates an output with the literal
## path "output_dir" interpreted as a FILE, which produces wrong results
## (e.g. shell 'mv X output_dir' renames X instead of moving it into the
## directory).
validate_output_folder_paths <- function(task_list) {
  for (op in task_list) {
    if (is.null(op$par_list)) next
    for (par in op$par_list) {
      if (!("out_par_object" %in% class(par))) next
      val <- par$value
      if (is.null(val)) next
      if (!any(c("file_object", "file_path") %in% class(val))) next
      val_chr <- as.character(val)
      for (p in val_chr) {
        if (is_ambiguous_folder_path(p)) {
          clean <- sub("/+$", "", strip_backticks(p))
          stop(flumen_reject(
            call  = "new_workflow()",
            task  = op$task_id,
            error = sprintf("ambiguous output path '%s', which looks like a folder", p),
            fix   = sprintf(paste("whole folder, new_file('`%s/`{{any}}'), or one file,",
                                  "new_file('%s/output.ext')"),
                            clean, clean)), call. = FALSE)
        }
      }
    }
  }
}

## Reject unbalanced backticks in any parameter value.
##
## flumen uses paired backticks to delimit a static prefix in path patterns
## (e.g. new_file("`prefix/`{{any}}.ext")), and the parser regex only matches
## when they are paired, so an UNCLOSED backtick falls through as a literal
## character. In a shell task that substitutes the value into the command, a
## stray backtick is read by bash as command substitution.
validate_balanced_backticks <- function(task_list) {
  for(op in task_list){
    if(is.null(op$par_list)) next
    for(par in op$par_list){
      if(is.null(par$value)) next

      check_val <- if(inherits(par$value, "dep_object")) par$value$value
                   else if(inherits(par$value, "dep_list") && length(par$value) > 0L) par$value[[1L]]$value
                   else par$value
      if(!is.character(check_val)) next

      for(elem in check_val){
        if(!nzchar(elem)) next
        n_bt <- nchar(elem) - nchar(strip_backticks(elem))
        if(n_bt %% 2L != 0L){
          param_role <- if(inherits(par, "out_par_object")) "output" else "input"
          stop(flumen_reject(
            call      = "new_workflow()",
            task      = op$task_id,
            parameter = sprintf("%s '%s'", param_role, par$name),
            error     = sprintf("unbalanced backtick (`) in value '%s'", elem),
            fix       = "backticks delimit a static prefix, new_file(\"`prefix/`{{any}}.ext\")"), call. = FALSE)
        }
      }
    }
  }
}

## Reject new_object_aggr() / new_file_aggr() used as a value OUTSIDE
## new_dep(value=...) when it has NO static wildcard to resolve. There are two
## legitimate ways to use _aggr standalone:
##
##   - Static-wildcard fan-in on pre-existing files, no producer needed:
##     new_file_aggr("data_{sample}.tsv"), where flumen resolves {sample} from
##     wildcard_list to a vector of paths and passes the list into a single
##     task instance.
##   - As a consumer marker inside new_dep(value=...), gathering the
##     per-instance outputs of the upstream producer.
##
## What this REJECTS: a standalone _aggr whose value holds only a DYNAMIC
## wildcard ({{x}}), or no wildcard at all: there is no upstream producer
## instance to gather from.
##
## Outputs are handled by the sibling check inside validate_workflow(), the
## "_exp / _aggr forbidden in output parameters" block.
validate_no_aggr_outside_dep <- function(task_list) {
  for(op in task_list){
    if(is.null(op$par_list)) next
    for(par in op$par_list){
      if(is.null(par$value)) next
      if(inherits(par, "out_par_object")) next                ## outputs covered elsewhere
      if(inherits(par$value, "dep_object")) next              ## inside new_dep -> legit
      if(inherits(par$value, "dep_list")) next                ## inside new_dep -> legit
      if(!inherits(par$value, "aggregation")) next

      ## Legit standalone usage: static wildcard present -> flumen resolves
      ## it from wildcard_list and aggregates the resulting paths/values.
      wcs <- extract_wildcards_from_value(par$value)
      if(length(wcs$static) > 0L) next

      stop(flumen_reject(
        call      = "new_workflow()",
        task      = op$task_id,
        parameter = sprintf("input '%s'", par$name),
        error     = "new_object_aggr() / new_file_aggr() has no static wildcard to aggregate by",
        fix       = "new_file_aggr(\"path_{wc}.ext\") or new_object_aggr(\"{{name}}\")"), call. = FALSE)
    }
  }
}

## Reject empty-string values inside wrapped flumen constructors (new_file /
## new_object family). Bare character "" stays allowed (a legit empty CLI
## argument), but new_file("") has no resolution path -- the wrapper looks
## for a file named "".
##
## Rejected when ANY element of a vector value is empty, new_file(c("", "valid"))
## included: the intention there is to filter the empty element out.
validate_no_empty_constructor_values <- function(task_list) {
  for(op in task_list){
    if(is.null(op$par_list)) next
    for(par in op$par_list){
      if(is.null(par$value)) next

      check_val <- if(inherits(par$value, "dep_object")) par$value$value
                   else if(inherits(par$value, "dep_list") && length(par$value) > 0L) par$value[[1L]]$value
                   else par$value

      ## Only target wrapped constructors -- bare character "" is allowed
      ## (legit empty CLI arg passed verbatim to the user function).
      if(!inherits(check_val, "flumen_object") && !inherits(check_val, "file_object")) next
      if(!is.character(check_val)) next

      if(any(!nzchar(check_val))){
        param_role <- if(inherits(par, "out_par_object")) "output" else "input"
        ctor <- if(inherits(check_val, "file_object")) "new_file()" else "new_object()"
        stop(flumen_reject(
          call      = "new_workflow()",
          task      = op$task_id,
          parameter = sprintf("%s '%s'", param_role, par$name),
          error     = sprintf("%s has an empty-string value", ctor),
          fix       = paste("give it a path, new_file(\"path/to/file\"), or a name,",
                            "new_object(\"{{name}}\")")), call. = FALSE)
      }
    }
  }
}

## Reject backtick pairs whose content, after stripping wildcards, is empty.
## Backticks in flumen path patterns delimit a STATIC PREFIX (e.g.
## new_file("`prefix/`{{any}}.ext")) and only make sense when the prefix
## contains literal text. Pure-wildcard prefixes are silent confusion:
##
##   - `{{x}}`literal.txt -> the wrapper treats the dynamic wildcard as the
##     static prefix, so it has no resolution path and the literal string
##     "{{x}}" travels downstream.
##   - `{sample}`.txt -> equivalent to new_object_exp("{sample}") after static
##     substitution.
##   - ``{{x}}.txt -> an empty backtick pair.
##
## A legitimate pattern (`prefix/`, `_{x}_`, `{x}_suffix`, ...) leaves at least
## one literal character behind once the wildcards are stripped.
validate_backtick_has_static_content <- function(task_list) {
  bt_pair_pat <- "`([^`]*)`"
  for(op in task_list){
    if(is.null(op$par_list)) next
    for(par in op$par_list){
      if(is.null(par$value)) next

      check_val <- if(inherits(par$value, "dep_object")) par$value$value
                   else if(inherits(par$value, "dep_list") && length(par$value) > 0L) par$value[[1L]]$value
                   else par$value
      if(!is.character(check_val)) next

      for(elem in check_val){
        if(!nzchar(elem)) next
        if(!grepl("`", elem, fixed = TRUE)) next  ## fast-path: no backticks at all

        bt_matches <- regmatches(elem, gregexpr(bt_pair_pat, elem, perl = TRUE))[[1]]
        for(bt in bt_matches){
          inner <- sub("^`(.*)`$", "\\1", bt)
          ## Dynamic first: `\\{[^{}]+\\}` would otherwise partial-match the
          ## inner braces of a {{x}}.
          stripped <- gsub(DYN_WILDCARD_PATTERN, "", inner)
          stripped <- gsub("\\{[^{}]+\\}",        "", stripped)
          if(!nzchar(stripped)){
            param_role <- if(inherits(par, "out_par_object")) "output" else "input"
            stop(flumen_reject(
              call      = "new_workflow()",
              task      = op$task_id,
              parameter = sprintf("%s '%s'", param_role, par$name),
              error     = sprintf("backtick pair '%s' enclosing only wildcards in value '%s'",
                                  bt, elem),
              fix       = paste("backticks must contain literal text,",
                                "new_file(\"`prefix/`{{any}}.ext\")")), call. = FALSE)
          }
        }
      }
    }
  }
}

## Reject backticks (`) in any parameter value of non-shell tasks
## (new_R_task / new_python_task). Backticks are a SHELL-ONLY DSL construct:
## they tell the wrapper to substitute the static prefix, the content between the
## pair, into `{name}` placeholders in shell script bodies. R and Python tasks
## receive the resolved file LIST either way, so there the marker carries no
## meaning at all.
##
## The check covers input, output and dep values uniformly. Backticks in an
## R/Python output are no-ops too, since strip_backticks() runs before the
## post-execution scan in R/fs.R.
validate_no_backticks_in_non_shell_task <- function(task_list) {
  for(op in task_list){
    if(is.null(op$type) || op$type == "shell") next       ## shell -> backticks allowed
    if(is.null(op$par_list)) next

    for(par in op$par_list){
      if(is.null(par$value)) next

      check_val <- if(inherits(par$value, "dep_object")) par$value$value
                   else if(inherits(par$value, "dep_list") && length(par$value) > 0L) par$value[[1L]]$value
                   else par$value
      if(!is.character(check_val)) next

      offending <- check_val[grepl("`", check_val, fixed = TRUE)]
      if(length(offending) > 0L){
        param_role <- if(inherits(par, "out_par_object")) "output" else "input"
        stop(flumen_reject(
          call      = "new_workflow()",
          task      = sprintf("%s (%s)", op$task_id, op$type),
          parameter = sprintf("%s '%s'", param_role, par$name),
          error     = sprintf("backticks in value '%s' are shell-only", offending[1]),
          fix       = "use new_file_*(\"path/{{any}}.ext\")"), call. = FALSE)
      }
    }
  }
}
