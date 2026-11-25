## Serialization layer: R-object <-> Arrow IPC <-> qs2 <-> raw bytes.
##
## get_R_type / is_arrow_compatible_type drive the dispatch decision in
## serialize_data, which packs a list of values to either Arrow IPC bytes
## (zero-copy across processes/languages) or qs2-serialized raw vectors.
## from_R_to_arrow / from_arrow_binary_to_R handle the actual Arrow round-trip.
## raw_payload, pack_int32_le, read_int64_le are byte-level helpers used by
## the messaging layer to assemble multi-frame envelopes.


get_R_type <- function(object) {
  if (inherits(object, "ArrowTabular")) {
    ## An arrow Table / RecordBatch is already in the transport's own format,
    ## and its R form is a data.frame -- the same type a Python pa.Table return
    ## uses, so calling it that lets every receiver decode it with no change.
    ## Without this branch the bare typeof() fallback below answers
    ## "environment", and a value Arrow carries natively is routed as one Arrow
    ## cannot carry.
    return("data.frame")
  }else if (inherits(object, "data.frame")) {
    return("data.frame")
  }else if(inherits(object, "matrix")) {
    return("matrix")
  }else if(inherits(object, "list")) {
    return("list")
  }else if(inherits(object, c("factor", "Date", "POSIXct"))) {
    ## Natively Arrow-encodable (factor->dictionary, Date->date32,
    ## POSIXct->timestamp) but not recognised by the bare-typeof fallback below,
    ## which would classify them as "integer"/"double" -> not arrow-compatible ->
    ## serialized via qs2 -> unreachable by a Python task (no qs2 in Python) and a
    ## dispatch crash on the direct path. Routing them through the vector path lets
    ## Arrow encode them natively; R consumers round-trip them losslessly.
    return("vector")
  }else if(is.raw(object)) {
    ## Arrow's vector encoder has no binary type: raw bytes pushed through it are
    ## re-read as an Arrow buffer on the consumer ("Expected to read N metadata
    ## bytes"). is.vector() is TRUE for a raw vector, so without this branch it
    ## falls into the generic "vector" case below and the payload is corrupted.
    ## Keep it out of the arrow-compatible set so it round-trips via qs2 -- the
    ## same "raw" data_type the shell stdout path already emits.
    return("raw")
  }else if(is.complex(object)) {
    ## Arrow has no complex type. Keep it out of the arrow-compatible set so R->R
    ## round-trips via qs2; from_R_to_arrow raises a clear typed error only when a
    ## Python task forces the arrow path.
    return("complex")
  }else if(is.atomic(object) && !is.null(dim(object))) {
    ## An atomic object with dimensions that is not a 2-D matrix (caught above):
    ## an array of any rank, a table, an xtabs. Arrow carries one column per
    ## vector, and the vector path below keeps only the first column of what
    ## as.data.frame() spreads them over -- 2 values of an array of 24, a table's
    ## labels without its counts, with exit 0. Not Arrow-compatible, so qs2:
    ## an R consumer receives it identical, a Python one is refused by name.
    ## Tested on dim, not on the class, so any class with dimensions follows.
    cls <- setdiff(class(object),
                   c("partial_aggregation", "aggregation", "expansion", "flumen_object"))[1]
    return(if(is.na(cls)) "array" else cls)
  }else if(is.vector(object) | inherits(object, "vector") |
           is.character(object) | is.logical(object) |
           is.numeric(object) | is.integer(object)) {
    return("vector")
  }else{
    return(typeof(object))
  }
}

is_arrow_compatible_type <- function(type){
  ifelse(type %in% c("vector", "matrix", "data.frame"), TRUE, FALSE)
}

## Types that legitimately travel to ANY consumer as non-Arrow bytes, so a
## Python wrapper receiving them is not a defect:
##   - file / file_path / file_object: paths, materialised to native strings
##     before dispatch (supervisor_deps.R) and Arrow-encoded from there;
##   - raw: an already-encoded payload, passed through by contract -- the shell
##     stdout path emits it and objects.py::deserialize_param returns the bytes.
## Kept next to is_arrow_compatible_type so the two questions a type can be
## asked -- "can Arrow encode it?" and "may it skip Arrow?" -- have one home.
is_passthrough_type <- function(type){
  base <- sub("^wrapped:?", "", as.character(type))
  base %in% c("file", "file_path", "file_object", "raw", "")
}

## The single wording for "Arrow cannot carry this". Shared by the encoder,
## which hits it while converting, and by the dispatch guard
## (supervisor_lib.R), which hits it when qs2 bytes are about to be handed to a
## Python wrapper that can only read Arrow, and by the build-time literal check.
arrow_unsupported_message <- function(type){
  ty   <- as.character(type)[1]
  base <- sub("^wrapped:?", "", ty)
  ## A "wrapped" prefix means the payload is one ELEMENT of a list the producing
  ## task returned. That prefix is flumen's own marker and must never reach the
  ## user, so name the element -- and its type when the producer stated one --
  ## instead of printing the marker.
  ##
  ## Matched on the PREFIX, not on the base: the producer states the element's
  ## type ("wrapped:list"), so a base-level match does not fire and the generic
  ## wording would print `class 'wrapped:list'` at the user, leaking the marker.
  if(isTRUE(startsWith(ty, "wrapped"))){
    named <- if(nzchar(base) && !identical(base, "wrapped"))
               paste0(" (that element is a ", base, ")") else ""
    return(list(
      error = paste0("a list element cannot be sent to a Python task over Arrow", named),
      fix   = "flatten it, or return the element as its own output"))
  }
  switch(base,
    raw = list(
      error = "raw vectors cannot be sent to a Python task over Arrow",
      fix   = "convert the bytes first (rawToChar()), or write them to a file"),
    complex = list(
      error = "complex vectors cannot be sent to a Python task over Arrow",
      fix   = "split into real and imaginary parts (Re()/Im())"),
    list(
      error = paste0("an R object of class '", type, "' cannot be sent over Arrow"),
      fix   = "convert it before returning, or write it to a file")
  )
}

## An arrow object that is not tabular. Kept apart from arrow_unsupported_message()
## because its wording would be false here: this value IS Arrow, it simply is not
## something Arrow's own writer can serialise. Two callers share it, the encoder
## and the build-time literal check.
##
## The class is read past flumen's own markers: new_object() and its variants
## prepend "flumen_object" and friends, and class(x)[1] would otherwise print
## the marker instead of the type the user is holding.
arrow_non_tabular_message <- function(object){
  cls <- setdiff(class(object),
                 c("partial_aggregation", "aggregation", "expansion", "flumen_object"))[1]
  list(
    error = paste0("an arrow ", cls, " is not a table"),
    fix   = "convert it first, arrow::as_arrow_table()")
}

## The same words as one sentence, for the paths that hand the text to the master
## inside an error_notify: there it is printed in the `error` field of a block, so
## it must not be a block itself.
arrow_message_sentence <- function(m)
  paste0(m$error, ". Fix: ", m$fix, ".")

from_R_to_arrow <- function(objects, only_object = FALSE){

  if(!"list" %in% class(objects)){
    objects <- list(objects)
  }
  
  r_objects <- lapply(objects, function(object){
    
    type <- get_R_type(object)
    
    switch(type,
           vector = {
             vn <- names(object)
             df_v <- as.data.frame(object)
             colnames(df_v) <- "x"
             if(!is.null(vn)){
               df_v[[".names"]] <- vn
             }
             arrow_object <- arrow::Table$create(df_v)
           },
           list = {
             arrow_object <- arrow::Table$create(as.data.frame(object))
           },
           matrix = {
             df <- as.data.frame(object)
             rn <- rownames(df)
             if(!is.null(rn) && !identical(rn, as.character(seq_len(nrow(df))))){
               df[[".rownames"]] <- rn
             }
             rownames(df) <- NULL
             arrow_object <- arrow::Table$create(df)
           },
           data.frame = {
             df <- object
             rn_attr <- attr(df, "row.names")
             if (is.character(rn_attr)) {
               df[[".rownames"]] <- rn_attr
               attr(df, "row.names") <- seq_len(nrow(df))
             }
             rownames(df) <- NULL
             arrow_object <- arrow::Table$create(df)
           },
           raw = {
             stop(arrow_message_sentence(arrow_unsupported_message("raw")), call. = FALSE)
           },
           complex = {
             stop(arrow_message_sentence(arrow_unsupported_message("complex")), call. = FALSE)
           },
           {
             stop(arrow_message_sentence(arrow_unsupported_message(type)), call. = FALSE)
           }
    )
    if(only_object){
      return(arrow_object)
    }else{
      return(list(type = type, object = arrow_object))
    }
  })
  return(r_objects)
}

arrow_write_to_raw_fast <- function(arrow_table){
  estimated <- arrow_table$nbytes() + 65536L  # data plus schema/metadata overhead
  buf    <- arrow::BufferOutputStream$create(estimated)
  writer <- arrow::RecordBatchStreamWriter$create(buf, arrow_table$schema)
  writer$write_table(arrow_table)
  writer$close()
  buf$finish()$data()
}

raw_payload <- function(x){
  if(inherits(x, "ChunkedArray")) return(x$chunk(0L)$as_vector()[[1L]])
  if(inherits(x, "Array"))        return(x$as_vector()[[1L]])
  x
}

pack_int32_le <- function(n){
  n <- as.integer(n)
  as.raw(c(bitwAnd(n,              0xFFL),
           bitwAnd(bitwShiftR(n,  8L), 0xFFL),
           bitwAnd(bitwShiftR(n, 16L), 0xFFL),
           bitwAnd(bitwShiftR(n, 24L), 0xFFL)))
}

read_int64_le <- function(raw8){
  lo <- readBin(raw8[1:4], "integer", size = 4L, endian = "little")
  hi <- readBin(raw8[5:8], "integer", size = 4L, endian = "little")
  (if(lo < 0L) lo + 2^32 else as.numeric(lo)) + hi * 2^32
}

from_arrow_binary_to_R <- function(data, data_type){

  # Strip "wrapped:" prefix if present (multi-output Arrow list element)
  data_type <- sub("^wrapped:", "", data_type)

  if(inherits(data, c("Array", "ChunkedArray"))){
    arr        <- if(inherits(data, "ChunkedArray")) data$chunk(0L) else data
    d          <- arr$data()
    bufs       <- d$buffers
    parent_off <- d$offset
    off_raw    <- bufs[[2L]]$data()
    byte_pos   <- parent_off * 8L + 1L
    start      <- read_int64_le(off_raw[byte_pos:(byte_pos + 7L)])
    reader     <- arrow::BufferReader$create(bufs[[3L]])
    reader$seek(start)
    data <- arrow::read_ipc_stream(reader, as_data_frame = FALSE)
  } else {
    data <- arrow::read_ipc_stream(data, as_data_frame = FALSE)
  }
  
  if("vector" %in% data_type){
    df_v <- as.data.frame(data)
    vn   <- if(".names" %in% names(df_v)) df_v[[".names"]] else NULL
    data <- df_v[["x"]]
    if(!is.null(vn)) names(data) <- vn
  }else if(any(c("file", "file_path") %in% data_type)){
    ## Arrow-encoded path strings produced by the Python wrapper's file-output
    ## branch (Python has no qs2). R/shell wrappers send file_path via qs2
    ## (is_arrow=FALSE) so they bypass this function entirely; this branch is
    ## reached only by Python file outputs and completes the Arrow protocol
    ## for the file_path data type with the same semantics as `vector`.
    df_v <- as.data.frame(data)
    data <- df_v[["x"]]
  }else if("list" %in% data_type){
    data <- as.list(data)
  }else if("matrix" %in% data_type){
    df <- as.data.frame(data)
    rn <- if(".rownames" %in% names(df)) df[[".rownames"]] else NULL
    df[[".rownames"]] <- NULL
    ## An int64 column beyond the int32 range arrives as bit64::integer64, which
    ## as.matrix() strips to its raw bits read as doubles, turning a large count
    ## into a number close to zero.
    ## A base matrix cannot hold integer64, so such columns become double, exact
    ## up to 2^53; beyond that a double would round, and the value is refused
    ## rather than changed.
    i64 <- vapply(df, inherits, logical(1), "integer64")
    if(any(i64)){
      ## Tested on the integer64 itself: after the conversion 2^53 + 1 is
      ## already 2^53 and the loss can no longer be seen.
      if(any(vapply(df[i64], function(x) any(abs(x) > 2^53, na.rm = TRUE), logical(1))))
        stop(paste("an integer matrix holds values beyond 2^53.",
                   "Fix: return a DataFrame instead."), call. = FALSE)
      df[i64] <- lapply(df[i64], as.double)
    }
    m <- as.matrix(df)
    if(!is.null(rn)){
      rownames(m) <- rn
      if(identical(colnames(m), paste0("V", seq_len(ncol(m))))){
        colnames(m) <- NULL
      }
    } else {
      if(identical(colnames(m), paste0("V", seq_len(ncol(m))))){
        colnames(m) <- NULL
      }
      if(identical(rownames(m), as.character(seq_len(nrow(m))))){
        rownames(m) <- NULL
      }
      if(is.null(rownames(m)) && is.null(colnames(m))){
        dimnames(m) <- NULL
      }
    }
    data <- m
  }else if("data.frame" %in% data_type){
    df <- as.data.frame(data)
    if(".rownames" %in% names(df)){
      rownames(df) <- df[[".rownames"]]
      df[[".rownames"]] <- NULL
    }
    local_tz <- Sys.timezone()
    for(col in names(df)){
      if(inherits(df[[col]], "POSIXct")){
        tz <- attr(df[[col]], "tzone")
        ## isTRUE: Sys.timezone() is NA on a host with TZ unset and no tzdata
        ## (a minimal Linux container), and NA == tz makes if() fail on every
        ## data.frame holding a time zone-aware column. An unknown local zone
        ## matches nothing, so the column keeps the zone it arrived with.
        if(isTRUE(!is.null(tz) && tz == local_tz)){
          tmp <- df[[col]]
          attr(tmp, "tzone") <- NULL
          df[[col]] <- tmp
        }
      }
    }
    data <- df
  }else{
    stop(flumen_fail(
      call  = "serialize",
      error = sprintf("object type '%s' not supported", paste(data_type, collapse = ", ")),
      fix   = "return a vector, matrix, data.frame or list"), call. = FALSE)
  }
  
  return(data)
}

## data is a list of 1 or more R objects
serialize_data <- function(data, data_type, to_arrow){
  
  binary_list <- list()
  data_struct <- list(data_struct = NULL, is_arrow = to_arrow)
  file_name <- rep("", length(data))
  
  for(i in seq_along(data)){

    if("raw" %in% class(data[[i]])){
      bin_data <- data[[i]]
      ## A bare "wrapped" or "R_object" label is a MESSAGE TAG, not a type.
      ## Aggregated slots arrive already DECODED (update_param_wildcard_with_value),
      ## so a raw element here is genuine unencoded payload and must be relabelled,
      ## or the receiver qs2-deserializes bytes that were never qs2-serialized.
      ## Only the BARE sentinel: a resolved "wrapped:<type>" already describes
      ## encoded bytes, as does a non-aggregated slot's declared type, and both
      ## are preserved.
      if(isTRUE(as.character(data_type[[i]])[1] == "wrapped")) data_type[i] <- "wrapped:raw"
      if(isTRUE(as.character(data_type[[i]])[1] == "R_object")) data_type[i] <- "raw"
      data_struct$data_struct <- c(data_struct$data_struct, list(data_type[i]))
    }else if("ArrowObject" %in% class(data[[i]])){
      ## The value is ALREADY in the transport's format: write_to_raw() emits
      ## Arrow IPC bytes here whatever codec was decided upstream, so whoever
      ## writes the bytes owns their type and codec.
      ##
      ## Only a TABULAR object can be written at all: arrow::write_to_raw()
      ## accepts nothing else, an Array fails inside it and a Schema produces
      ## bytes that decode to nothing. The rest are refused here in flumen's own
      ## words rather than in another library's.
      if(!inherits(data[[i]], "ArrowTabular"))
        stop(arrow_message_sentence(arrow_non_tabular_message(data[[i]])), call. = FALSE)
      bin_data <- arrow::write_to_raw(data[[i]])
      prefix <- if(startsWith(as.character(data_type[i])[1], "wrapped")) "wrapped:" else ""
      data_type[i] <- paste0(prefix, "data.frame")
      data_struct$is_arrow[i] <- TRUE
      data_struct$data_struct <- c(data_struct$data_struct, list(data_type[i]))
    }else if("file" %in% data_type[i]){
      bin_data <- data[[i]]
      file_name[i] <- bin_data
      data_struct$data_struct <- c(data_struct$data_struct, list(data_type[i]))
    }else{
      
      if(to_arrow[i]){

        ## Preserve the "wrapped:" prefix end-to-end (list-return / aggregation
        ## marker; from_R_to_arrow returns only the base type). Both the
        ## resolved form "wrapped:<type>" AND the bare sentinel "wrapped"
        ## (set when per-element types are not pre-resolved -- e.g. after
        ## composite-producer flattening in expand_wrapped_params) must
        ## propagate the prefix so the receiver knows each field is one
        ## element of an aggregation (and Python's deserialize_param can
        ## return a scalar for a 1-row field instead of a 1-element list).
        wrapped_prefix_i <- if(data_type[i] == "wrapped" ||
                              startsWith(data_type[i], "wrapped:")) "wrapped:" else ""

        ## Must check compatibility of each element with arrow
        if(identical(class(data[[i]]), "list")){

          data_list <- data[[i]]
          data_type_list <- list()

          for(j in seq_along(data_list)){

            data_i <- from_R_to_arrow(data_list[[j]])
            data_type[i] <- paste0(wrapped_prefix_i, data_i[[1]]$type)
            bin_data <- arrow_write_to_raw_fast(data_i[[1]]$object)
            data_type_list <- c(data_type_list, list(data_type[i]))
          }

          data_struct$data_struct <- c(data_struct$data_struct, list(data_type_list))

        }else{
          data_i <- from_R_to_arrow(data[[i]])
          data_type[i] <- paste0(wrapped_prefix_i, data_i[[1]]$type)
          data_struct$data_struct <- c(data_struct$data_struct, list(data_type[i]))
          bin_data <- arrow_write_to_raw_fast(data_i[[1]]$object)
        }
      }else{
        bin_data <- qs2::qs_serialize(data[[i]], compress_level = 1)
        
        data_struct$data_struct <- c(data_struct$data_struct, list(data_type[i]))
      }
    }
    binary_list[[i]] <- bin_data
  }
  
  serialized <- list(
    data = binary_list,
    data_type = data_struct,
    file_name = file_name
  )

  return(serialized)
}

## `forms`: the fixed dispatch field params_form (defined in R/supervisor_lib.R).
## "list" or "aggregate" is delivered as a list, "bare" as one value. Required:
## the form is never recovered by counting slots.
aggregate_params <- function(input_list, forms) {

  if (length(input_list) == 0) return(list())
  data_names <- vapply(input_list, function(x) x$data_names, character(1))
  data_types <- vapply(input_list, function(x) x$data_type, character(1))
  .names <- unique(data_names)
  if (length(forms) != length(.names))
    stop(sprintf(paste("%d parameter form(s) declared for %d parameter(s).",
                       "Fix: reinstall the same flumen build everywhere."),
                 length(forms), length(.names)), call. = FALSE)
  form_of <- setNames(as.character(forms), .names)
  is_arrows  <- vapply(input_list, function(x) isTRUE(x$is_arrow), logical(1))
  ## elem_name per field: per-element name within a data_names group, set by
  ## the supervisor dispatcher from the producer's named multi-output. Used
  ## below to reattach names() onto the aggregated list so consumers can
  ## access by name. Every envelope carries the field (new_message defaults it
  ## to "" per row), and "" is its legitimate value: the producer named none.
  elem_names <- vapply(input_list, function(x){
    v <- x$elem_name
    if (is.null(v)) "" else as.character(v)
  }, character(1))

  output_list <- list()
  unique_names <- unique(data_names)

  for (name in unique_names) {

    idx <- which(data_names == name)

    extracted_data <- lapply(input_list[idx], function(par){
      par_data_type <- par$data_type
      par <- decode_message_data(par$data, par_data_type, par$is_arrow)
      par
    })
    grp_elem_names <- elem_names[idx]
    if (any(nzchar(grp_elem_names))) {
      names(extracted_data) <- grp_elem_names
    }

    ## DECLARED: the container is what the user declared, read off the
    ## message, not inferred from the number of slots it happens to carry.
    is_list <- form_of[[name]] %in% c("list", "aggregate")
    if (is_list) {
      output_list[[name]] <- extracted_data
    } else {
      if (length(extracted_data) != 1L)
        stop(sprintf(paste("parameter '%s' expects one value, the message carries %d.",
                           "Fix: reinstall the same flumen build everywhere."),
                     name, length(extracted_data)), call. = FALSE)
      output_list[[name]] <- extracted_data[[1]]
    }
  }

  return(output_list)
}

## A dependency parameter is ALWAYS a list of constituents (output_req_list),
## a single producer included. The delivered form is decided once, in
## prepare_code_and_parameters_for_wrapper, from the declared params_form.
group_by_name <- function(input_list, data_types = NULL) {
  nari <- names(input_list)
  as_container <- function(x) if (inherits(x, "output_req")) new_output_req_list(x) else x

  if (is.null(nari) || !any(duplicated(nari))) {
    input_list[] <- lapply(input_list, as_container)
    return(input_list)
  }

  ## Split into groups preserving first-occurrence order (like aggregate_params)
  unique_names <- unique(nari)
  grouped_list <- lapply(unique_names, function(name) {
    group <- unname(input_list[which(nari == name)])
    is_req <- vapply(group, function(x) inherits(x, "output_req"), logical(1))
    if (all(is_req)) {
      return(do.call(new_output_req_list, group))
    }
    ## A single-occurrence, non-output_req parameter (e.g. a plain constant
    ## input) must be delivered BARE -- identical to the no-duplicate early-exit
    ## above. Only genuinely duplicated names (aggregation constituents split
    ## across N fields) become a list. file/file_path single params are
    ## intentionally left as a list: the caller's re-classing loop
    ## (supervisor_handlers.R) rebuilds the classed character vector from a list.
    if (length(group) == 1L) {
      dt <- if (!is.null(data_types) && name %in% names(data_types))
              data_types[[name]] else NA_character_
      if (is.na(dt) || !(dt %in% c("file", "file_path"))) {
        return(group[[1]])
      }
    }
    return(group)
  })
  names(grouped_list) <- unique_names

  return(grouped_list)
}
