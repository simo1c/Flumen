## Messaging layer: build, encode, decode, and chunk-transport NNG messages.
##
## new_message frames a payload (one or more values produced by serialize_data)
## into the wire envelope used across scheduler <-> supervisor <-> wrapper.
## decode_message_metadata / decode_message_data parse incoming raw envelopes.
## send_message_by_chunks splits a message into chunks bounded by chunk_size
## (raw payload OR file path on disk), and push_chunked_message / merge_all_chunks
## reassemble them on the receiving side. write_chunk_to_file streams a file-typed
## chunk straight to disk so the receiver never holds the full file in memory.
## read_chunk is the low-level rawConnection / file reader used by send_message_by_chunks.


read_chunk <- function(con,  
                       chunk_number = 1,  
                       chunk_size = NULL) {
  
  if(is(con, "rawConnection")){
    size <- length(rawConnectionValue(con))
  }else if(is(con, "file")){
    file_path <- summary(con)$description
    size <- file.size(file_path)
  }
  
  n_chunks <- ceiling(size / chunk_size)
  
  if(is.null(chunk_size)) {
    chunk <- readBin(con, "raw", n = size)
    chunk_size <- size
    n_chunks <- 1
  }else{
    if(chunk_number <= n_chunks){
      start_position <- (chunk_number - 1) * chunk_size
      seek(con, where = start_position, origin = "start")
      chunk <- readBin(con, "raw", n = chunk_size)
    }else{
      chunk <- raw(0)
      n_chunks <- n_chunks
    }
  }
  
  chunk <- list(
    chunk = chunk,
    chunk_number = chunk_number,
    n_chunks = n_chunks,
    tot_size = size
  )
  
  return(chunk)
}

new_message <- function(sender,
                        msg_id = "msg_id",
                        msg_type,
                        data_type = NULL,
                        data_names = "",
                        data,  ## list of 1 or more serialized R objects or file path
                        is_arrow,
                        file_name = "",
                        elem_name = "",  ## per-element name within a data_names group
                        chunk = 1, ## first, second etc.
                        chunk_size = NULL){ ## in bytes

  ## Each element is chunked independently and a wire message carries chunk i of
  ## every one, so a per-element budget of chunk_size would let the assembled payload
  ## reach n_elements * chunk_size. `eff` is instead the largest cap for which
  ## sum_i min(size_i, eff) still fits in chunk_size: small elements take what they
  ## need, large ones share the rest. Chunk 1 is the largest, since every element
  ## contributes there while later chunks carry only the tails, so bounding it bounds
  ## them all. Monotone in eff, hence the binary search. The bound is on the payload,
  ## with the Arrow header additive on top.
  n_elements     <- length(data)
  eff_chunk_size <- chunk_size
  if(!is.null(chunk_size) && n_elements > 1L){
    elem_sizes <- vapply(seq_along(data), function(i){
      sz <- if(identical(data_type[i], "file")) suppressWarnings(file.size(data[[i]]))
            else length(data[[i]])
      if(is.na(sz)) 0 else as.numeric(sz)
    }, numeric(1))
    if(sum(elem_sizes) > chunk_size){
      lo <- 1; hi <- chunk_size
      while(lo < hi){
        mid <- (lo + hi + 1) %/% 2
        if(sum(pmin(elem_sizes, mid)) <= chunk_size) lo <- mid else hi <- mid - 1
      }
      eff_chunk_size <- max(1, lo)
    }
    ## else: all elements together fit in one chunk -> keep eff = chunk_size.
  }

  data <- lapply(seq_along(data), function(i){
    bin <- data[[i]]
    if(data_type[i] != "file"){
      size <- length(bin)
      if(is.null(chunk_size)){
        list(chunk = bin, chunk_number = 1L, n_chunks = 1L, tot_size = size)
      } else {
        n_ch <- ceiling(size / eff_chunk_size)
        if(chunk <= n_ch){
          start <- (chunk - 1L) * eff_chunk_size + 1L
          end   <- min(as.numeric(chunk) * eff_chunk_size, size)
          chunk_data <- if(start == 1L && end == size) bin else slice_raw(bin, start, end)
          list(chunk = chunk_data, chunk_number = chunk, n_chunks = n_ch, tot_size = size)
        } else {
          list(chunk = raw(0), chunk_number = chunk,
               n_chunks = n_ch, tot_size = size)
        }
      }
    }else{
      ## Read bytes from master:disk for chunked transfer to a worker.
      ## Surface a structured error if the file is missing.
      if(!file.exists(bin)){
        stop(flumen_fail(
          call  = "dispatch",
          error = sprintf("file '%s' not found on the master at dispatch time", bin),
          fix   = "create it on the master, or wrap the input with pre_staged()"), call. = FALSE)
      }
      ## A directory passes file.exists() but file(dir, "rb") fails with the
      ## opaque "cannot open the connection". This happens when a "full bundle"
      ## wildcard (e.g. new_file('`dir/`{{any}}')) matches sub-directories, not
      ## just files.
      if(dir.exists(bin)){
        stop(flumen_fail(
          call  = "dispatch",
          error = sprintf("output path '%s' is a directory and flumen transfers files only", bin),
          fix   = sprintf("narrow the pattern, new_file('`%s/`{{any}}.ext')",
                          dirname(bin))), call. = FALSE)
      }
      con <- file(bin, "rb")
      chunk_info <- read_chunk(con, chunk, eff_chunk_size)
      close(con)
      chunk_info
    }
  })

  chunks_number   <- sapply(data, "[[", 2)
  n_chunks_per_el <- sapply(data, "[[", 3)
  tot_size_per_el <- sapply(data, "[[", 4)

  ## Two distinct notions, both needed:
  ##  - message-level n_chunks/tot_size (scalar max): used by receivers as completion
  ##    counter for the whole multi-element message (readers of msg[[1]]$n_chunks).
  ##  - per-element element_n_chunks/element_tot_size: used by write_chunk_to_file
  ##    to compute the correct byte offset for THIS file (wrong if we used the
  ##    scalar max when files have different sizes).
  n_chunks <- max(n_chunks_per_el)
  tot_size <- max(tot_size_per_el)

  ## Build header Arrow table -- metadata only, no payload column
  payload_sizes <- sapply(data, function(d) length(d$chunk))
  header_tbl <- arrow::arrow_table(
    sender           = sender,
    msg_id           = msg_id,
    msg_type         = msg_type,
    data_type        = data_type,
    data_names       = data_names,
    is_arrow         = is_arrow,
    file_name        = file_name,
    elem_name        = elem_name,
    chunk            = chunks_number,
    n_chunks         = n_chunks,
    tot_size         = tot_size,
    element_n_chunks = n_chunks_per_el,
    element_tot_size = tot_size_per_el,
    payload_size     = as.double(payload_sizes)
  )

  header_sink <- arrow::BufferOutputStream$create()
  arrow::write_ipc_stream(header_tbl, header_sink)
  header_raw <- header_sink$finish()$data()

  ## Assemble: [4B header_len][header][payload_1]...[payload_N]
  ## concat_raw uses std::memcpy in C++ -- much faster than do.call(c, ...) for
  ## large payloads
  all_parts <- c(
    list(pack_int32_le(length(header_raw)), header_raw),
    lapply(data, function(d) d$chunk)
  )
  message <- concat_raw(all_parts)

  message <- list(
    message = message,
    chunk = chunk,
    n_chunks = n_chunks
  )
  
  class(message) <- c("message_arrow", class(message))

  return(message)
}

.flumen_aio_ring <- new.env(parent = emptyenv())
.flumen_aio_ring$buf <- vector("list", 64L)
.flumen_aio_ring$pos <- 0L

## Drop-in replacement for a fire-and-forget nanonext::send_aio(): sends and
## returns the aio exactly as before, but also retains it in the ring so a
## discarded return value cannot be GC-cancelled mid-flush.
flumen_send_aio <- function(socket, message, mode = "raw"){
  aio <- nanonext::send_aio(socket, message, mode = mode)
  r <- .flumen_aio_ring
  r$pos <- (r$pos %% length(r$buf)) + 1L
  r$buf[[r$pos]] <- aio
  invisible(aio)
}

## Fire-and-forget keepalive frame. A stateful firewall or NAT evicts idle TCP
## flows, so a tiny "heartbeat" pushed periodically keeps each flow warm in its
## state table. Single-frame, with no payload of interest: the receiver drains
## it and re-arms.
send_heartbeat <- function(socket, sender){
  data <- serialize_data(list(0L), "R_object", FALSE)
  hb   <- new_message(sender = sender, msg_id = "heartbeat", msg_type = "heartbeat",
                      data_type = unlist(data$data_type$data_struct), data_names = "",
                      data = data$data, is_arrow = data$data_type$is_arrow)
  flumen_send_aio(socket, hb$message)
}


decode_message_data <- function(data, data_type, is_arrow){

  if(is_arrow){
    data <- from_arrow_binary_to_R(data, data_type)
  }else if(identical(sub("^wrapped:", "", data_type), "raw")){
    ## Match the sender-side pass-through in serialize_data() for raw vectors.
    ## Used by shell ops emitting stdout as bytes (no file output declared).
    data <- raw_payload(data)
  }else{
    data <- qs2::qs_deserialize(raw_payload(data))
  }

  return(data)
}


decode_message_metadata <- function(message){

  # first 4 bytes: header_len (little-endian int32)
  header_len <- readBin(message[1:4], "integer", size = 4L, endian = "little")

  # header Arrow IPC (metadata only, no payload)
  h_start    <- 5L
  h_end      <- 4L + header_len
  envelope   <- arrow::read_ipc_stream(slice_raw(message, h_start, h_end), as_data_frame = FALSE)
  n_rows     <- nrow(envelope)

  senders       <- envelope$sender$as_vector()
  msg_ids       <- envelope$msg_id$as_vector()
  msg_types     <- envelope$msg_type$as_vector()
  data_types    <- envelope$data_type$as_vector()
  data_names    <- envelope$data_names$as_vector()
  is_arrows     <- envelope$is_arrow$as_vector()
  file_names    <- envelope$file_name$as_vector()
  ## elem_name: per-row per-element name (named multi-output IPC). Always
  ## present in the envelope schema (new_message defaults to "" per row).
  elem_names    <- envelope$elem_name$as_vector()
  chunks        <- envelope$chunk$as_vector()
  n_chunks_v    <- envelope$n_chunks$as_vector()
  tot_sizes     <- envelope$tot_size$as_vector()
  element_n_chunks_v <- envelope$element_n_chunks$as_vector()
  element_tot_sizes  <- envelope$element_tot_size$as_vector()
  payload_sizes <- as.numeric(envelope$payload_size$as_vector())

  # cursor positioned at first payload byte, kept numeric so an offset above
  # 2.1 GB does not overflow an integer
  pos <- as.numeric(5L + header_len)

  message_decoded <- vector("list", n_rows)

  for(i in seq_len(n_rows)){
    psize <- payload_sizes[i]
    payload_raw <- if(psize > 0L) slice_raw(message, pos, pos + psize - 1L) else raw(0L)
    pos <- pos + psize

    message_decoded[[i]] <- list(
      sender           = senders[i],
      msg_id           = msg_ids[i],
      msg_type         = msg_types[i],
      data_type        = data_types[i],
      data_names       = data_names[i],
      data             = payload_raw,
      is_arrow         = is_arrows[i],
      file_name        = file_names[i],
      elem_name        = elem_names[i],
      chunk            = chunks[i],
      n_chunks         = n_chunks_v[i],
      tot_size         = tot_sizes[i],
      element_n_chunks = element_n_chunks_v[i],
      element_tot_size = element_tot_sizes[i]
    )
  }
  return(message_decoded)
}

## Chunked-message dispatch for NNG over IPC/TCP.
##
## SLIDING WINDOW OF K=2 IN-FLIGHT sendAio OBJECTS: issuing chunk i (i >= 2)
## drains chunk i-1 with call_aio(), so at most two chunk_size buffers are pinned.
## Handing NNG the bytes is not putting them on the wire, so R serialises chunk
## i+1 while i-1 drains: the pipe stays full without the whole message in RAM.
##
## PER-CHUNK TIMEOUT = 5 MINUTES (send_timeout_ms = 300000L). A chunk draining with
## a non-zero result (5 = timed out, or a transport error) is logged, the chunk
## already in flight is cancelled with stop_aio(), the remaining chunks are NOT
## sent, and the function returns send_ok = FALSE so the caller retries the whole
## message. The receiver reassembles all n_chunks, so continuing is wasted work,
## and with a dead peer each chunk would cost its own 5-minute deadline.
##
## DEFERRED SENDS (`defer` non-NULL). Waiting here is safe only for a caller that
## has nothing else to do. An event loop has: it is the ONLY reader of the channel
## its peer sends on, so a loop waiting inside a send stops draining, the peer's
## buffer fills, the peer blocks in ITS send, and neither returns to its loop. A
## caller that passes `defer` hands the send to the registry below as a JOB and
## returns at once, and the loop advances it one step per turn through
## advance_pending_sends(), which applies the same K=2 window, the same per-chunk
## deadline and the same failure rule.
send_message_by_chunks <- function(sender,
                                   message,  ## R list and not message_object
                                   socket,
                                   chunk_size = NULL,
                                   defer = NULL){
  ## Default elem_name to rep("", N) when the caller did not set it: most
  ## message types (errors, parameter_requests, notify) carry no per-element names.
  msg <- new_message(
    sender = message$sender,
    msg_id = message$msg_id,
    msg_type = message$msg_type,
    data_type = message$data_type,
    data_names = message$data_names,
    data = message$data,
    is_arrow = message$is_arrow,
    file_name = message$file_name,
    elem_name = if(!is.null(message$elem_name)) message$elem_name
                else rep("", length(message$sender)),
    chunk = 1,
    chunk_size = chunk_size
  )

  n_chunks <- msg$n_chunks

  ## Hand the whole message over as a job and return. Chunk 1 is already built,
  ## so it travels with the job instead of being serialised a second time.
  if(!is.null(defer)){
    park_send_job(sender      = sender,
                  message     = message,
                  socket      = socket,
                  channel     = defer$channel,
                  chunk_size  = chunk_size,
                  n_chunks    = n_chunks,
                  first_chunk = msg$message,
                  context     = defer$context,
                  retries     = if(is.null(defer$retries)) 0L else defer$retries,
                  retry_delay = if(is.null(defer$retry_delay)) 0 else defer$retry_delay)
    return(TRUE)
  }

  in_flight       <- vector("list", n_chunks)
  send_timeout_ms <- 300000L   ## 5 min per-chunk deadline; see comment above.
  send_ok         <- TRUE      ## flipped to FALSE if any chunk drains with a
                               ## non-zero NNG result.

  ## Orphan-sender guard; see the PER-CHUNK TIMEOUT block above.
  aborted         <- FALSE

  ## ----- chunk 1 -----
  in_flight[[1]] <- nanonext::send_aio(socket, msg$message, mode = "raw",
                                       timeout = send_timeout_ms)

  ## ----- chunks 2..N: sliding window of K=2 in-flight sendAios -----
  ## A non-zero $result on drain means either the 5-min deadline elapsed
  ## (NNG_ETIMEDOUT = 5) or a transport error.
  if(!is.null(chunk_size) & n_chunks > 1){
    for(i in 2:n_chunks){
      msg <- new_message(
        sender = message$sender,
        msg_id = message$msg_id,
        msg_type = message$msg_type,
        data_type = message$data_type,
        data_names = message$data_names,
        data = message$data,
        is_arrow = message$is_arrow,
        file_name = message$file_name,
        chunk = i,
        chunk_size = chunk_size
      )

      in_flight[[i]] <- nanonext::send_aio(socket, msg$message, mode = "raw",
                                           timeout = send_timeout_ms)

      ## Drain chunk i-1: block until NNG has accepted its buffer and
      ## reported a result, then release the R-side reference so the raw
      ## payload can be reclaimed. Bounds peak in-flight RAM to 2 chunks.
      nanonext::call_aio(in_flight[[i - 1L]])
      res <- in_flight[[i - 1L]]$result
      if(!is.null(res) && !is.na(res) && as.integer(res) != 0L){
        send_ok <- FALSE
        to_log(paste0("send_message_by_chunks: chunk ", i - 1L, "/", n_chunks,
                      " of ", message$msg_id[1],
                      " resolved with NNG error code ", as.integer(res),
                      " (5-min timeout or transport error) -- message undeliverable,",
                      " aborting remaining ", n_chunks - (i - 1L), " chunk(s)"), sender)
        ## chunk i was just issued and is in flight; cancel it so we don't wait
        ## another 5-min deadline on a peer we've already given up on.
        tryCatch(nanonext::stop_aio(in_flight[[i]]), error = function(e) NULL)
        in_flight[i]     <- list(NULL)
        in_flight[i - 1L] <- list(NULL)
        aborted <- TRUE
        break
      }
      ## Release the previous slot's reference WITHOUT shrinking the list.
      in_flight[i - 1L] <- list(NULL)
    }
  }

  ## ----- drain the last in-flight chunk -----
  ## The sliding-window loop drains chunk i-1 inside each iteration; chunk N
  ## is the only entry still live after the loop exits.
  if(n_chunks >= 1 && !aborted && !is.null(in_flight[[n_chunks]])){
    nanonext::call_aio(in_flight[[n_chunks]])
    res <- in_flight[[n_chunks]]$result
    if(!is.null(res) && !is.na(res) && as.integer(res) != 0L){
      send_ok <- FALSE
      to_log(paste0("send_message_by_chunks: chunk ", n_chunks, "/", n_chunks,
                    " of ", message$msg_id[1],
                    " resolved with NNG error code ", as.integer(res),
                    " (5-min timeout or transport error)"), sender)
    }
    in_flight[n_chunks] <- list(NULL)
  }

  return(send_ok)
}


## ---------------------------------------------------------------------------
## Deferred sends: the registry an event loop drives instead of waiting.
##
## One job per message, never with more than SEND_WINDOW chunks in flight. Jobs
## are kept in arrival order and only the FIRST job of a channel is advanced, so
## a message finishes before the next one on the same socket starts.
## ---------------------------------------------------------------------------

.flumen_send_jobs <- new.env(parent = emptyenv())
.flumen_send_jobs$items <- list()

## Same K as the blocking sliding window, for the same reason: two chunk buffers
## pinned at a time keeps the pipe full without holding the whole message.
SEND_WINDOW <- 2L

## Issue chunks until the window is full or the message is exhausted. Chunk 1 is
## the one already built by send_message_by_chunks; chunks 2..N are built here.
job_issue_chunks <- function(job){
  while(length(job$in_flight) < SEND_WINDOW && job$next_chunk <= job$n_chunks){
    i <- job$next_chunk
    payload <- if(i == 1L && !is.null(job$first_chunk)){
      job$first_chunk
    } else if(i == 1L){
      new_message(
        sender = job$message$sender,
        msg_id = job$message$msg_id,
        msg_type = job$message$msg_type,
        data_type = job$message$data_type,
        data_names = job$message$data_names,
        data = job$message$data,
        is_arrow = job$message$is_arrow,
        file_name = job$message$file_name,
        elem_name = if(!is.null(job$message$elem_name)) job$message$elem_name
                    else rep("", length(job$message$sender)),
        chunk = 1,
        chunk_size = job$chunk_size
      )$message
    } else {
      new_message(
        sender = job$message$sender,
        msg_id = job$message$msg_id,
        msg_type = job$message$msg_type,
        data_type = job$message$data_type,
        data_names = job$message$data_names,
        data = job$message$data,
        is_arrow = job$message$is_arrow,
        file_name = job$message$file_name,
        chunk = i,
        chunk_size = job$chunk_size
      )$message
    }
    if(i == 1L) job$first_chunk <- NULL   ## released once handed to NNG
    job$in_flight[[length(job$in_flight) + 1L]] <-
      list(i = i, aio = nanonext::send_aio(job$socket, payload, mode = "raw",
                                           timeout = 300000L))
    job$next_chunk <- i + 1L
  }
  job
}

## Register a message as a job. Chunks are issued straight away when this job is
## the only one queued for its channel; otherwise the job waits its turn behind
## the message already going out on that socket.
park_send_job <- function(sender, message, socket, channel, chunk_size,
                          n_chunks, first_chunk, context,
                          retries = 0L, retry_delay = 0){
  job <- list(sender = sender, message = message, socket = socket,
              channel = if(is.null(channel)) "" else channel,
              chunk_size = chunk_size, n_chunks = n_chunks,
              first_chunk = first_chunk, next_chunk = 1L, in_flight = list(),
              context = context, attempt = 1L, retries = retries,
              retry_delay = retry_delay, not_before = 0)
  it <- .flumen_send_jobs$items
  busy <- any(vapply(it, function(j) identical(j$channel, job$channel), logical(1)))
  if(!busy) job <- job_issue_chunks(job)
  it[[length(it) + 1L]] <- job
  .flumen_send_jobs$items <- it
  invisible(NULL)
}

## Advance every registered send by one step. Returns the `context` of every job
## that has definitively failed, so the loop reacts exactly as it reacted to a
## FALSE return from send_message_by_chunks(). Jobs still in flight stay.
advance_pending_sends <- function(){
  it <- .flumen_send_jobs$items
  if(!length(it)) return(list())
  now    <- proc.time()[["elapsed"]]
  failed <- list()
  keep   <- rep(TRUE, length(it))
  head_done <- character(0)
  for(k in seq_along(it)){
    job <- it[[k]]
    if(job$channel %in% head_done) next          ## not the head of its channel
    head_done <- c(head_done, job$channel)
    if(now < job$not_before) next                ## waiting between retries

    ## Harvest. A chunk resolving with a non-zero result makes the whole message
    ## undeliverable: the receiver reassembles all n_chunks, so the rest is
    ## wasted work and each would cost its own deadline.
    still <- list(); bad <- NULL
    for(f in job$in_flight){
      if(nanonext::unresolved(f$aio)){ still[[length(still) + 1L]] <- f; next }
      res <- f$aio$result
      if(is.null(bad) && !is.null(res) && !is.na(res) && as.integer(res) != 0L)
        bad <- list(i = f$i, code = as.integer(res))
    }
    job$in_flight <- still

    if(!is.null(bad)){
      to_log(paste0("send_message_by_chunks: chunk ", bad$i, "/", job$n_chunks,
                    " of ", job$message$msg_id[1],
                    " resolved with NNG error code ", bad$code,
                    " (5-min timeout or transport error) -- message undeliverable,",
                    " aborting remaining ", job$n_chunks - bad$i, " chunk(s)"),
             job$sender)
      for(f in job$in_flight)
        tryCatch(nanonext::stop_aio(f$aio), error = function(e) NULL)
      job$in_flight <- list()
      if(job$attempt <= job$retries){
        to_log(paste0("send of ", job$message$msg_id[1], " FAILED on attempt ",
                      job$attempt, "/", job$retries + 1L,
                      "; retrying after ", job$retry_delay, "s"), job$sender)
        job$attempt    <- job$attempt + 1L
        job$next_chunk <- 1L
        job$not_before <- now + job$retry_delay
        it[[k]] <- job
        next
      }
      if(!is.null(job$context)) failed[[length(failed) + 1L]] <- job$context
      keep[k] <- FALSE
      next
    }

    job <- job_issue_chunks(job)
    if(job$next_chunk > job$n_chunks && !length(job$in_flight)) keep[k] <- FALSE
    else it[[k]] <- job
  }
  .flumen_send_jobs$items <- it[keep]
  failed
}

## Wait for the registered sends on the way out: a process that parks a message
## and then exits would otherwise drop it. Bounded, so teardown can never hang.
flush_pending_sends <- function(timeout_secs = 30){
  deadline <- proc.time()[["elapsed"]] + timeout_secs
  while(length(.flumen_send_jobs$items) && proc.time()[["elapsed"]] < deadline){
    advance_pending_sends()
    if(length(.flumen_send_jobs$items)) nanonext::msleep(2)
  }
  invisible(length(.flumen_send_jobs$items) == 0L)
}

## Drop whatever an earlier run of this session left registered. The registry
## lives in the package namespace, so it outlives the run that filled it: a run
## that failed or was interrupted with a send still pending handed that job to
## the NEXT run, whose loop then reported it as its own dispatch failure and
## aborted a perfectly valid workflow. Those jobs belong to sockets already
## closed and to a run nobody waits on any more, so they are stopped, not sent.
discard_pending_sends <- function(){
  for(job in .flumen_send_jobs$items)
    for(f in job$in_flight)
      tryCatch(nanonext::stop_aio(f$aio), error = function(e) NULL)
  .flumen_send_jobs$items <- list()
  invisible(NULL)
}

## `strict = FALSE` keeps the reassembly but drops the refusal, and is meant for
## ONE situation: a message that is itself carrying an error. A truncated stack
## trace still tells you what failed, while refusing it would replace the real
## cause with a transport complaint. The mismatch is logged instead, so the loss
## is recorded rather than ignored.
merge_all_chunks <- function(chunk_list, strict = TRUE) {
  
  list_lengths <- sapply(chunk_list, function(chunk){
    return(length(chunk$message))
  })
  
  num_elements <- list_lengths[1]
  
  sender <- chunk_list[[1]]$message[[1]]$sender
  msg_type <- chunk_list[[1]]$message[[1]]$msg_type
  data_type <- chunk_list[[1]]$message[[1]]$data_type
  data_names <- chunk_list[[1]]$message[[1]]$data_names
  is_arrow <- chunk_list[[1]]$message[[1]]$is_arrow
  file_name <- chunk_list[[1]]$message[[1]]$file_name
  n_chunks <- chunk_list[[1]]$message[[1]]$n_chunks
  
  output_list <- lapply(1:num_elements, function(i) {
    
    data_type <- chunk_list[[1]]$message[[i]]$data_type
    file_name <- chunk_list[[1]]$message[[i]]$file_name

    if(data_type == "file"){

      merged_data <- qs2::qs_serialize(file_name, compress_level = 1)

    }else{
      merged_data <- if(length(chunk_list) == 1L){
        raw_payload(chunk_list[[1]]$message[[i]]$data)
      } else {
        concat_raw(lapply(chunk_list, function(l) raw_payload(l$message[[i]]$data)))
      }
    }

    names(merged_data) <- NULL

    ## The only check the transport performs on itself: the reassembled payload
    ## must be exactly as long as the sender said. Python declares the same field
    ## (objects.py, tot = len(d)), so a message produced there is checked too.
    ##
    ## Skipped for data_type == "file": there merged_data is a fresh
    ## serialisation of the file NAME, the bytes having gone to disk chunk by
    ## chunk via write_chunk_to_file, so its length has nothing to do with the
    ## declared size.
    exp_size <- chunk_list[[1]]$message[[i]]$element_tot_size
    if(!identical(data_type, "file") && !is.null(exp_size) &&
       is.finite(exp_size) && exp_size > 0){
      got_size <- length(merged_data)
      if(!identical(as.numeric(got_size), as.numeric(exp_size))){
      if(!strict){
        to_log(sprintf(paste0("[TRANSPORT] element %d of the message from '%s' arrived ",
                              "TRUNCATED: %s of %s bytes (%d chunk(s)). Not refused because ",
                              "this message carries an error; what follows may be incomplete."),
                       i, chunk_list[[1]]$message[[i]]$sender,
                       format(got_size, scientific = FALSE),
                       format(as.numeric(exp_size), scientific = FALSE),
                       length(chunk_list)), "merge_all_chunks")
      } else
        stop(flumen_fail(
          call  = "transfer",
          error = sprintf(paste("element %d from '%s' reassembled to %s bytes,",
                                "sender declared %s (%d chunk(s))"),
                          i, chunk_list[[1]]$message[[i]]$sender,
                          format(got_size, scientific = FALSE),
                          format(as.numeric(exp_size), scientific = FALSE),
                          length(chunk_list)),
          fix   = "re-run, and report it with the worker logs if it repeats"), call. = FALSE)
      }
    }

    chunk_num  <- chunk_list[[1]]$message[[i]]$chunk
    sender     <- chunk_list[[1]]$message[[i]]$sender
    msg_type   <- chunk_list[[1]]$message[[i]]$msg_type
    data_names <- chunk_list[[1]]$message[[i]]$data_names
    elem_name  <- chunk_list[[1]]$message[[i]]$elem_name
    n_chunks   <- chunk_list[[1]]$message[[i]]$n_chunks
    is_arrow_i <- chunk_list[[1]]$message[[i]]$is_arrow

    list(
      sender = sender,
      msg_type = msg_type,
      data_type = data_type,
      data_names = data_names,
      elem_name = elem_name,
      data = merged_data,
      is_arrow = is_arrow_i,
      file_name = file_name,
      chunk = chunk_num,
      n_chunks = n_chunks
    )
  })
  return(output_list)
}

## Accumulate one received chunk under its msg_id, keeping the per-msg_id list
## ordered by chunk number so merge_all_chunks() can reassemble it. Elements
## whose data_type is "file" are streamed straight to disk here and their data
## slot emptied, so a large file transfer never accumulates in RAM.
push_chunked_message <- function(chunked_message,
                                 message,
                                 msg_id,
                                 chunk) {

  w_file <- which(sapply(message, function(m){
    m$data_type == "file"
  }))
  
  if(length(w_file)){
    
    for(i in w_file){

      msg_file <- message[[i]]

      write_chunk_to_file(
        msg_file$file_name,
        msg_file$data,
        msg_file$chunk,
        msg_file$element_n_chunks,
        msg_file$element_tot_size,
        ## same leniency merge_all_chunks applies to objects: a message that
        ## carries an error is logged, not refused, so a transport failure does
        ## not replace the error the message was sent to report.
        strict = !identical(msg_file$msg_type, "error"),
        transfer_id = msg_id
      )
      ## empty data slot
      msg_file$data <- raw(0)
      message[[i]] <- msg_file
    }
  }
  
  new_chunk <- list(
    chunk = chunk,
    message = message
  )

  if (msg_id %in% names(chunked_message)) {
    # If msg_id exists, append the new chunk and reorder
    chunked_message[[msg_id]] <- append(chunked_message[[msg_id]], list(new_chunk))

    # Order the chunks
    all_chunks <- chunked_message[[msg_id]]
    chunk_order <- order(sapply(all_chunks, "[[", "chunk"))
    chunked_message[[msg_id]] <- all_chunks[chunk_order]

  } else {
    # If msg_id is new, create a new list with the single chunk
    chunked_message[[msg_id]] <- list(new_chunk)
  }

  return(chunked_message)
}

## Write one chunk of a file received over the wire into a temporary file beside
## the destination: chunk_index == 1 opens "wb" and truncates, every later chunk
## opens "ab" and appends, and the last one renames it onto the destination.
##
## "ab" always writes at EOF, so the result does not depend on seek positioning
## on an "r+b" connection. What it does depend on is ORDER: chunks of the same
## file must be processed in increasing chunk_index. That holds because each
## file travels on exactly one NNG socket (one pipe per master->worker dispatch,
## one dedicated temp_socket per P2P request), TCP is FIFO on a single
## connection, the supervisor loop consumes messages serially, and distinct
## files have distinct destination paths.
##
## Anything that breaks that order breaks the file.
write_chunk_to_file <- function(file_name,
                                chunk,
                                chunk_index,
                                n_chunks,
                                tot_size,
                                strict = TRUE,
                                transfer_id = ""){

  dirname <- dirname(file_name)
  if(!dir.exists(dirname)){
    ## An ABSOLUTE declared path is used verbatim on every worker, so the worker
    ## must be able to create it HERE. Said plainly, and before the open: without
    ## this the failure surfaced as "cannot open file", which names the path but
    ## neither the reason nor the way out. Same shape as the two failures already
    ## reported from this function (a truncated transfer, a rename that fails).
    suppressWarnings(dir.create(dirname, recursive = TRUE))
    if(!dir.exists(dirname))
      stop(flumen_fail(
        call  = "transfer",
        error = sprintf("cannot create the directory '%s' for the incoming file '%s'",
                        dirname, file_name),
        fix   = "create it on this worker, or declare the path relative to root_path"), call. = FALSE)
  }

  ## Chunks go to a temporary file beside the destination, renamed onto it once
  ## the last one is in. The destination is never opened for writing while the
  ## transfer runs: when it is the very file the sender is reading (a directory
  ## the worker shares with the master, undeclared in shared_path), truncating
  ## it on chunk 1 destroyed the source.
  ##
  ## Named after the transfer, so every chunk of one transfer finds the same
  ## file and two concurrent transfers of the same path never share one. Hashed:
  ## transfer ids carry parameter names and uuids, too long for a file name
  ## already near the limit, and "<supervisor>" is not a legal Windows name.
  part_name <- paste0(file_name, ".flumen-part-", substr(rlang::hash(transfer_id), 1, 16))

  ## The chunk that completes THIS file. An empty file is announced with 0
  ## chunks (ceiling(0 / chunk_size)) yet still travels as chunk 1, so its last
  ## chunk is 1: waiting for chunk 0 left it as an unrenamed .flumen-part file
  ## and the consumer found no file at all. In a message carrying several
  ## files, a smaller one receives empty chunks past its own end; they hold no
  ## bytes and must not reopen a part file that was already renamed into place.
  nch  <- suppressWarnings(as.integer(n_chunks))
  last <- if (length(nch) == 1L && !is.na(nch)) max(1L, nch) else NA_integer_
  if (isTRUE(as.integer(chunk_index) > last)) return(TRUE)

  if (chunk_index == 1L) {
    con <- file(part_name, "wb")
  } else {
    con <- file(part_name, "ab")
  }
  on.exit(close(con), add = TRUE)
  writeBin(chunk, con)

  ## The self-check on the file path, mirroring the one merge_all_chunks performs
  ## on objects and explicitly skips here, because for a file the bytes go to disk
  ## rather than into the merged payload. It is comparable because read_chunk()
  ## declares tot_size as file.size() of the source and sends the bytes RAW, with
  ## no compression or serialisation, so once the last chunk is written the two
  ## must agree exactly.
  ##
  ## Without it a transfer that loses a chunk reaches the consumer short and the
  ## run reports success.
  ##
  ## The connection is closed BEFORE measuring: writeBin buffers, and file.size()
  ## with the connection still open can read short.
  if (!isTRUE(as.integer(chunk_index) == last))
    return(TRUE)

  close(con)
  on.exit()
  if (!is.null(tot_size) && is.finite(tot_size) && tot_size > 0) {
    got <- file.size(part_name)
    if (!identical(as.numeric(got), as.numeric(tot_size))) {
      if (!strict) {
        to_log(sprintf(paste0("[TRANSPORT] file '%s' arrived TRUNCATED: %s of %s bytes ",
                              "(%s chunk(s)). Not refused because this message carries ",
                              "an error; what follows may be incomplete."),
                       file_name, format(got, scientific = FALSE),
                       format(as.numeric(tot_size), scientific = FALSE), n_chunks),
               "write_chunk_to_file")
      } else {
        unlink(part_name)
        stop(flumen_fail(
          call  = "transfer",
          error = sprintf("file '%s' was written with %s bytes, sender declared %s (%s chunk(s))",
                          file_name, format(got, scientific = FALSE),
                          format(as.numeric(tot_size), scientific = FALSE), n_chunks),
          fix   = "re-run, and report it with the worker logs if it repeats"), call. = FALSE)
      }
    }
  }

  if (!file.rename(part_name, file_name)) {
    unlink(part_name)
    stop(flumen_fail(
      call  = "transfer",
      error = sprintf("could not move the received file into place at '%s'", file_name),
      fix   = "check the permissions of the destination directory on the worker"),
      call. = FALSE)
  }

  return(TRUE)
}

## Wrapper-side helper: send an "error" message to the supervisor.
## Called from wrapper.R and wrapper_shell.R inside tryCatch or after non-zero shell
## exit status. The supervisor's "error" handler forwards the error to the scheduler,
## which broadcasts terminate and quits with status 1; run_workflow() then calls stop().
##
##   message : error string (conditionMessage for R, sprintf for shell status)
##   stderr  : captured stderr (shell op; "" for R op)
##   status  : exit status (shell op; NA_integer_ for R op)
send_error_message <- function(task_id, s, chunk_size, message, stderr = "", status = NA_integer_){

  err_payload <- list(
    task_id   = task_id,
    message = as.character(message),
    stderr  = as.character(stderr),
    status  = as.integer(status)
  )

  data <- serialize_data(list(err_payload), "R_object", FALSE)

  out_message <- list(
    sender     = task_id,
    msg_id     = task_id,
    msg_type   = "error",
    data_type  = unlist(data$data_type$data_struct),
    data_names = "",
    data       = data$data,
    is_arrow   = data$data_type$is_arrow,
    file_name  = data$file_name
  )

  send_message_by_chunks(task_id, out_message, s, chunk_size)
}

add_socket <- function(socket_list,
                       new_socket,
                       new_socket_name){

  if(!length(socket_list)){
    socket_list <- list(new_socket)
    names(socket_list) <- new_socket_name
  }else{
    socket_names <- names(socket_list)
    socket_list <- c(socket_list, new_socket)
    names(socket_list) <- c(socket_names, new_socket_name)
  }

  return(socket_list)
}

add_message <- function(message_list,
                        socket_list,
                        socket_name,
                        timeout = NULL){

  if(!length(message_list)){
    new_message <- nanonext::recv_aio(socket_list[[socket_name]], mode = "raw", timeout = timeout)
    message_list <- list(new_message)
    names(message_list) <- socket_name
  }else{
    message_list_names <- names(message_list)
    new_message <- nanonext::recv_aio(socket_list[[socket_name]], mode = "raw", timeout = timeout)
    message_list <- c(message_list, new_message)
    names(message_list) <- c(message_list_names, socket_name)
  }

  return(message_list)
}

update_message <- function(message_list,
                           socket_list,
                           socket_name,
                           timeout = NULL){

  message_list[[socket_name]] <- nanonext::recv_aio(socket_list[[socket_name]], mode = "raw", timeout = timeout)
  return(message_list)
}

## Longest Unix-socket path the OS accepts. sun_path is 104 bytes on macOS
## including the terminator, so 103 are usable. Linux allows a few more, so this
## lower bound keeps behaviour identical on both. The limit applies to the string
## handed to bind(), not to its resolution: a RELATIVE path stays short whatever
## the working directory is, which is why the supervisor<->wrapper socket, built relative
## in sup_handle_execute_task(), needs none of this.
IPC_PATH_MAX <- 103L

## Per-process counter for socket file names. Every R process gets its own
## tempdir(), so a process-local sequence is enough for uniqueness.
.ipc_seq <- local({
  i <- 0L
  function(){ i <<- i + 1L; i }
})

## Build a socket file path short enough to bind. tempfile()'s descriptive names
## are what overflows: a deep tempdir() plus "sock_ctrl_w1_1a2b3c4d.socket"
## pushes the path past the limit and NNG fails with "15 | Address invalid".
## Counted in bytes, not characters: sun_path is a byte buffer, and the two
## differ once a path contains non-ASCII.
short_ipc_file <- function(){
  path <- file.path(tempdir(), sprintf("f%d.s", .ipc_seq()))
  n <- nchar(path, type = "bytes")
  if(n > IPC_PATH_MAX){
    stop(flumen_reject(
      call  = "run_workflow()",
      error = sprintf("temporary directory too deep for IPC sockets: '%s' is %d bytes, max %d",
                      path, n, IPC_PATH_MAX),
      fix   = "point TMPDIR at a shorter directory, TMPDIR=/tmp, and restart R"),
      call. = FALSE)
  }
  path
}

generate_socket_path <- function(prefix, host = NULL, port = NULL){

  if(is.null(host)){
    ## local IPC -- socket file under tempdir() (CRAN policy: no writes to cwd).
    socket_file <- short_ipc_file()
    socket_name <- paste0(prefix, "-", tools::file_path_sans_ext(basename(socket_file)))
    socket_path <- paste0("ipc://", socket_file)
    dial_path   <- socket_path
  } else {
    ## remote TCP
    socket_name <- paste0(prefix, "-", port)
    socket_path <- paste0("tcp://0.0.0.0:", port)   ## listen (all interfaces)
    dial_path   <- paste0("tcp://", host, ":", port) ## dial (caller's perspective)
  }

  return(
    list(
      socket_name = socket_name,
      socket_path = socket_path,
      dial_path   = dial_path
    )
  )
}

create_temp_socket <- function(worker, protocol, type, host = NULL, port = NULL){

  socket     <- generate_socket_path(worker, host = host, port = port)
  socket_name <- socket$socket_name
  socket_path <- socket$socket_path
  dial_path   <- socket$dial_path

  if(type == "dial"){
    sock_obj <- nanonext::socket(protocol, dial = socket_path)
  }else{
    sock_obj <- nanonext::socket(protocol, listen = socket_path)
  }

  return(list(
    socket_name = socket_name,
    socket_path = socket_path,
    dial_path   = dial_path,   ## path communicated to external callers (= socket_path for IPC)
    socket      = sock_obj
  ))
}

init_sockets <- function(socket_list,
                         message_list,
                         worker_name,
                         data_req_socket = NULL,
                         sock_ctrl_path = NULL,   ## IPC dial path for local supervisor (per-worker, push/pull)
                         sock_term_path = NULL,   ## IPC dial path for local supervisor's dedicated terminate channel
                         sock_res_path  = NULL,   ## scheduler's PULL listen path; supervisor's PUSH dial path
                         master_url     = NULL    ## worker-initiated TCP: dial outbound to master at this URL prefix
                         ){

  if(worker_name == "Central scheduler"){

    ## Central scheduler listens for results on a single unique path. Per-worker
    ## dispatch sockets sock_ctrl_<name> are opened in sched_init_workflow()
    ## after launch_local_and_register_supervisors() returns, with the per-worker
    ## IPC path (local) or TCP port (remote) recorded in supervisor_info.
    if(is.null(sock_res_path)){
      stop(flumen_reject(
        error = "the scheduler was started without its result socket",
        fix   = "engine invariant, report the workflow"), call. = FALSE)
    }
    sock_res  <- nanonext::socket(protocol = "pull", listen = sock_res_path)

  }else{

    if(!is.null(master_url)){
      ## Worker-initiated TCP mode: supervisor DIALS the master.
      ## master_url has format "tcp://host:notify_port,ctrl_port". The master
      ## listens on the SHARED notify_port, where all remote supervisors fan in,
      ## and on this worker's dedicated ctrl_port; the terminate port is
      ## ctrl_port + 1, derived here. Channels:
      ##   - sock_res  (PUSH dial -> notify_port)     results & READY uplink, SHARED
      ##   - sock_ctrl (PAIR dial -> ctrl_port)       handshake / execute_task down,
      ##                                              worker keepalive up
      ##   - sock_term (PAIR dial -> ctrl_port + 1)   terminate down, keepalive up.
      ##       A dedicated channel that never carries bulk data, so an abort
      ##       signal cannot be head-of-line blocked behind a chunked transfer.
      ## ctrl/term are PAIR rather than PULL so the worker, the dialer, can also
      ## SEND on them: its keepalive is what keeps each flow warm and what lets NNG
      ## detect a broken pipe and reconnect. See the worker heartbeat in supervisor.R.
      body      <- sub("^tcp://", "", master_url)
      base_host <- sub(":.*$", "", body)
      pp        <- strsplit(sub("^[^:]+:", "", body), ",", fixed = TRUE)[[1]]
      if(length(pp) < 2L){
        stop(flumen_reject(
          call  = "init_sockets()",
          error = sprintf("malformed master_url '%s'", master_url),
          fix   = "the form is 'tcp://host:61000,61001'"), call. = FALSE)
      }
      notify_port <- as.integer(pp[1])
      ctrl_port   <- as.integer(pp[2])
      term_port   <- ctrl_port + 1L
      sock_res  <- nanonext::socket(protocol = "push",
                                    dial = paste0("tcp://", base_host, ":", notify_port))
      sock_ctrl <- nanonext::socket(protocol = "pair",
                                    dial = paste0("tcp://", base_host, ":", ctrl_port))
      sock_term <- nanonext::socket(protocol = "pair",
                                    dial = paste0("tcp://", base_host, ":", term_port))
    } else {
      ## Local IPC mode: supervisor DIALS to the scheduler's per-worker paths.
      ## sock_ctrl_path / sock_term_path are unique to this worker (one PAIR socket
      ## each); broadcasts (e.g., terminate) are emulated by the scheduler
      ## iterating over every sock_term_<name>.
      if(is.null(sock_res_path) || is.null(sock_ctrl_path) || is.null(sock_term_path)){
        stop(flumen_reject(
          error = "a local supervisor was started without its sockets",
          fix   = "engine invariant, report the workflow"), call. = FALSE)
      }
      sock_res  <- nanonext::socket(protocol = "push", dial = sock_res_path)
      sock_ctrl <- nanonext::socket(protocol = "pair", dial = sock_ctrl_path)
      sock_term <- nanonext::socket(protocol = "pair", dial = sock_term_path)
    }

    ## For a request from another supervisor
    if(!is.null(data_req_socket)){
      sock_data_req <- nanonext::socket(protocol = "pull", listen = data_req_socket)
    }
  }

  socket_list <- add_socket(socket_list, sock_res, "sock_res")

  if(worker_name == "Central scheduler"){
    message_list <- add_message(message_list, socket_list, "sock_res")
  }else{
    socket_list <- add_socket(socket_list, sock_ctrl, "sock_ctrl")
    socket_list <- add_socket(socket_list, sock_term, "sock_term")
    ## sock_data_req is only created above when data_req_socket was provided
    ## (local IPC mode). In worker-initiated TCP mode it is opened later,
    ## after the handshake delivers the master-allocated data_req_port.
    if(exists("sock_data_req", inherits = FALSE)){
      socket_list <- add_socket(socket_list, sock_data_req, "sock_data_req")
      message_list <- add_message(message_list, socket_list, "sock_data_req")
    }
    message_list <- add_message(message_list, socket_list, "sock_ctrl")
    message_list <- add_message(message_list, socket_list, "sock_term")
  }

  to_log("Sockets initialized", worker_name)

  return(
    list(
      socket_list = socket_list,
      message_list = message_list
    )
  )
}

## Probe whether TCP endpoints are reachable with transient NNG dialers (nanonext),
## the SAME transport flumen uses for real connections. A "push" socket dials the
## target (a worker's data-request port is a "pull" listener, so the two are
## compatible), and `pipe_notify` signals `cv` when a pipe is added, so a non-zero
## cv_value within `timeout` means a connection established. This detects upfront
## whether a master or peer can reach a worker's P2P port, avoiding the silent hang
## of a buffered send_aio on an unreachable dial.
##
## All endpoints are probed CONCURRENTLY in ONE `timeout` window. `targets` is a
## list of length-2 c(host, port) vectors, the dials all fire async, and a single
## wait reports which established a pipe, so wall-clock stays about `timeout`
## however many targets are unreachable. Returns a logical vector aligned to
## `targets`, and never errors.
probe_tcp_reachable_many <- function(targets, timeout = 3){
  if(length(targets) == 0L) return(logical(0))
  units <- lapply(targets, function(t){
    s     <- nanonext::socket("push")
    c1    <- nanonext::cv()
    armed <- isTRUE(tryCatch({nanonext::pipe_notify(s, c1, add = TRUE, remove = FALSE); TRUE},
                             error = function(e) FALSE))
    if(armed) tryCatch(nanonext::dial(s, url = paste0("tcp://", t[[1]], ":", as.integer(t[[2]])),
                                      autostart = TRUE), error = function(e) NULL)
    list(s = s, cv = c1, armed = armed)
  })
  Sys.sleep(timeout)                                  ## ONE window for all targets
  res <- vapply(units, function(u)
    isTRUE(u$armed) && isTRUE(tryCatch(nanonext::cv_value(u$cv) > 0, error = function(e) FALSE)),
    logical(1))
  lapply(units, function(u) tryCatch(close(u$s), error = function(e) NULL))
  res
}

close_socket <- function(socket_list, socket_name){
  close(socket_list[[socket_name]])
  socket_list <- socket_list[-which(names(socket_list) == socket_name)]
  return(socket_list)
}

close_message <- function(message_list, socket_name){
  message_list <- message_list[-which(names(message_list) == socket_name)]
  return(message_list)
}
