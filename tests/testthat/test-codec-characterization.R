## PHASE-0 characterization / golden oracle for the serialization codec seam.
##
## Purpose: pin the CURRENT observable behaviour of the encode/decode core
## (serialize_data <-> decode_message_data, driven by get_R_type /
## is_arrow_compatible_type) so a future "codec" refactor can be verified to
## preserve it EXACTLY. Two invariants are captured per value:
##   1. round-trip fidelity: decode(encode(v)) is identical to v (or the
##      documented transformation);
##   2. the emitted wire tags (data_type string + is_arrow flag) that every
##      consumer relies on.
## A refactor that changes either is a regression this test will catch.
##
## Pure-function, no workers, deterministic.

## Production round-trip: choose the codec exactly as a producer would
## (flumen:::is_arrow_compatible_type(flumen:::get_R_type(v))), encode, then decode.
codec_roundtrip <- function(v) {
  ty  <- flumen:::get_R_type(v)
  ta  <- flumen:::is_arrow_compatible_type(ty)
  ser <- flumen:::serialize_data(list(v), ty, ta)
  dt  <- unlist(ser$data_type$data_struct)[[1]]
  ia  <- ser$data_type$is_arrow[[1]]
  list(dt = dt, ia = ia, back = flumen:::decode_message_data(ser$data[[1]], dt, ia))
}

## Golden table: value, expected emitted data_type, expected is_arrow.
## `back` must be identical to the input for every one of these (R<->R lossless).
golden <- list(
  int_vec      = list(v = c(1L, 2L, 3L),                         dt = "vector",     ia = TRUE),
  dbl_vec      = list(v = c(1.5, 2.5),                           dt = "vector",     ia = TRUE),
  chr_vec      = list(v = c("a", "b", "c"),                      dt = "vector",     ia = TRUE),
  lgl_vec      = list(v = c(TRUE, FALSE),                        dt = "vector",     ia = TRUE),
  named_num    = list(v = c(x = 1, y = 2),                       dt = "vector",     ia = TRUE),
  factor       = list(v = factor(c("lo", "hi", "lo")),           dt = "vector",     ia = TRUE),
  Date         = list(v = as.Date("2020-01-01") + 0:2,           dt = "vector",     ia = TRUE),
  POSIXct      = list(v = as.POSIXct("2020-01-01", tz = "UTC") + 0:1, dt = "vector", ia = TRUE),
  matrix_plain = list(v = matrix(1:6, 2, 3),                     dt = "matrix",     ia = TRUE),
  matrix_rn    = list(v = {m <- matrix(1:4, 2, 2); rownames(m) <- c("r1","r2"); m}, dt = "matrix", ia = TRUE),
  df_simple    = list(v = data.frame(a = 1:2, b = c("x","y")),   dt = "data.frame", ia = TRUE),
  df_rn        = list(v = {d <- data.frame(a = 1:2); rownames(d) <- c("g1","g2"); d}, dt = "data.frame", ia = TRUE),
  df_factorcol = list(v = data.frame(g = factor(c("a","b")), v = 1:2, stringsAsFactors = TRUE), dt = "data.frame", ia = TRUE),
  list_plain   = list(v = list(1L, "two", TRUE),                 dt = "list",       ia = FALSE),
  named_list   = list(v = list(n = 3L, s = "hi"),                dt = "list",       ia = FALSE),
  complex      = list(v = complex(real = 1:2, imaginary = 3:4),  dt = "complex",    ia = FALSE),
  scalar_chr   = list(v = "some/path.txt",                       dt = "vector",     ia = TRUE)
)

for (nm in names(golden)) {
  local({
    g <- golden[[nm]]
    label <- nm
    test_that(sprintf("codec round-trip is lossless + tags stable: %s", label), {
      r <- codec_roundtrip(g$v)
      expect_identical(r$dt, g$dt)                 # emitted data_type tag
      expect_identical(r$ia, g$ia)                 # emitted is_arrow flag
      expect_identical(r$back, g$v)                # value fidelity R<->R
    })
  })
}

test_that("named vector preserves names across the codec (R<->R)", {
  r <- codec_roundtrip(c(alpha = 10, beta = 20))
  expect_identical(names(r$back), c("alpha", "beta"))
})

test_that("matrix rownames survive the .rownames sentinel round-trip", {
  m <- matrix(1:4, 2, 2); rownames(m) <- c("row1", "row2")
  r <- codec_roundtrip(m)
  expect_identical(rownames(r$back), c("row1", "row2"))
})

test_that("data.frame character row.names survive the round-trip", {
  d <- data.frame(x = 1:2); rownames(d) <- c("obs_a", "obs_b")
  r <- codec_roundtrip(d)
  expect_identical(rownames(r$back), c("obs_a", "obs_b"))
})

test_that("raw codec (shell stdout bytes) passes through verbatim", {
  b   <- as.raw(c(1, 2, 3, 255))
  ser <- flumen:::serialize_data(list(b), "raw", FALSE)
  dt  <- unlist(ser$data_type$data_struct)[[1]]
  ia  <- ser$data_type$is_arrow[[1]]
  expect_identical(dt, "raw")
  expect_false(ia)
  expect_identical(flumen:::decode_message_data(ser$data[[1]], dt, ia), b)
})

test_that("arrow-capable value also round-trips losslessly when forced to qs2", {
  ## The codec choice is separable from fidelity: a vector encoded via qs2
  ## (to_arrow=FALSE) must still decode identically. Guards a refactor that
  ## changes which codec is chosen without breaking either codec.
  v   <- c(7L, 8L, 9L)
  ser <- flumen:::serialize_data(list(v), "vector", FALSE)
  dt  <- unlist(ser$data_type$data_struct)[[1]]
  ia  <- ser$data_type$is_arrow[[1]]
  expect_false(ia)
  expect_identical(flumen:::decode_message_data(ser$data[[1]], dt, ia), v)
})
