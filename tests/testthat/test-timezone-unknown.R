## Sys.timezone() is NA on a host with TZ unset and no tzdata, a minimal Linux
## container among them. The Arrow decoder must not compare a POSIXct column's
## zone with it inside if(), which fails with "missing value where TRUE/FALSE
## needed" (R/serialize.R, from_arrow_binary_to_R). An unknown local zone matches
## nothing, so the column keeps the zone it arrived with.

## The decoder with Sys.timezone() answering NA, as on such a host.
decoder_without_local_zone <- function() {
  dec <- flumen:::from_arrow_binary_to_R
  env <- new.env(parent = environment(dec))
  env$Sys.timezone <- function(...) NA_character_
  environment(dec) <- env
  dec
}

roundtrip <- function(df, dec) {
  s <- flumen:::serialize_data(list(df), "data.frame", TRUE)
  dec(s$data[[1]], "data.frame")
}

test_that("time zone-aware columns decode when the local zone is unknown", {
  dec <- decoder_without_local_zone()
  for (tz in c("UTC", "Europe/Rome", "America/New_York")) {
    df <- data.frame(t = as.POSIXct("2020-01-01 10:00:00", tz = tz), v = 1)
    expect_identical(roundtrip(df, dec), df, info = tz)
  }
})

test_that("columns without a time zone are unaffected by an unknown local zone", {
  dec <- decoder_without_local_zone()
  df <- data.frame(d = as.Date("2020-01-01"), a = 1:2, b = c("x", "y"))
  expect_identical(roundtrip(df, dec), df)
})
