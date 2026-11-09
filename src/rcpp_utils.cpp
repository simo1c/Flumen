#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
RawVector slice_raw(RawVector x, double start, double end) {
  // start and end are 1-based, as in R, and double so a payload above 2.1 GB
  // still indexes. The bounds check turns invalid input (NA/NaN/Inf, an index
  // out of range, end < start) into a diagnosable R error instead of a segfault.
  if (!R_FINITE(start) || !R_FINITE(end)) {
    Rcpp::stop("slice_raw(): 'start' and 'end' must be finite (got start=%g, end=%g)",
               start, end);
  }
  if (start < 1.0) {
    Rcpp::stop("slice_raw(): 'start' must be >= 1, got %g", start);
  }
  if (end < start) {
    Rcpp::stop("slice_raw(): 'end' (%g) must be >= 'start' (%g)", end, start);
  }
  R_xlen_t s = (R_xlen_t)start;
  R_xlen_t e = (R_xlen_t)end;
  if (e > x.size()) {
    Rcpp::stop("slice_raw(): 'end' (%g) exceeds length(x) (%g)",
               (double)e, (double)x.size());
  }
  R_xlen_t len = e - s + 1;
  RawVector out(len);
  std::memcpy(out.begin(), x.begin() + s - 1, (size_t)len);
  return out;
}

// [[Rcpp::export]]
RawVector concat_raw(List chunks) {
  // Validate first to fail fast with informative error before allocation
  int n = chunks.size();
  for(int i = 0; i < n; i++){
    if (TYPEOF(chunks[i]) != RAWSXP) {
      Rcpp::stop("concat_raw(): element %d is not a raw vector", i + 1);
    }
  }
  // total size first, so the output is allocated once
  size_t total = 0;
  for(int i = 0; i < n; i++){
    total += as<RawVector>(chunks[i]).size();
  }
  RawVector out(total);
  size_t offset = 0;
  for(int i = 0; i < n; i++){
    RawVector chunk = as<RawVector>(chunks[i]);
    std::memcpy(out.begin() + offset, chunk.begin(), chunk.size());
    offset += chunk.size();
  }
  return out;
}
