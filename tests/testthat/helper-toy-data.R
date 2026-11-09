## Synthetic data generators shared across the testthat suite.
## Goal: minimal payloads (<1 KB), deterministic outputs (set.seed),
## CRAN-friendly (only base R + tempdir).

#' Build a tiny counts data.frame keyed by gene_id (deterministic).
#' Seed is derived from the FULL sample_name (sum of UTF-8 codepoints) so that
#' distinct names yield distinct counts. flumen's lineage-based dedup fuses
#' tasks whose par_list hashes match; identical synthetic inputs would
#' silently collapse N task instances into 1, breaking expansion tests.
make_toy_counts <- function(n_genes = 20L, sample_name = "s1", lambda = 50L) {
  set.seed(sum(utf8ToInt(sample_name)))
  data.frame(
    gene_id = sprintf("g%03d", seq_len(n_genes)),
    counts  = rpois(n_genes, lambda = lambda),
    stringsAsFactors = FALSE
  )
}

#' Build a list of toy counts, one per sample name.
make_toy_counts_list <- function(sample_names = c("s1", "s2", "s3"),
                                 n_genes = 20L) {
  setNames(
    lapply(sample_names, function(s) make_toy_counts(n_genes, s)),
    sample_names
  )
}

#' Allocate an isolated working directory under tempdir(); return its path.
#' Caller is responsible for setwd() and on.exit(unlink(...)).
make_test_workdir <- function(prefix = "flumen_test_") {
  d <- tempfile(prefix)
  dir.create(d, recursive = TRUE)
  d
}

#' Materialise a tiny placeholder file at `path` and return path.
make_placeholder_file <- function(path, content = "x") {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  writeLines(content, path)
  path
}
