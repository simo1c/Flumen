#' flumen: An R Workflow Manager for Distributed Bioinformatics without Centralized Storage
#'
#' A workflow manager for data pipelines with streaming execution,
#' locality-aware scheduling, cross-machine distribution via TCP, and polyglot
#' tasks (R, Python, shell).
#'
#' @keywords internal
#' @useDynLib flumen, .registration = TRUE
#' @importFrom Rcpp sourceCpp
#' @importFrom stats setNames na.omit
#' @importFrom methods is
#' @importFrom igraph add_vertices
#' @importFrom utils capture.output flush.console head
#' @importFrom rlang %||%
"_PACKAGE"
