#' flumen: An R Workflow Manager for Distributed Bioinformatics without Centralized Storage
#'
#' A workflow manager for data pipelines with streaming execution,
#' locality-aware scheduling, cross-machine distribution via TCP, and polyglot
#' tasks (R, Python, shell).
#'
#' @keywords internal
#' @useDynLib flumen, .registration = TRUE
#' @importFrom Rcpp sourceCpp
#' @importFrom stats setNames
#' @importFrom methods is
#' @importFrom utils capture.output
#' @importFrom rlang %||%
"_PACKAGE"

## The external dependencies, called with `::` and so untouched until a task runs.
## Loaded here, R CMD INSTALL's load test refuses an installed but broken one.
.onLoad <- function(libname, pkgname){
  for(p in c("nanonext", "arrow", "qs2", "igraph", "sys", "uuid")){
    err <- tryCatch({ loadNamespace(p); NULL }, error = function(e) conditionMessage(e))
    if(!is.null(err))
      stop(sprintf(paste0("flumen requires '%s', which is installed but cannot be loaded:\n  %s\n",
                          "Reinstall it together with its own dependencies:\n",
                          "  install.packages(\"%s\")"),
                   p, err, p), call. = FALSE)
  }
}
