## Keep every to_log() write out of the package directory for the whole suite.
## run_workflow() honours an already-set FLUMEN_LOG_DIR when no log_dir is
## passed, and to_log() reads the same variable, so one setting covers the
## scheduler, the supervisors and the wrappers they spawn. Without this the
## suite creates tests/testthat/log/, which R CMD check then reports.
## testthat sources setup-*.R before any test file, so this applies from the
## first test on (a Sys.setenv() inside a single test-*.R would only take
## effect once that file is reached).
Sys.setenv(FLUMEN_LOG_DIR = file.path(tempdir(), "flumen_test_log"))
dir.create(Sys.getenv("FLUMEN_LOG_DIR"), recursive = TRUE, showWarnings = FALSE)
