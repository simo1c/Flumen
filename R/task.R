## Task constructors and script/cmd tags.
##
## Public new_R_task / new_python_task / new_shell_task entry points,
## the internal new_task() factory they share, the task_list container,
## and the new_script() / new_cmd() exec-tagging helpers.


## `input` and `output` must come from their wrapper constructors. Passing a
## bare new_object()/new_file() reaches new_*_params() as a value with no
## parameter name, so the task registers no such output: the workflow builds
## and runs, but a downstream new_dep() on it fails much later with an
## unrelated "input was not resolved" error pointing at the consumer. Reject
## it here, where the mistake is made. NULL stays valid (tasks may declare no
## input, or no output).
validate_task_io <- function(fn, task_id, input, output){

  check <- function(value, arg_name, wrapper, example){
    if(is.null(value)) return(invisible(NULL))
    if(inherits(value, paste0(arg_name, "_object"))) return(invisible(NULL))
    stop(flumen_reject(
      call  = sprintf("%s(task_id = '%s')", fn, task_id %||% "<unnamed>"),
      error = sprintf("'%s' must be created with %s(), but has class '%s'",
                      arg_name, wrapper, paste(class(value), collapse = "/")),
      fix   = sprintf("wrap it and give it a name, e.g. %s", example)), call. = FALSE)
  }

  check(input,  "input",  "new_input",
        'input = new_input(x = new_object_exp("{sample}"))')
  check(output, "output", "new_output",
        'output = new_output(res = new_object("{{res}}"))')

  invisible(NULL)
}


#' Define an R task
#'
#' Creates a workflow task whose command is an R script. The script must
#' define at least one top-level function (any name); Flumen calls the last
#' top-level function with input values and captures the returned object(s)
#' as output.
#'
#' @param task_id Character. Unique identifier for the task within the
#'   workflow.
#' @param input An [new_input()] object describing the task's inputs (R objects,
#'   files, or dependencies on other tasks).
#' @param output An [new_output()] object describing what the task produces.
#' @param exec R script source created with [new_script()].
#' @param requires Optional named list of minimum resource requirements, with
#'   fields `cpu` (non-negative integer), `mem` (size with unit, for example
#'   `"32G"`), `gpu` (non-negative integer) and `tags` (character vector of
#'   capability labels, all of which the worker must advertise). The task is
#'   dispatched only to workers declaring matching or greater resources. `NULL`
#'   (default) means any worker is eligible.
#'
#' @return An object of class `task_object` with subclass `R_task_object`.
#'
#' @seealso [new_python_task()], [new_shell_task()], [new_script()], [new_dep()]
#'
#' @examples
#' task <- new_R_task(
#'   task_id  = "square",
#'   input  = new_input(x = 2),
#'   output = new_output(out = new_object("{{out}}")),
#'   exec   = new_script('square <- function(x) x^2')
#' )
#'
#' @export
new_R_task <- function(task_id,
                     input = NULL,
                     output = NULL,
                     exec,
                     requires = NULL){

  if(!inherits(exec, "script")){
    stop(flumen_reject(
      call  = "new_R_task()",
      error = "'exec' must be created with new_script()",
      fix   = "exec = new_script(code = \"...\") or new_script(file = \"task.R\")"),
      call. = FALSE)
  }
  validate_task_io("new_R_task", task_id, input, output)

  op <- new_task(
    task_id = task_id,
    type = "R",
    exec = exec,
    par_list = new_R_params(input, output),
    ## exe_id is flumen's OWN key, generated here and never derived from
    ## task_id, as in the other two task constructors. Deriving it from the
    ## user's name would make the two namespaces overlap: a task called "T_1"
    ## produces the same string as instance 1 of task "T", and the two collide
    ## in the expansion registry (workflow_prepare.R::register_op).
    exe_id = uuid::UUIDgenerate(),
    requires = requires
  )
}

## Strip common leading indentation from a multi-line Python source string.
## Python is indentation-sensitive, so embedding source as a literal in R
## (typically indented 4-6 spaces inside a task constructor) requires removing
## the wrapper's leading whitespace before the wrapper.py side parses it.
dedent_python <- function(code) {
  lines <- strsplit(code, "\n")[[1]]
  if (length(lines) > 0 && trimws(lines[1])         == "") lines <- lines[-1]
  if (length(lines) > 0 && trimws(lines[length(lines)]) == "") lines <- lines[-length(lines)]
  non_empty   <- lines[nzchar(trimws(lines))]
  if (length(non_empty) == 0) return("")
  min_indent  <- min(nchar(regmatches(non_empty, regexpr("^\\s*", non_empty))))
  if (min_indent > 0) lines <- sub(paste0("^\\s{", min_indent, "}"), "", lines)
  paste(lines, collapse = "\n")
}

#' Define a Python task
#'
#' Creates a workflow task whose command is a Python script. The script
#' must define at least one top-level function (any name); Flumen calls the
#' last top-level function with input values via Arrow IPC and captures the
#' returned object(s) as output.
#'
#' @param task_id Character. Unique identifier for the task.
#' @param input An [new_input()] object.
#' @param output An [new_output()] object.
#' @param exec Python script source created with [new_script()]. Common
#'   leading indentation is stripped automatically (dedent).
#' @param requires Optional named list of minimum resource requirements, with
#'   fields `cpu` (non-negative integer), `mem` (size with unit, for example
#'   `"32G"`), `gpu` (non-negative integer) and `tags` (character vector of
#'   capability labels, all of which the worker must advertise). The task is
#'   dispatched only to workers declaring matching or greater resources. `NULL`
#'   (default) means any worker is eligible.
#'
#' @return An object of class `task_object` with subclass `python_task_object`.
#'
#' @section Python environment:
#' Each worker must have Python 3.9+ available with `pyarrow`, `pandas`,
#' `numpy` and `pynng` installed: all four are imported when the Python
#' wrapper starts, so a missing one stops the task before it runs. The
#' interpreter is configured at worker level via
#' `new_worker(python_cmd = ...)`.
#'
#' @seealso [new_R_task()], [new_shell_task()], [new_script()]
#'
#' @examples
#' task <- new_python_task(
#'   task_id  = "double",
#'   input  = new_input(x = 3),
#'   output = new_output(out = new_object("{{out}}")),
#'   exec   = new_script('
#'     def double(x):
#'         return x * 2
#'   ')
#' )
#'
#' @export
new_python_task <- function(task_id,
                          input = NULL,
                          output = NULL,
                          exec,
                          requires = NULL){

  if(!inherits(exec, "script")){
    stop(flumen_reject(
      call  = "new_python_task()",
      error = "'exec' must be created with new_script()",
      fix   = "exec = new_script(code = \"...\") or new_script(file = \"task.py\")"),
      call. = FALSE)
  }
  validate_task_io("new_python_task", task_id, input, output)

  exec_dedented <- dedent_python(exec)
  class(exec_dedented) <- class(exec)

  op <- new_task(
    task_id             = task_id,
    type              = "python",
    exec              = exec_dedented,
    par_list          = new_python_params(input, output),
    exe_id            = uuid::UUIDgenerate(),   ## see new_R_task(): flumen's own key
    cmd_interpreter   = "python3",
    shell_interpreter = "bash",
    requires          = requires
  )
}


#' Define a shell task
#'
#' Creates a workflow task that invokes a shell command or script. Inputs
#' and outputs are marshalled as CLI arguments; files are passed by path.
#'
#' @param task_id Character. Unique identifier for the task.
#' @param input An [new_input()] object.
#' @param output An [new_output()] object.
#' @param exec Either a [new_cmd()] (single executable name) or a
#'   [new_script()] (inline shell script).
#' @param requires Optional named list of minimum resource requirements, with
#'   fields `cpu` (non-negative integer), `mem` (size with unit, for example
#'   `"32G"`), `gpu` (non-negative integer) and `tags` (character vector of
#'   capability labels, all of which the worker must advertise). The task is
#'   dispatched only to workers declaring matching or greater resources. `NULL`
#'   (default) means any worker is eligible.
#'
#' @return An object of class `task_object` with subclass `shell_task_object`.
#'
#' @section Shell interpreter:
#' The shell used to run scripts (`bash` by default) is configured at worker
#' level via
#' `new_worker(shell_cmd = ...)`, not at task level.
#'
#' @seealso [new_R_task()], [new_python_task()], [new_cmd()], [new_script()]
#'
#' @examples
#' task <- new_shell_task(
#'   task_id  = "fastqc",
#'   input  = new_input(new_file_exp("fastq/{sample}.fastq")),
#'   output = new_output("-o" = new_file("qc/{sample}_fastqc.html")),
#'   exec   = new_cmd("fastqc")
#' )
#'
#' @export
new_shell_task <- function(task_id,
                         input = NULL,
                         output = NULL,
                         exec,
                         requires = NULL){

  ## Re-tag with the internal class name wrapper_shell.R reads from
  ## class(func_code) to choose the script-vs-cmd branch.
  if(inherits(exec, "script")){
    class(exec) <- unique(c("sh_script", class(exec)))
  } else if(inherits(exec, "cmd")){
    class(exec) <- unique(c("sh_cmd", class(exec)))
  } else {
    stop(flumen_reject(
      call  = "new_shell_task()",
      error = "'exec' must be created with new_script() or new_cmd()",
      fix   = "exec = new_cmd(\"bwa mem ...\") or new_script(file = \"task.sh\")"),
      call. = FALSE)
  }
  validate_task_io("new_shell_task", task_id, input, output)

  op <- new_task(
    task_id = task_id,
    type = "shell",
    exec = exec,
    par_list = new_sh_params(input, output),
    exe_id = uuid::UUIDgenerate(),
    cmd_interpreter = "Rscript",
    shell_interpreter = "bash",
    requires = requires
  )
}

new_task <- function(task_id,
                   type = "R",
                   exec,
                   par_list,
                   exe_id,
                   cmd_interpreter = "Rscript",
                   shell_interpreter = "bash",
                   wildcards = NULL,
                   output_files = "",
                   output_auto_mkdir = TRUE,
                   output_par_names = "",
                   output_object_par_names = "",
                   requires = NULL){

  ## Checked here, before the internal logic that would otherwise reach it.
  if(missing(task_id) || is.null(task_id) || !is.character(task_id) ||
     length(task_id) != 1L || !nzchar(task_id)){
    stop(flumen_reject(
      call  = "new_task()",
      error = "'task_id' is required and must be a single non-empty string",
      fix   = "new_task(task_id = \"align\", ...)"), call. = FALSE)
  }

  if(missing(par_list) || !is.list(par_list) || length(par_list) == 0L){
    stop(flumen_reject(
      call  = "new_task()",
      task  = task_id,
      error = "the task declares neither input nor output",
      fix   = "declare at least one, new_input(...) or new_output(...)"), call. = FALSE)
  }

  validate_resources(requires,
                     context = sprintf("new_task(task_id = '%s'): 'requires'", task_id))

  par_ids <- seq_along(par_list)

  par_list <- lapply(par_ids, function(par_id){
    par <- par_list[[par_id]]
    par$par_id <- par_id
    return(par)
  })

  w_out_par <- which(sapply(par_list, function(par){
     "out_par_object" %in% class(par)
  }))

  dyn_wildcards <- sapply(par_list, function(par){
    par$dyn_wildcard
  })

  is_output_par <- rep(FALSE, length(par_list))
  is_output_par[w_out_par] <- TRUE

  class(par_list) <- "par_list_object"

  par_names <- sapply(par_list, function(par){
    par$name
  })

  par_uuids <- sapply(par_list, function(par){
    par$par_uuid
  })

  task_object <- list(
    task_id = task_id,
    exe_id = exe_id,
    type = type,
    exec = exec,
    cmd_interpreter = cmd_interpreter,
    shell_interpreter = shell_interpreter,
    par_list = new_par_list(
      par_list
    ),
    par_names = par_names,
    par_uuids = par_uuids,
    wildcards = wildcards,
    dyn_wildcards = dyn_wildcards,
    output_files = output_files,
    output_auto_mkdir = output_auto_mkdir,
    output_par_names = output_par_names,
    output_object_par_names = output_object_par_names,
    is_output_par = is_output_par,
    shape = exe_id,
    colour = rlang::hash(par_list),
    lineage_colour = rlang::hash(par_list),
    requires = requires
  )

  switch(type,
         shell={
           class <- "shell_task_object"
         },
         R={
           class <- "R_task_object"
         },
         python={
           class <- "python_task_object"
         }
  )

  class(task_object) <- c(class, "task_object")
  return(task_object)
}

new_task_list <- function(task_list){
  class(task_list) <- "task_list_object"
  return(task_list)
}


## -- script/cmd exec tags ----------------------------------------------------

#' Declare a script as a task's command
#'
#' Tags a character string of source code as the `exec` for a task
#' constructor. The language of the script is determined by the task type:
#' [new_R_task()] expects R, [new_python_task()] expects Python, and
#' [new_shell_task()] expects shell.
#'
#' For Python, common leading indentation is stripped automatically by
#' [new_python_task()] (Python is indentation-sensitive); R and shell scripts
#' are passed through verbatim.
#'
#' @section Placeholder syntax:
#' Inside the script, `{name}` refers to *task parameters*, the keys of
#' [new_input()] and [new_output()] for this task, substituted at execution
#' time with the resolved value. A [new_shell_task()] with
#' `input = new_input(threads = 8, reads = "...")` uses `{threads}` and
#' `{reads}` in its script body.
#'
#' The same `{name}` syntax denotes a *workflow wildcard* elsewhere, for
#' example `{sample}` inside a [new_file()] path. The two roles are
#' disambiguated by *location*, not by syntax:
#'
#' - inside path/value strings (in `new_input`/`new_output` values, or
#'   [new_file()]/[new_file_exp()]/[new_object()]), `{name}` is a workflow
#'   wildcard, resolved at workflow construction by the wildcard expansion
#'   pass;
#' - inside the script body passed to `new_script()` (i.e., `exec`),
#'   `{name}` is a task-parameter placeholder, resolved at task execution
#'   by the wrapper.
#'
#' Naming a parameter the same as a wildcard, for example a parameter `sample`
#' in a workflow that has a `{sample}` wildcard, is allowed but usually
#' unintended. Rename one of the two.
#'
#' @param code Character. Inline source code as a single string. Multi-line
#'   strings are supported. Mutually exclusive with `file`.
#' @param file Character. Path to a file containing the source code, resolved
#'   relative to the calling session's working directory. The contents are read
#'   at the time of this call, so later edits to the file have no effect.
#'   Mutually exclusive with `code`.
#'
#' @return A character vector with class `script`.
#'
#' @seealso [new_R_task()], [new_python_task()], [new_shell_task()], [new_cmd()],
#'   [new_input()], [new_output()], [new_file()]
#'
#' @examples
#' # Inline: R task, {x} is the parameter named 'x' in new_input(...)
#' new_script('square <- function(x) x^2')
#'
#' # Inline: shell task, {threads}, {reads}, {sam} are task parameters;
#' # {sample} (if present in a path) would be a workflow wildcard.
#' new_script('
#'   bowtie2 -p {threads} -x {genome} -U {reads} -S {sam}
#' ')
#'
#' \dontrun{
#' # File-based: keeps long shell/R/Python sources in their own
#' # syntax-highlighted file. The path is relative to getwd() and is read at
#' # construction time, so the file has to exist already.
#' new_script(file = "scripts/rsem_quant.sh")
#' }
#'
#' @export
new_script <- function(code = NULL, file = NULL){

  if(is.null(code) && is.null(file)){
    stop(flumen_reject(
      call  = "new_script()",
      error = "neither 'code' nor 'file' was given",
      fix   = "new_script(code = \"...\") or new_script(file = \"task.R\")"), call. = FALSE)
  }
  if(!is.null(code) && !is.null(file)){
    stop(flumen_reject(
      call  = "new_script()",
      error = "'code' and 'file' were both given",
      fix   = "pass exactly one of them"), call. = FALSE)
  }

  if(!is.null(file)){
    if(!is.character(file) || length(file) != 1L || !nzchar(file)){
      stop(flumen_reject(
        call  = "new_script()",
        error = "'file' must be a single non-empty path",
        fix   = "new_script(file = \"task.R\")"), call. = FALSE)
    }
    if(!file.exists(file)){
      stop(flumen_reject(
        call  = "new_script()",
        error = sprintf("file not found: '%s'", file),
        fix   = "check the path; it is read at construction time, on the master"),
        call. = FALSE)
    }
    code <- paste(readLines(file, warn = FALSE), collapse = "\n")
  }

  class(code) <- c("script", "character", "vector")
  return(code)
}

#' Declare a single shell command as a task's command
#'
#' Tags a single-token command (e.g., `"fastqc"`, `"bowtie2-build"`) as the
#' `exec` for [new_shell_task()]. Inputs/outputs are marshalled as CLI arguments.
#' Use [new_script()] instead for inline multi-line shell scripts.
#'
#' @param code Character. The executable name (must be on PATH) or full path.
#'
#' @return A character vector with class `cmd`.
#'
#' @seealso [new_shell_task()], [new_script()]
#'
#' @examples
#' new_cmd("fastqc")
#'
#' @export
new_cmd <- function(code){
  class(code) <- c("cmd", "character", "vector")
  return(code)
}
