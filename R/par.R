## Parameter object constructors and helpers.
##
## Atomic par_object factories (R/python/shell × input/output) plus the
## helpers that turn user-facing new_input()/new_output() declarations into
## flat lists of par_objects consumed by new_task().


## -- atomic par_object factories ---------------------------------------------

## Set the S3 class hierarchy on a freshly built par_object.
## Centralises the c("<lang>_<direction>_par_object", "<direction>_par_object",
## "par_object") sequence so all atomic constructors stay consistent and a
## new language only needs a 1-liner constructor below.
##   lang:      "R" | "python" | "sh"
##   direction: "in" | "out"
assign_par_class <- function(par_object, lang, direction) {
  class(par_object) <- c(
    paste0(lang, "_", direction, "_par_object"),
    paste0(direction, "_par_object"),
    "par_object"
  )
  par_object
}

## Build one par_object and stamp its class. The six atomic constructors below
## differ ONLY in the (lang, direction) pair they stamp, so the shape lives here
## once: a field added to a par_object is added in a single place instead of six,
## and the six cannot drift apart. They are kept as named functions because
## process_param() receives one of them as a value (`do.call(fn, args)`), so the
## name IS the selector.
## Not named `new_par`: that identifier is already a local variable throughout
## the assemblers below.
new_par_object <- function(lang,
                           direction,
                           par_id = 0,
                           name = "",
                           value = "",
                           dyn_wildcard){

  par_object <- list(
    par_id = par_id,
    name = name,
    value = value,
    par_uuid = uuid::UUIDgenerate(),
    dyn_wildcard = dyn_wildcard
  )

  assign_par_class(par_object, lang, direction)
}

new_R_in_par <- function(par_id = 0,
                         name = "",
                         value = "",
                         dyn_wildcard){
  new_par_object("R", "in", par_id = par_id, name = name,
                 value = value, dyn_wildcard = dyn_wildcard)
}

new_R_out_par <- function(par_id = 0,
                          name = "",
                          value = "",
                          dyn_wildcard){
  new_par_object("R", "out", par_id = par_id, name = name,
                 value = value, dyn_wildcard = dyn_wildcard)
}

new_python_in_par <- function(par_id = 0,
                              name = "",
                              value = "",
                              dyn_wildcard){
  new_par_object("python", "in", par_id = par_id, name = name,
                 value = value, dyn_wildcard = dyn_wildcard)
}

new_python_out_par <- function(par_id = 0,
                               name = "",
                               value = "",
                               dyn_wildcard){
  new_par_object("python", "out", par_id = par_id, name = name,
                 value = value, dyn_wildcard = dyn_wildcard)
}

new_sh_in_par <- function(par_id = 0,
                          name = "",
                          value = "",
                          dyn_wildcard){
  new_par_object("sh", "in", par_id = par_id, name = name,
                 value = value, dyn_wildcard = dyn_wildcard)
}

new_sh_out_par <- function(par_id = 0,
                           name = "",
                           value = "",
                           dyn_wildcard){
  new_par_object("sh", "out", par_id = par_id, name = name,
                 value = value, dyn_wildcard = dyn_wildcard)
}

new_par_list <- function(par_list_object){
  class(par_list_object) <- "par_list_object"
  return(par_list_object)
}


## -- per-language assemblers from input_object/output_object ------------------

process_param <- function(fn, name, value, dyn_wildcard) {
  args <- list(value = value)
  if (!is.null(name) && name != "") {
    args$name <- name
  }
  args$dyn_wildcard = dyn_wildcard
  result <- do.call(fn, args)
  return(result)
}

## Flatten the user's new_input() / new_output() declarations into the flat list
## of par_objects that new_task() consumes. The three per-language assemblers
## below differ ONLY in the pair of atomic constructors they stamp and in whether
## a value that is neither an input_object nor an output_object is refused, so
## the traversal lives here once rather than three times over.
##   in_par, out_par: this language's atomic constructors. Passed as VALUES,
##                    because process_param() calls them through do.call(), so
##                    the name of the function IS the selector.
##   refuse_unknown:  the calling assembler's name, used in the error message, or
##                    NULL to skip an unexpected class in silence. Only the shell
##                    assembler refuses; R and Python skip, and that difference is
##                    preserved rather than unified.
## A NULL entry is skipped before the class tests, which is what makes the silent
## and the refusing behaviour differ only on a genuinely unexpected class.
build_params <- function(params, in_par, out_par, refuse_unknown = NULL) {

  final_params <- list()

  for (par in params) {

    if (is.null(par)) next

    if (inherits(par, "input_object")) {

      for (j in seq_along(par)) {

        inner_name  <- names(par)[j]
        inner_value <- par[[j]]

        ## A dep_list is N dependencies declared under ONE parameter name: each
        ## becomes its own par_object and they all carry that same name. Any other
        ## value is a single par_object, wrapped here so both take one path.
        values <- if (inherits(inner_value, "dep_list")) inner_value else list(inner_value)

        for (value in values) {
          new_par <- process_param(in_par, inner_name, value,
                                   !is.null(find_dynamic_wildcards(value)))
          final_params <- c(final_params, list(new_par))
        }
      }

    } else if (inherits(par, "output_object")) {

      for (j in seq_along(par)) {

        new_par <- process_param(out_par, names(par)[j], par[[j]],
                                 !is.null(find_dynamic_wildcards(par[[j]])))
        final_params <- c(final_params, list(new_par))
      }

    } else if (!is.null(refuse_unknown)) {

      stop(refuse_unknown, ": expected 'input_object' or 'output_object' but got class '",
           paste(class(par), collapse = ", "), "'.", call. = FALSE)
    }
  }

  return(final_params)
}

new_R_params <- function(...){
  build_params(list(...), new_R_in_par, new_R_out_par)
}

new_python_params <- function(...){
  build_params(list(...), new_python_in_par, new_python_out_par)
}

new_sh_params <- function(...){
  build_params(list(...), new_sh_in_par, new_sh_out_par,
               refuse_unknown = "new_sh_params()")
}
