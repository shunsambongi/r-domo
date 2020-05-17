
`%||%` <- function(x, y) {
  if (is.null(x)) x <- y
  x
}

cat_line <- function(..., .envir = parent.frame()) {
  out <- glue::glue(paste0(..., collapse = "\n"), .envir = .envir)
  cat(out, "\n", sep = "")
}

named_list <- function(...) {
  values <- rlang::list2(...)
  exprs <- rlang::exprs(...)
  names <- rlang::names2(values)
  names[names == ""] <- map(exprs, deparse)[names == ""]
  rlang::set_names(values, names)
}
