
`%||%` <- function(x, y) {
  if (is.null(x)) x <- y
  x
}

cat_line <- function(..., .envir = parent.frame()) {
  out <- glue::glue(paste0(..., collapse = "\n"), .envir = .envir)
  cat(out, "\n", sep = "")
}
