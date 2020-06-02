

map <- function(.x, .f, ...) {
  lapply(.x, .f, ...)
}

map_mold <- function(.x, .f, .mold, ...) {
  out <- vapply(.x, .f, .mold, ..., USE.NAMES = FALSE)
  names(out) <- names(.x)
  out
}

map_chr <- function(.x, .f, ...) {
  map_mold(.x, .f, character(1), ...)
}

map_lgl <- function(.x, .f, ...) {
  map_mold(.x, .f, logical(1), ...)
}

map2 <- function(.x, .y, .f, ...) {
  out <- mapply(.f, .x, .y, MoreArgs = list(...), SIMPLIFY = FALSE)
  if (length(out) == length(.x)) {
    rlang::set_names(out, names(.x))
  } else {
    rlang::set_names(out, NULL)
  }
}

probe <- function(.x, .p, ...) {
  if (is.logical(.p)) {
    stopifnot(length(.p) == length(.x))
    .p
  } else {
    map_lgl(.x, .p, ...)
  }
}

map_if <- function(.x, .p, .f, ...) {
  matches <- probe(.x, .p)
  .x[matches] <- map(.x[matches], .f, ...)
  .x
}

imap <- function(.x, .f, ...) {
  index <- names(.x) %||% seq_along(.x)
  map2(.x, index, .f, ...)
}

iwalk <- function(.x, .f, ...) {
  imap(.x, .f, ...)
  invisible(.x)
}

compact <- function(.x) {
  Filter(length, .x)
}

transpose <- function (.l) {
  inner_names <- names(.l[[1]])
  if (is.null(inner_names)) {
    fields <- seq_along(.l[[1]])
  }
  else {
    fields <- rlang::set_names(inner_names)
  }
  map(fields, function(i) {
    map(.l, .subset2, i)
  })
}
