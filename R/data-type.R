# types -------------------------------------------------------------------

domo_data_type  <- function(x) {
  if (is.character(x) | is.factor(x) | is.logical(x)) {
    "STRING"
  } else if (is_datetime(x)) {
    "DATETIME"
  } else if (is_date(x)) {
    "DATE"
  } else if (is.integer(x)) {
    "LONG"
  } else if (is.numeric(x)) {
    "DOUBLE"
  } else {
    cls <- paste(class(x), collapse = "/")
    msg <- paste0("Invalid data type: ", cls)
    stop(msg, call. = FALSE)
  }
}

r_data_type <- function(x) {
  mapping <- list(
    STRING = character(),
    LONG = integer(),
    DOUBLE = double(),
    DATE = vctrs::new_date(),
    DATETIME = vctrs::new_datetime()
  )

  mapping[x]
}

is_date <- function(x) {
  is(x, "Date")
}

is_datetime <- function(x) {
  inherits(x, "POSIXt")
}


# fields ------------------------------------------------------------------

domo_fields <- function(x, ...) {
  UseMethod("domo_fields")
}

domo_fields.data.frame <- function(x, ...) {
  map_chr(x, domo_data_type)
}

domo_fields.DomoConnection <- function(x, dataset_id, ...) {
  result <- with_refresh(x@token, retrieve_dataset(x@token, dataset_id))
  cols <- result$content$schema$columns
  out <- map_chr(cols, function(col) col$type)
  names(out) <- map_chr(cols, function(col) col$name)
  out
}


# schema ------------------------------------------------------------------

dataset_schema <- function(names, types) {
  choices <- c("STRING", "DATETIME", "DATE", "LONG", "DOUBLE")
  assert_that(
    !anyDuplicated(names),
    is.character(names),
    is.character(types),
    all(types %in% choices)
  )
  types <- vctrs::vec_recycle(types, size = vctrs::vec_size(names))
  columns <- map2(names, types, function(name, type) named_list(name, type))
  structure(named_list(columns), class = "domo_dataset_schema")
}

print.domo_dataset_schema <- function(x, ...) {
  print(jsonlite::toJSON(unclass(x), pretty = TRUE, auto_unbox = TRUE))
  invisible(x)
}

as_dataset_schema <- function(x) {
  UseMethod("as_dataset_schema")
}

as_dataset_schema.data.frame <- function(x) {
  fields <- domo_fields(x)
  dataset_schema(names(fields), unname(fields))
}

as_dataset_schema.character <- function(x) {
  assert_that(rlang::is_named(x))
  dataset_schema(names(x), unname(x))
}

as_dataset_schema.NULL <- function(x) {
  NULL
}
