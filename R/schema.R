
schema <- function(data) {
  unname(imap(as.list(data), function(col, name) {
    list(name = name, type = domo_data_type(col))
  }))
}

domo_data_type  <- function(x) {
  if (is.character(x) | is.factor(x)) {
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
    "STRING"
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

