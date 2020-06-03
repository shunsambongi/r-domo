# domo_result_set ---------------------------------------------------------

domo_result_set <- function(con, sql, dataset_id, params = NULL) {
  fields <- list(
    con = con,
    sql = sql,
    dataset_id = dataset_id,
    params = params,
    fetched = 0L,
    complete = FALSE,
    cleared = FALSE
  )
  out <- list2env(fields, parent = emptyenv())
  structure(out, class = "domo_result_set")
}

methods::setOldClass("domo_result_set")

validate_domo_result_set <- function(rs) {
  if (rs$cleared) {
    rlang::abort("Invalid result set", class = "domo_invalid_result_set")
  }
  invisible(rs)
}

#' @export
print.domo_result_set <- function(x, ...) {
  vctrs::obj_print(x, ...)
  invisible(x)
}

# zzz.R
obj_print_header.domo_result_set <- function(x, ...) {
  cat("<DOMO RESULT SET>\n")
  invisible(x)
}

# zzz.R
obj_print_data.domo_result_set <- function(x, ...) {
  status <- if (x$complete) "complete" else "incomplete"
  cat("  SQL  ", x$sql, "\n", sep = "")
  cat("  ROWS Fetched: ", x$fetched, " [", status, "]\n", sep = "")
  invisible(x)
}

# zzz.R
obj_print_footer.domo_result_set <- function(x, ...) {
  invisible(x)
}

# DomoResult --------------------------------------------------------------

#' Domo Result Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package for
#' DomoResult objects.
#' @name DomoResult
#' @docType methods
#' @examples
#' \dontrun{
#' library(DBI)
#' con <- dbConnect(domo::domo())
#' dataset_id <- "77194824-a5c1-11ea-9600-0116159276e2"
#' res <- dbSendQuery(con, "SELECT mpg FROM mtcars WHERE mpg > ?", dataset_id)
#' dbBind(res, list(20))
#' dbHasCompleted(res)
#' dbGetRowCount(res)
#'
#' dbFetch(res, n = 1)
#' dbHasCompleted(res)
#' dbGetRowCount(res)
#'
#' dbFetch(res)
#' dbHasCompleted(res)
#' dbGetRowCount(res)
#'
#' dbClearResult(res)
#' dbDisconnect(con)
#' }
NULL

#' @rdname DomoResult
#' @export
setClass(
  "DomoResult",
  contains = "DBIResult",
  slots = list(
    result_set = "domo_result_set"
  )
)

#' @rdname DomoResult
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "DomoResult",
  function(object) {
    cat("<DomoResult>\n")
    if (object@result_set$cleared) {
      cat("EXPIRED\n")
    } else {
      vctrs::obj_print_data(object@result_set)
    }
  }
)


# dbIsValid ---------------------------------------------------------------

domo_result_is_valid <- function(dbObj, ...) {
  ellipsis::check_dots_empty()
  !dbObj@result_set$cleared
}

#' @rdname DomoResult
#' @inheritParams DBI::dbIsValid
#' @export
setMethod("dbIsValid", "DomoResult", domo_result_is_valid)


# dbBind ------------------------------------------------------------------

domo_bind <- function(res, params, ...) {
  ellipsis::check_dots_empty()
  rs <- validate_domo_result_set(res@result_set)
  assert_that(is.list(params))
  rs$fetched <- 0
  rs$params <- params
  rs$complete <- FALSE
  invisible(res)
}

#' @rdname DomoResult
#' @inheritParams DBI::dbBind
#' @export
setMethod("dbBind", "DomoResult", domo_bind)


# dbFetch -----------------------------------------------------------------

domo_fetch <- function(res, n = -1, ...) {
  rs <- validate_domo_result_set(res@result_set)
  msg <- glue::glue("`n` must be a scalar integer >= -1L or Inf")
  assert_that(
    !is.na(n), rlang::is_integerish(n, n = 1L), n >= -1, msg = msg
  )
  if (n == -1) {
    n <- Inf
  }

  # fetch an extra row to check if there is more data after this query
  limit <- n + 1
  out <- domo_fetch_data(rs, limit = limit, offset = rs$fetched)

  if (limit == Inf) {
    rs$complete <- TRUE
  } else if (nrow(out) == limit) { # remove the extra row if it exists
    out <- utils::head(out, -1)
  } else {
    rs$complete <- TRUE
  }
  rs$fetched <- rs$fetched + nrow(out)

  out
}

domo_fetch_data <- function(rs, limit, offset = rs$fetched) {
  sql <- domo_fetch_sql(rs, limit, offset)

  token <- rs$con@token
  result <- with_refresh(token, query_dataset(token, rs$dataset_id, sql))
  content <- result$content

  cols <- content$columns
  if (content$numRows == 0) {
    out <- r_data_type(map_chr(content$metadata, function(x) x$type))
    out <- rlang::set_names(out, cols)
    return(dplyr::as_tibble(out))
  }

  rows <- map(content$rows, rlang::set_names, cols)
  dplyr::bind_rows(rows)
}

domo_fetch_sql <- function(rs, limit, offset) {
  assert_that(limit > 0, offset >= 0)
  limit <- if (limit == Inf) NULL else as.integer(limit)
  offset <- as.integer(offset)

  con <- rs$con
  sql <- DBI::sqlInterpolate(con, rs$sql, .dots = rs$params)

  if (is.null(limit) && offset == 0L) {
    return(sql)
  }

  query <- compact(list(
    dplyr::sql_select(
      con = con,
      select = dbplyr::sql("*"),
      from = dplyr::sql_subquery(con, dbplyr::sql(sql)),
      limit = limit
    ),
    if (offset > 0L) dbplyr::build_sql("OFFSET ", offset, con = con)
  ))
  dbplyr::escape(query, parens = FALSE, collapse = "\n", con = con)
}

#' @rdname DomoResult
#' @inheritParams DBI::dbFetch
#' @export
setMethod("dbFetch", "DomoResult", domo_fetch)


# dbGetRowCount -----------------------------------------------------------

domo_get_row_count <- function(res, ...) {
  ellipsis::check_dots_empty()
  rs <- validate_domo_result_set(res@result_set)
  rs$fetched
}

#' @rdname DomoResult
#' @inheritParams DBI::dbGetRowCount
#' @export
setMethod("dbGetRowCount", "DomoResult", domo_get_row_count)


# dbGetRowsAffected -------------------------------------------------------

#' @rdname DomoResult
#' @inheritParams DBI::dbGetRowsAffected
#' @export
setMethod("dbGetRowsAffected", "DomoResult", function(res, ...) 0)


# dbGetStatement ----------------------------------------------------------

domo_get_statement <- function(res, ...) {
  ellipsis::check_dots_empty()
  rs <- validate_domo_result_set(res@result_set)
  rs$sql
}

#' @rdname DomoResult
#' @inheritParams DBI::dbGetStatement
#' @export
setMethod("dbGetStatement", "DomoResult", domo_get_statement)


# dbColumnInfo ------------------------------------------------------------

domo_column_info <- function(res, ...) {
  ellipsis::check_dots_empty()
  rs <- validate_domo_result_set(res@result_set)
  data <- domo_fetch_data(rs, 1, 0)
  name <- names(data)
  type <- unname(map_chr(data, function(x) class(x)[1]))
  type[type == "numeric"] <- "double"
  dplyr::tibble(name, type)
}

#' @rdname DomoResult
#' @inheritParams DBI::dbColumnInfo
#' @export
setMethod("dbColumnInfo", "DomoResult", domo_column_info)


# dbClearResult -----------------------------------------------------------

domo_clear_result <- function(res, ...) {
  ellipsis::check_dots_empty()
  rs <- res@result_set
  if (rs$cleared) {
    warning("Expired, result set already closed", call. = FALSE)
  } else {
    vars <- setdiff(ls(rs), "cleared")
    rm(list = vars, envir = rs)
    rs$cleared <- TRUE
  }
  invisible(TRUE)
}

#' @rdname DomoResult
#' @inheritParams DBI::dbClearResult
#' @export
setMethod("dbClearResult", "DomoResult", domo_clear_result)


# dbHasCompleted ----------------------------------------------------------

domo_has_completed <- function(res, ...) {
  ellipsis::check_dots_empty()
  rs <- validate_domo_result_set(res@result_set)
  rs$complete
}

#' @rdname DomoResult
#' @inheritParams DBI::dbHasCompleted
#' @export
setMethod("dbHasCompleted", "DomoResult", domo_has_completed)
