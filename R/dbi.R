# driver ------------------------------------------------------------------

#' Class DomoDriver (and methods)
#'
#' DomoDriver objects are created by [domo()], and are used to select the
#' correct method in [DBI::dbConnect()].
#' It is used purely for dispatch.
#'
#' @keywords internal
#' @export
setClass("DomoDriver", contains = "DBIDriver")

#' @rdname DomoDriver-class
#' @export
setMethod("dbUnloadDriver", "DomoDriver", function(drv, ...) {
  TRUE
})

#' @rdname DomoDriver-class
#' @export
setMethod("dbDataType", "DomoDriver", function(dbObj, obj, ...) {
  domo_data_type(obj)
})

#' @rdname DomoDriver-class
#' @export
setMethod("dbIsValid", "DomoDriver", function(dbObj, ...) {
  TRUE
})

#' Connect to a Domo instance
#'
#' `domo()` and [DBI::dbConnect()] allow you to connect to a Domo instance, to
#' read and write from Domo datasets.
#'
#' @export
#' @return
#'   * For `domo()`: An object of class [DomoDriver-class]
#'   * For `dbConnect()`: An object of class [DomoConnection-class]
domo <- function() {
  new("DomoDriver")
}

#' @param drv An object created by [domo()]
#' @param client_id OAuth 2.0 client ID as a string
#' @param client_secret OAuth 2.0 client secret as a string
#' @param scope A character vector of OAuth 2.0 scopes
#'
#' @export
#' @rdname domo
setMethod("dbConnect", "DomoDriver", function(
  drv,
  client_id = domo_client_id(),
  client_secret = domo_client_secret(),
  scope = "data"
) {
  token <- domo_token(client_id, client_secret, scope)
  new("DomoConnection", token = token)
})


# DomoConnection ----------------------------------------------------------

#' Class DomoConnection (and methods)
#'
#' DomoConnection objects are created by passing [domo()] as the first argument
#' to [DBI::dbConnect()].
#' They are a subclass of the [DBIConnection-class] class.
#'
#' @keywords internal
#' @export
setClass(
  "DomoConnection",
  contains = "DBIConnection",
  slots = list(
    token = "domo_token"
  )
)

#' @rdname DomoConnection-class
#' @export
setMethod("show", "DomoConnection", function(object) {
  cat_line("<DomoConnection>")
  if (DBI::dbIsValid(object)) {
    vctrs::obj_print_data(object@token)
  } else {
    cat_line("  DISCONNECTED")
  }
})

#' @rdname DomoConnection-class
#' @export
setMethod("dbDisconnect", "DomoConnection", function(conn, ...) {
  conn@token$content <- NULL
  invisible(TRUE)
})

#' @rdname DomoConnection-class
#' @export
setMethod("dbIsValid", "DomoConnection", function(dbObj, ...) {
  !is.null(dbObj@token$content)
})

#' @rdname DomoConnection-class
#' @export
setMethod("dbDataType", "DomoConnection", function(dbObj, obj, ...) {
  domo_data_type(obj)
})


# dbSendQuery -------------------------------------------------------------

#' @rdname DomoConnection-class
#' @export
setMethod("dbSendQuery", c("DomoConnection", "character"), function(
  conn, statement, dataset_id, ...
) {
  with_refresh(conn@token, {
    result <- query_dataset(
      conn@token, dataset_id = dataset_id, sql = statement
    )
  })
  result <- structure(list2env(result), class = class(result))
  new("DomoResult", result = result, ...)
})




# result ------------------------------------------------------------------

#' Domo results class
#'
#' @keywords internal
#' @export
setClass(
  "DomoResult",
  contains = "DBIResult",
  slots = list(
    result = "domo_api_result"
  )
)

#' @rdname DomoResult-class
#' @export
setMethod("dbClearResult", "DomoResult", function(res, ...) {
  TRUE
})

#' @rdname DomoResult-class
#' @export
setMethod("dbFetch", "DomoResult", function(res, n = -1, ...) {
  content <- res@result$content
  columns <- content$columns
  rows <- content$rows
  len <- length(rows)

  if (n == 0 || !len) {
    out <- r_data_type(map_chr(content$metadata, function(x) x$type))
    out <- rlang::set_names(out, columns)
    return(dplyr::as_tibble(out))
  }

  if (n < -1) {
    rlang::abort("Invalid `n`")
  }

  if (n == -1) {
    n <- Inf
  }

  n <- min(n, len)

  if (n == len) {
    res@result$content$rows <- list()
  } else {
    res@result$content$rows <- rows[(n + 1):len]
  }

  rows <- map(rows[1:n], function(row) {
    row <- rlang::set_names(row, columns)
    dplyr::as_tibble(row)
  })

  vctrs::vec_rbind(!!!rows)
})

#' @rdname DomoResult-class
#' @export
setMethod("dbHasCompleted", "DomoResult", function(res, ...) {
  !length(res@result$content$rows)
})
