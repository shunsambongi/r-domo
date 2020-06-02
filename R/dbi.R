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
