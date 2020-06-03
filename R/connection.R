#' Domo Connection Methods
#'
#' Implementations of pure virtual functions defined in the `DBI` package
#' for DomoConnection objects.
#' @name DomoConnection
NULL

#' @rdname DomoConnection
#' @export
setClass(
  "DomoConnection",
  contains = "DBIConnection",
  slots = list(
    token = "domo_token"
  )
)

#' @rdname DomoConnection
#' @inheritParams methods::show
#' @export
setMethod(
  "show", "DomoConnection",
  function(object) {
    cat_line("<DomoConnection>")
    if (DBI::dbIsValid(object)) {
      vctrs::obj_print_data(object@token)
    } else {
      cat_line("  DISCONNECTED")
    }
  }
)

#' @rdname DomoConnection
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "DomoConnection",
  function(dbObj, ...) {
    !is.null(dbObj@token$content)
  }
)

#' @rdname DomoConnection
#' @inheritParams DBI::dbDisconnect
#' @export
setMethod(
  "dbDisconnect", "DomoConnection",
  function(conn, ...) {
    conn@token$content <- NULL
    invisible(TRUE)
  }
)

domo_send_query <- function(conn, statement, dataset_id, ...) {
  ellipsis::check_dots_empty()
  rs <- domo_result_set(conn, statement, dataset_id)

  peek <- domo_fetch_data(rs, limit = 1)
  if (!nrow(peek)) {
    rs$complete <- TRUE
  }

  new("DomoResult", result_set = rs)
}

#' Query Domo dataset
#'
#' @inheritParams DBI::dbSendQuery
#' @param dataset_id The dataset ID
#'   (e.g. `"77194824-a5c1-11ea-9600-0116159276e2"`).
#' @export
setMethod("dbSendQuery", c("DomoConnection", "character"), domo_send_query)


#' @rdname DomoConnection
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "DomoConnection",
  function(dbObj, obj, ...) {
    domo_data_type(obj)
  }
)

domo_data_type_df <- function(dbObj, obj, ...) {
  ellipsis::check_dots_empty()
  domo_fields(obj)
}

#' @rdname DomoConnection
#' @inheritParams DBI::dbDataType
#' @export
setMethod("dbDataType", c("DomoConnection", "data.frame"), domo_data_type_df)

#' @rdname DomoConnection
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "DomoConnection",
  function(dbObj, ...) {
    if (!DBI::dbIsValid(dbObj)) {
      stop("Invalid connection", call. = FALSE)
    }
    content <- dbObj@token$content
    list(
      db.version = NA,
      dbname = NA,
      username = content$userId,
      host = NA,
      port = NA,
      scope = content$scope,
      customer = content$customer,
      env = content$env,
      role = content$role,
      domain = content$domain
    )
  }
)
