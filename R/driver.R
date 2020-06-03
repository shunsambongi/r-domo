#' Domo Driver Methods
#'
#' Implementation of pure virtual functions defined in the `DBI` package for
#' DomoDriver objects.
#' @name DomoDriver
NULL

#' Domo driver
#'
#' Driver for Domo
#'
#' @export
#' @examples
#' \dontrun{
#' # library(DBI)
#' domo::domo()
#' }
domo <- function() {
  new("DomoDriver")
}

#' @rdname DomoDriver
#' @export
setClass("DomoDriver", contains = "DBIDriver")

#' @rdname DomoDriver
#' @inheritParams methods::show
#' @export
setMethod("show", "DomoDriver", function(object) {
  cat("<DomoDriver>\n")
})

domo_connect <- function(
  drv,
  client_id = domo_client_id(),
  client_secret = domo_client_secret(),
  ...
) {
  ellipsis::check_dots_empty()
  token <- domo_token(client_id, client_secret, scope = "data")
  new("DomoConnection", token = token)
}

#' Connect to a Domo instance
#'
#' @inheritParams ellipsis::dots_empty
#' @inheritParams DBI::dbConnect
#' @param client_id OAuth 2.0 client ID as a string.
#' @param client_secret OAuth 2.0 client secret as a string.
#'
#' @details
#' By default the `client_id` and `client_secret` are read in from environment
#' variables `DOMO_CLIENT_ID` and `DOMO_CLIENT_SECRET`, respectively. To obtain
#' the credentials, login to your Domo instance and visit
#' [Domo's developer portal](https://developer.domo.com/new-client). The client
#' must have the "Data" application scope.
#'
#' @export
setMethod("dbConnect", "DomoDriver", domo_connect)

#' @rdname DomoDriver
#' @inheritParams DBI::dbDataType
#' @export
setMethod(
  "dbDataType", "DomoDriver",
  function(dbObj, obj, ...) {
    domo_data_type(obj)
  }
)


#' @rdname DomoDriver
#' @inheritParams DBI::dbDataType
#' @export
setMethod("dbDataType", c("DomoDriver", "data.frame"), domo_data_type_df)

#' @rdname DomoDriver
#' @inheritParams DBI::dbIsValid
#' @export
setMethod(
  "dbIsValid", "DomoDriver",
  function(dbObj, ...) {
    TRUE
  }
)

#' @rdname DomoDriver
#' @inheritParams DBI::dbGetInfo
#' @export
setMethod(
  "dbGetInfo", "DomoDriver",
  function(dbObj, ...) {
    version <- utils::packageVersion("domo")
    list(
      driver.version = version,
      client.version = version
    )
  }
)
