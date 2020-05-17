
# token -------------------------------------------------------------------

#' @export
domo_token <- function(
  client_id = domo_client_id(),
  client_secret = domo_client_secret(),
  scope = "data"
) {
  result <- GET(
    path = "/oauth/token",
    query = list(
      grant_type = "client_credentials",
      scope = paste(scope, collapse = " ")
    ),
    httr::authenticate(client_id, client_secret)
  )

  out <- list2env(result)
  structure(out, class = c("domo_token", class(result)))
}

setOldClass(c("domo_token", "domo_api_result"))

#' @export
print.domo_token <- function(x, ...) {
  vctrs::obj_print(x, ...)
  invisible(x)
}

#' @export
obj_print_header.domo_token <- function(x, ...) {
  cat_line("<DOMO TOKEN>")
  invisible(x)
}

#' @export
obj_print_data.domo_token <- function(x, ...) {
  domain <- x$content$domain
  userId <- x$content$userId
  scope <- x$content$scope
  cat_line("  domain: {domain}")
  cat_line("  userId: {userId}")
  cat_line("  scope: {scope}")
}

#' @export
as.character.domo_token <- function(x, ...) {
  x$content$access_token
}

is_token <- function(x) {
  inherits(x, "domo_token")
}

as_header <- function(token) {
  httr::add_headers(
    Authorization = paste("Bearer", as.character(token))
  )
}

# credentials -------------------------------------------------------------

domo_client_id <- function() {
  domo_client_credential("DOMO_CLIENT_ID", "client ID")
}

domo_client_secret <- function() {
  domo_client_credential("DOMO_CLIENT_SECRET", "client secret")
}

domo_client_credential <- function(envvar, desc) {
  out <- Sys.getenv(envvar)
  if (!identical(out, "")) {
    return(out)
  }

  msg <- glue::glue(
    "Missing credentials",
    "Please set the environment variable {envvar} to your {desc}",
    "<https://developer.domo.com/manage-clients>",
    .sep = "\n"
  )
  rlang::abort(
    message = msg,
    class = "domo_missing_credentials"
  )
}
