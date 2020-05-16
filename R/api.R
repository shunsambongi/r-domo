new_api_result <- function(content, path, response) {
  structure(
    list(
      content = content,
      path = path,
      response = response
    ),
    class = "domo_api_result"
  )
}

#' @export
print.domo_api_result <- function(x, ...) {
  method <- x$response$request$method
  path <- x$path
  cat_line("<DOMO: {method} {path}>")
  str(x$content)
  invisible(x)
}

domo_api <- function(
  verb, path, ..., expected_type = "application/json", .envir = parent.frame()
) {
  path <- glue::glue(path, .envir = .envir)
  url <- httr::modify_url("https://api.domo.com", path = path)

  user_agent <- httr::user_agent("https://github.com/shunsambongi/domo")
  resp <- httr::VERB(verb, url = url, user_agent, ...)
  content <- parse_content(resp)
  validate_response(resp, content)
  validate_type(resp, expected_type, content)
  new_api_result(content = content, path = path, response = resp)
}

GET <- function(path, ..., .envir = parent.frame()) {
  domo_api(verb = "GET", path = path, ..., .envir = .envir)
}

validate_type <- function(response, expected_type, content) {
  if (httr::http_type(response) != expected_type) {
    rlang::abort(
      message = glue::glue("API did not return {expected_type}"),
      class = "domo_unexpected_type",
      response = response,
      content = content
    )
  }
  invisible(response)
}

validate_response <- function(response, content) {
  if (!httr::http_error(response)) {
    return(invisible(response))
  }

  status_code <- httr::status_code(response)
  description <- content$error_description %||%
    content$message %||%
    "<no description available>"
  msg <- glue::glue(
    "DOMO API request failed [{status_code}]",
    "{description}",
    .sep = "\n"
  )
  rlang::abort(
    message = msg,
    class = "domo_api_error",
    response = response,
    content = content
  )
}

parse_content <- function(response) {
  type <- httr::http_type(response)
  text <- httr::content(response, "text")
  switch(
    type,
    "application/json" = jsonlite::fromJSON(text, simplifyVector = FALSE),
    {
      rlang::abort(
        message = glue::glue("Cannot parse type {type}"),
        class = "domo_unparseable_type",
        response = response,
        content = text
      )
      text
    }
  )
}
