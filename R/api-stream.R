# https://developer.domo.com/docs/streams-api-reference/streams


# streams -----------------------------------------------------------------

retrieve_stream <- function(token, stream_id, ...) {
  GET("/v1/streams/{stream_id}", token)
}

create_stream <- function(
  token,
  name,
  schema,
  ...,
  description = "",
  update_method = c("REPLACE", "APPEND")
) {
  dataset <- dataset_body(name, description, schema)
  body <- list(dataSet = dataset, updateMethod = match.arg(update_method))
  POST(
    "/v1/streams",
    token,
    body = body,
    encode = "json"
  )
}

update_stream <- function(
  token, stream_id, ..., update_method = c("APPEND", "REPLACE")
) {
  body <- list(updateMethod = match.arg(update_method))
  PATCH("/v1/streams/{stream_id}", token, body = body, encode = "json")
}

delete_stream <- function(token, stream_id, ...) {
  DELETE(
    "/v1/streams/{stream_id}",
    token,
    expected_type = "application/octet-stream"
  )
}

list_streams <- function(token, ..., limit = NULL, offset = NULL) {
  query <- named_list(limit, offset)
  GET("/v1/streams", token, query = query)
}

search_streams <- function(token, datasource_id = NULL, owner_id = NULL, ...) {
  missing_datasource_id <- is.null(datasource_id)
  missing_owner_id <- is.null(owner_id)

  if (missing_datasource_id && missing_owner_id) {
    rlang::abort(
      "`datasource_id` or `owner_id` must be provided",
      class = "domo_search_streams_query_error"
    )
  }

  if (!missing_datasource_id && !missing_owner_id) {
    rlang::abort(
      "Only one of `datasource_id` and `owner_id` can be provided",
      class = "domo_search_streams_query_error"
    )
  }

  format_q <- function(prefix, value) {
    if (!is.null(value)) {
      paste(prefix, value, sep = ":")
    }
  }

  query <- list(
    q = format_q("dataSource.id", datasource_id),
    q = format_q("dataSource.owner.id", owner_id)
  )
  GET("/v1/streams/search", token, query = query)
}


# stream executions -------------------------------------------------------

retrieve_stream_execution <- function(token, stream_id, execution_id, ...) {
  GET("/v1/streams/{stream_id}/executions/{execution_id}", token)
}

create_stream_execution <- function(token, stream_id, ...) {
  POST("/v1/streams/{stream_id}/executions", token)
}

list_stream_executions <- function(
  token, stream_id, ..., limit = NULL, offset = NULL
) {
  query <- named_list(limit, offset)
  GET(
    "/v1/streams/{stream_id}/executions",
    token,
    query = query
  )
}

upload_data_part <- function(
  token, stream_id, execution_id, part_id, data, ...
) {
  body <- readr::format_csv(data, col_names = FALSE)
  PUT(
    "/v1/streams/{stream_id}/executions/{execution_id}/part/{part_id}",
    token,
    body = body,
    encode = "raw",
    httr::content_type("text/csv")
  )
}

commit_stream_execution <- function(token, stream_id, execution_id, ...) {
  PUT("/v1/streams/{stream_id}/executions/{execution_id}/commit", token)
}

abort_stream_execution <- function(token, stream_id, execution_id, ...) {
  PUT("/v1/streams/{stream_id}/executions/{execution_id}/abort", token)
}
