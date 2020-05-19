retrieve_dataset <- function(token, dataset_id, ...) {
  GET("/v1/datasets/{dataset_id}", token)
}

create_dataset <- function(token, name, schema, description = "", ...) {
  body <- dataset_body(name, description, schema)
  POST(
    "/v1/datasets",
    token,
    body = body,
    encode = "json"
  )
}

delete_dataset <- function(token, dataset_id, ...) {
  DELETE(
    "/v1/datasets/{dataset_id}",
    token,
    expected_type = "application/octet-stream"
  )
}

query_dataset <- function(token, dataset_id, sql, ...) {
  body <- named_list(sql)
  POST(
    "/v1/datasets/query/execute/{dataset_id}",
    token, body = body, encode = "json"
  )
}

list_datasets <- function(token, ..., sort = NULL, limit = NULL, offset = NULL) {
  ellipsis::check_dots_empty()
  query <- named_list(sort, limit, offset)
  GET("/v1/datasets", token, query = query)
}

export_dataset <- function(token, dataset_id, ...) {
  query = named_list(
    includeHeader = TRUE,
    fileName = paste0("domo-dataset-", dataset_id, ".csv")
  )
  GET(
    "/v1/datasets/{dataset_id}/data",
    query = query,
    expected_type = "text/csv",
    token
  )
}

import_dataset <- function(
  token, dataset_id, data, ..., update_method = c("REPLACE", "APPEND")
) {
  query <- list(updateMethod = match.arg(update_method))
  body <- readr::format_csv(data, col_names = FALSE)
  PUT(
    "/v1/datasets/{dataset_id}/data",
    query = query,
    body = body,
    encode = "raw",
    token,
    httr::content_type("text/csv"),
    expected_type = "application/octet-stream"
  )
}


# helpers -----------------------------------------------------------------

dataset_body <- function(name, description, schema) {
  if (is.data.frame(schema)) {
    schema <- domo::schema(schema)
  }
  named_list(name, description, schema = list(columns = schema))
}
