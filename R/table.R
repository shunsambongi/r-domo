#' Convenience functions for reading/writing Domo datasets
#'
#' @param name
#' * For `dbCreateTable`: the new dataset's name
#' * For everything else: an existing dataset's ID
#' @param temporary Must be `FALSE`. Domo cannot create temporary datasets.
#' @examples
#' \dontrun{
#' library(DBI)
#' con <- dbConnect(domo::domo())
#' dbListTables(con)
#' dataset_id <- dbCreateTable(con, "mtcars", mtcars)
#' dbWriteTable(con, dataset_id, mtcars, overwrite = TRUE)
#' dbReadTable(con, dataset_id)
#'
#' dbExistsTable(con, dataset_id)
#'
#' dbDisconnect(con)
#' }
#'
#' @name domo-tables
NULL


# dbCreateTable -----------------------------------------------------------

domo_create_table <- function(
  conn, name, fields, ..., row.names = NULL, temporary = FALSE
) {
  assert_that(is.null(row.names), isFALSE(temporary))
  token <- conn@token
  result <- with_refresh(token, create_stream(token, name, fields))
  id <- result$content$dataSet$id
  message(glue::glue("Created dataset {id}"))
  invisible(id)
}

#' @rdname domo-tables
#' @inheritParams DBI::dbCreateTable
#' @export
setMethod("dbCreateTable", "DomoConnection", domo_create_table)


# dbWriteTable ------------------------------------------------------------

domo_prepare_data <- function(data) {
  old <- options(stringsAsFactors = FALSE)
  on.exit(do.call(options, old), add = TRUE)
  as.data.frame(data)
}

domo_check_dataset_exists <- function(con, dataset_id) {
  if (DBI::dbExistsTable(con, dataset_id)) {
    return(invisible(TRUE))
  }
  msg <- glue::glue(
    "The dataset \"{name}\" does not exist.",
    "* Please ensure you are using the dataset ID, not the name.",
    "* To create a dataset, use `DBI::dbCreateTable()`.",
    .sep = "\n"
  )
  rlang::abort(msg)
}

domo_choose_update_method <- function(overwrite, append) {
  same <- (overwrite && append) || !(overwrite || append)
  if (same) {
    msg <- paste0(
      "Both `overwrite` and `append` are ", as.character(overwrite), "."
    )
    rlang::abort(msg)
  }
  if (overwrite) "REPLACE" else "APPEND"
}

domo_upload_stream <- function(con, dataset_id, data, update_method) {
  token <- con@token
  with_refresh(token, {
    result <- search_streams(token, dataset_id = dataset_id)
  })
  if (!length(result$content)) {
    rlang::abort(
      glue::glue(
        "Could not find stream for dataset {name}. ",
        "Only datasets created using the stream API can use streaming."
      )
    )
  } else if (length(result$content) > 1) {
    rlang::abort(glue::glue("Found multiple streams for dataset {name}"))
  }

  stream <- result$content[[1L]]
  stream_id <- stream$id

  if (update_method != stream$updateMethod) {
    with_refresh(token, {
      update_stream(token, stream_id, update_method = update_method)
    })
  }

  with_refresh(token, {
    result <- create_stream_execution(token, stream_id)
  })
  exec_id <- result$content$id

  parts <- split_data_parts(data)

  withCallingHandlers({
    with_refresh(token, {
      iwalk(parts, function(part, part_id) {
        upload_data_part(token, stream_id, exec_id, part_id, part)
      })
      commit_stream_execution(token, stream_id, exec_id)
    })
  }, error = function(e) {
    with_refresh(token, {
      abort_stream_execution(token, stream_id, exec_id)
    })
  })

  invisible(TRUE)
}

domo_upload_dataset <- function(con, dataset_id, data, update_method) {
  with_refresh(con@token, {
    import_dataset(con@token, dataset_id, data, update_method = update_method)
  })
  invisible(TRUE)
}

domo_write_table <- function(
  conn,
  name,
  value,
  ...,
  overwrite = FALSE,
  append = FALSE,
  stream = TRUE
) {
  ellipsis::check_dots_empty()
  data <- domo_prepare_data(value)
  assert_that(
    is.string(name),
    is.data.frame(data),
    is.flag(stream),
    is.flag(overwrite),
    is.flag(append)
  )
  domo_check_dataset_exists(conn, dataset_id = name)
  update_method <- domo_choose_update_method(overwrite, append)

  if (stream) {
    domo_upload_stream(conn, name, data, update_method)
  } else {
    domo_upload_dataset(conn, name, data, update_method)
  }
}


#' @rdname domo-tables
#' @inheritParams DBI::dbWriteTable
#' @param overwrite Allow overwriting the destination dataset. Cannot be `TRUE`
#'   if `append` is also `TRUE`.
#' @param append Allow appending to the destination table. Cannot be `TRUE`
#'   if `overwrite` is also `TRUE`.
#' @param stream If `TRUE` use
#'   [Stream API](https://developer.domo.com/docs/streams-api-reference/streams),
#'   otherwise use [Dataset API](https://developer.domo.com/docs/dataset-api-reference/dataset).
#' @export
setMethod("dbWriteTable", c("DomoConnection", "character"), domo_write_table)


# dbAppendTable -----------------------------------------------------------

domo_append_table <- function(
  conn, name, value, ..., stream = TRUE, row.names = NULL
) {
  ellipsis::check_dots_empty()
  assert_that(is.null(row.names))
  domo_write_table(conn, name, value, stream = stream, append = TRUE)
}

#' @rdname domo-tables
#' @inheritParams DBI::dbAppendTable
#' @export
setMethod("dbAppendTable", c("DomoConnection", "character"), domo_append_table)


# dbReadTable -------------------------------------------------------------

domo_read_table <- function(conn, name, ...) {
  token <- conn@token
  with_refresh(token, {
    result <- retrieve_dataset(token = token, dataset_id = name)
    if (result$content$rows == 0) {
      columns <- result$content$schema$columns
      names <- map_chr(columns, function(x) x$name)
      out <- r_data_type(map_chr(columns, function(x) x$type))
      out <- rlang::set_names(out, names)
      return(dplyr::as_tibble(out))
    } else {
      result <- export_dataset(token = token, dataset_id = name, ...)
      result$content
    }
  })
}

#' @rdname domo-tables
#' @inheritParams DBI::dbReadTable
#' @export
setMethod("dbReadTable", c("DomoConnection", "character"), domo_read_table)


# dbRemoveTable -----------------------------------------------------------

domo_remove_table <- function(conn, name, ...) {
  with_refresh(conn@token, {
    delete_dataset(token = conn@token, dataset_id = name, ...)
  })
  invisible(TRUE)
}

#' @rdname domo-tables
#' @inheritParams DBI::dbRemoveTable
#' @export
setMethod("dbRemoveTable", c("DomoConnection", "character"), domo_remove_table)


# dbExistsTable -----------------------------------------------------------

domo_exists_table <- function(conn, name, ...) {
  withRestarts(
    withCallingHandlers(
      with_refresh(conn@token, {
        retrieve_dataset(token = conn@token, dataset_id = name, ...)
        TRUE
      }),
      domo_api_error = function(e) {
        if (e$response$status_code == 404) invokeRestart("not_found")
      }
    ),
    not_found = function() FALSE
  )
}

#' @rdname domo-tables
#' @inheritParams DBI::dbExistsTable
#' @export
setMethod("dbExistsTable", c("DomoConnection", "character"), domo_exists_table)


# dbListTables ------------------------------------------------------------

domo_list_tables <- function(conn, ...) {
  with_refresh(conn@token, {
    result <- list_datasets(conn@token, ...)
  })
  map_chr(result$content, function(x) x$id)
}

#' @rdname domo-tables
#' @inheritParams DBI::dbListTables
#' @export
setMethod("dbListTables", "DomoConnection", domo_list_tables)


# dbListFields ------------------------------------------------------------

domo_list_fields <- function(conn, name, ...) {
  with_refresh(conn@token, {
    result <- retrieve_dataset(token = conn@token, dataset_id = name, ...)
  })
  map_chr(result$content$schema$columns, function(x) x$name)
}

#' @rdname domo-tables
#' @inheritParams DBI::dbListFields
#' @export
setMethod(
  "dbListFields",
  c("DomoConnection", "character"),
  domo_list_fields
)
