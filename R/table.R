# dbCreateTable -----------------------------------------------------------

#' @rdname DomoConnection-class
#' @export
setMethod(
  "dbCreateTable",
  "DomoConnection",
  function(conn, name, fields, ..., row.names = NULL, temporary = FALSE) {
    with_refresh(conn@token, {
      result <- create_stream(
        token = conn@token, name = name, schema = fields, ...
      )
    })
    id <- result$content$dataSet$id
    message(glue::glue("Created table {id}"))
    invisible(id)
  }
)


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


#' @rdname DomoConnection-class
#' @export
setMethod(
  "dbWriteTable",
  c("DomoConnection", "character"),
  domo_write_table
)


# dbReadTable -------------------------------------------------------------

#' @rdname DomoConnection-class
#' @export
setMethod(
  "dbReadTable",
  c("DomoConnection", "character"),
  function(conn, name, ...) {
    with_refresh(conn@token, {
      result <- retrieve_dataset(token = conn@token, dataset_id = name)
      if (result$content$rows == 0) {
        columns <- result$content$schema$columns
        names <- map_chr(columns, function(x) x$name)
        out <- r_data_type(map_chr(columns, function(x) x$type))
        out <- rlang::set_names(out, names)
        return(dplyr::as_tibble(out))
      } else {
        result <- export_dataset(token = conn@token, dataset_id = name, ...)
        result$content
      }
    })
  }
)


# dbRemoveTable -----------------------------------------------------------

#' @rdname DomoConnection-class
#' @export
setMethod(
  "dbRemoveTable",
  c("DomoConnection", "character"),
  function(conn, name, ...) {
    with_refresh(conn@token, {
      delete_dataset(token = conn@token, dataset_id = name, ...)
    })
    invisible(TRUE)
  }
)


# dbExistsTable -----------------------------------------------------------

#' @rdname DomoConnection-class
#' @export
setMethod(
  "dbExistsTable",
  c("DomoConnection", "character"),
  function(conn, name, ...) {
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
)


# dbListTables ------------------------------------------------------------

#' @rdname DomoConnection-class
#' @export
setMethod("dbListTables", c("DomoConnection"), function(conn, ...) {
  with_refresh(conn@token, {
    result <- list_datasets(conn@token, ...)
  })
  map_chr(result$content, function(x) x$id)
})


# dbListFields ------------------------------------------------------------

#' @rdname DomoConnection-class
#' @export
setMethod(
  "dbListFields",
  c("DomoConnection", "character"),
  function(conn, name, ...) {
    with_refresh(conn@token, {
      result <- retrieve_dataset(token = conn@token, dataset_id = name, ...)
    })
    map_chr(result$content$schema$columns, function(x) x$name)
  }
)
