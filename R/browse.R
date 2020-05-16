#' Browse the dataset in your browser
#'
#' @param con A [DomoConnection-class] object
#' @param dataset_id The dataset's ID as a string
#'
#' @export
browse_dataset <- function(con, dataset_id) {
  stopifnot(is.character(dataset_id), length(dataset_id) == 1)
  domain <- con@token$content$domain
  url <- httr::modify_url(
    url = "",
    scheme = "https",
    hostname = domain,
    path = c("datasources", dataset_id, "details", "data", "table")
  )
  httr::BROWSE(url)
}
