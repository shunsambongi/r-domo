.onLoad <- function(...) {
  vctrs::s3_register("vctrs::obj_print_header", "domo_token")
  vctrs::s3_register("vctrs::obj_print_data", "domo_token")
}
