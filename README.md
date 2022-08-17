
<!-- README.md is generated from README.Rmd. Please edit that file -->

# domo

<!-- badges: start -->

[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://www.tidyverse.org/lifecycle/#experimental)
<!-- badges: end -->

This is an R package that wraps [Domo’s REST
API](https://developer.domo.com/docs/dataset/overview-5). This package
is [DBI](https://github.com/r-dbi/DBI)-compliant.

## Installation

And the development version from [GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("shunsambongi/domo")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(domo)

con <- dbConnect(domo())

dataset_id <- dbCreateTable(con, "iris", iris)

# dataset ID will also be available in the dataset URL 
# e.g. b06e0040-9aa6-11ea-8515-f76f845b5ce6
dbWriteTable(con, dataset_id, iris, overwrite = TRUE)
dbReadTable(con, dataset_id)
```

## dbplyr

[dbplyr](https://github.com/tidyverse/dbplyr/) support is experimental.

``` r
library(dplyr)

tbl(con, "b06e0040-9aa6-11ea-8515-f76f845b5ce6") %>%
  select(Sepal.Length, Sepal.Width, Species) %>%
  mutate(Sepal.Sum = Sepal.Length + Sepal.Width) %>%
  group_by(Species) %>%
  summarise(Max.Sepal.Sum = max(Sepal.Sum, na.rm = TRUE)) %>%
  ungroup() %>%
  filter(Species == "setosa")
```

## Credentials

Create credentials by logging in at [Domo’s developer
website](https://developer.domo.com/login), and [creating a new
client](https://developer.domo.com/new-client). The client will need the
`data` application scope.

After creating your client, set the environment variables
`DOMO_CLIENT_ID` and `DOMO_CLIENT_SECRET` to the client’s ID and secret
values from the [client’s
webpage](https://developer.domo.com/manage-clients).

If you need to connect to Domo using multiple clients with different
permission scopes, or you need to connect to multiple Domo instances,
pass the client ID and secret values to the `dbConnect` method directly:

``` r
con1 <- dbConnect(domo::domo(), "<client id 1>", "<client secret 1>")
con2 <- dbConnect(domo::domo(), "<client id 2>", "<client secret 2>")
```
