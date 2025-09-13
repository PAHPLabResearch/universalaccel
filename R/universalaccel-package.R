"_PACKAGE"

#' universalaccel: Unified Accelerometry Metrics
#'
#' This package provides harmonized metric computation across ActiGraph, Axivity, and GENEActiv devices.
#' It includes ENMO, MAD, MIMS, AI, ROCAM, and counts, with support for calibrated input and reproducible pipelines.
#'
#' @name universalaccel
#' @description
#' A unified framework for computing harmonized accelerometry metrics across ActiGraph, Axivity, and GENEActiv devices.
#'
#' @importFrom dplyr %>%
#' @importFrom lubridate floor_date
#' @importFrom readr write_csv
#'
#' @details
#' `universalaccel` is designed for researchers, data scientists, and developers working with raw accelerometry data.
#' It is suitable for physical activity researchers, public health analysts, and developers building reproducible pipelines.
#'
#' For a quick overview, see the README at <https://github.com/JimmyDhr/universalaccel>.
#' For developer validation, run `source("tools/preflight.R")` from the package root.
#' For interactive exploration, launch the Shiny app via `run_calibrated_data_app()`.
#'
#' @seealso
#' - `accel_summaries()` for batch metric computation
#' - `run_calibrated_data_app()` for Shiny-based visualization
#' - `vignette("getting-started")` for a full walkthrough
#' - `tools/preflight.R` for reproducibility checks
NULL
