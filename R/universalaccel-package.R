#' universalaccel: Unified Accelerometry Metrics
#'
#' A framework for loading, harmonizing, and analyzing accelerometry data
#' across multiple devices and processing workflows.
#'
#' @description
#' `universalaccel` provides a unified framework for working with raw and
#' precomputed accelerometry data. It supports harmonized metric computation
#' across ActiGraph, Axivity, GENEActiv, and compatible generic inputs, with
#' tools for reproducible processing pipelines and downstream summary workflows.
#'
#' The package supports commonly used accelerometry metrics including ENMO,
#' MAD, MIMS, AI, ROCAM, and counts, with support for calibrated or
#' autocalibrated input where available.
#'
#' @details
#' `universalaccel` is designed for researchers, data scientists, and
#' developers working with accelerometry data in epidemiology, behavioral
#' science, and related fields.
#'
#' The package currently supports two broad workflows:
#'
#' \itemize{
#'   \item raw device files to harmonized epoch-level metrics
#'   \item precomputed epoch-level metrics to standardized analytic summaries
#' }
#'
#' Main features include:
#'
#' \itemize{
#'   \item device-aware loading and harmonization
#'   \item reproducible metric computation pipelines
#'   \item support for batch processing
#'   \item downstream summary and analytic utilities for precomputed metrics
#' }
#'
#' For an overview of the package, see the README at
#' <https://github.com/PAHPLabResearch/universalaccel>.
#'
#' For a full walkthrough, see the getting started vignette:
#' `vignette("getting-started")`.
#'
#' For developer validation, run `source("tools/preflight.R")` from the
#' package root.
#'
#' @seealso
#' \code{\link{accel_summaries}} for batch metric computation from raw files.
#'
#' @docType package
#' @name universalaccel
#'
#' @importFrom dplyr %>%
#' @importFrom lubridate floor_date
#' @importFrom readr write_csv
NULL
