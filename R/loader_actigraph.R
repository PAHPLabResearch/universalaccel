# R/actigraph_loader.R
#' Read + calibrate ActiGraph GT3X with GGIR C++ core (in agcalibrate)
#' @noRd
read_and_calibrate_actigraph <- function(file_path, sample_rate = 100, tz = "UTC") {
  if (!requireNamespace("read.gt3x", quietly = TRUE)) stop("Package 'read.gt3x' is required.")
  if (!requireNamespace("agcounts", quietly = TRUE)) stop("Package 'agcounts' is required.")
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("Package 'lubridate' is required.")
  if (!requireNamespace("tibble", quietly = TRUE)) stop("Package 'tibble' is required.")

  raw <- read.gt3x::read.gt3x(file_path, asDataFrame = TRUE, imputeZeroes = FALSE)
  if (!inherits(raw$time, "POSIXt")) raw$time <- as.POSIXct(raw$time, tz = tz)

  raw <- raw |>
    dplyr::transmute(
      time = lubridate::force_tz(time, tz),
      X = as.numeric(X),
      Y = as.numeric(Y),
      Z = as.numeric(Z)
    ) |>
    dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
    dplyr::arrange(time)

  if (!nrow(raw)) stop("No samples after ActiGraph read: ", basename(file_path))

  cal <- agcounts::agcalibrate(raw = raw, verbose = FALSE, tz = tz)
  cal <- as.data.frame(cal)

  if (!all(c("time", "X", "Y", "Z") %in% names(cal))) {
    stop("ActiGraph calibration output did not contain required columns: ", basename(file_path))
  }

  n <- nrow(cal)
  t0 <- lubridate::floor_date(as.POSIXct(cal$time[1], tz = tz), "second")

  out <- tibble::tibble(
    time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
    X = as.numeric(cal$X),
    Y = as.numeric(cal$Y),
    Z = as.numeric(cal$Z)
  )

  spec <- list(
    model = "actigraph_gt3x",
    source_tz_detected = TRUE,
    source_tz = tz,
    compute_tz = tz,
    output_tz = tz,
    tz_rule = "compute_in_source_tz",
    grid_action = "reindexed",
    regularized_for_uniformity = FALSE,
    calibration_input_provenance = "device_loader_native",
    calibration_expected = TRUE,
    calibration_source = "agcounts_agcalibrate",
    calibration_attempted = TRUE,
    calibration_success = TRUE,
    calibration_note = "ok",
    units_choice = "g",
    autocalibrated = TRUE,
    autocalibrate_note = "ok"
  )

  report <- c(
    "ActiGraph loader used native GT3X read path.",
    "ActiGraph calibration was attempted with agcounts::agcalibrate().",
    "Output timeline was rebuilt to requested sample_rate by order-preserving reindex.",
    "No X/Y/Z interpolation was performed in UA."
  )

  attr(out, "ua_time_spec") <- spec
  attr(out, "ua_time_report") <- report
  out
}
