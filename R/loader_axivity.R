# R/axivity_loader.R
#' Read Axivity resampled CSV (already calibrated upstream)
#' @noRd
read_and_calibrate_axivity <- function(file_path, sample_rate = 100, tz = "UTC") {
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("lubridate", quietly = TRUE)) stop("Package 'lubridate' is required.")
  if (!requireNamespace("tibble", quietly = TRUE)) stop("Package 'tibble' is required.")

  df <- read.csv(file_path, header = TRUE) |>
    dplyr::rename(
      time = dplyr::any_of(c("Time","time","timestamp")),
      X    = dplyr::any_of(c("Accel.X..g.","X","AccelX","accel_x")),
      Y    = dplyr::any_of(c("Accel.Y..g.","Y","AccelY","accel_y")),
      Z    = dplyr::any_of(c("Accel.Z..g.","Z","AccelZ","accel_z"))
    ) |>
    dplyr::mutate(
      time = as.POSIXct(time, format = "%Y-%m-%d %H:%M:%OS", tz = tz),
      X = as.numeric(X), Y = as.numeric(Y), Z = as.numeric(Z)
    ) |>
    dplyr::filter(!is.na(time) & is.finite(X) & is.finite(Y) & is.finite(Z)) |>
    dplyr::arrange(time)

  n <- nrow(df)
  if (!n) stop("No samples after read: ", basename(file_path))

  t0 <- lubridate::floor_date(df$time[1], "second")

  out <- tibble::tibble(
    time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
    X = df$X,
    Y = df$Y,
    Z = df$Z
  )

  spec <- list(
    model = "axivity_resampled_csv",
    source_tz_detected = TRUE,
    source_tz = tz,
    compute_tz = tz,
    output_tz = tz,
    tz_rule = "compute_in_source_tz",
    grid_action = "reindexed",
    regularized_for_uniformity = FALSE,
    calibration_input_provenance = "upstream_calibrated_resampled_csv",
    calibration_expected = TRUE,
    calibration_source = "upstream_resampled_calibrated",
    calibration_attempted = FALSE,
    calibration_success = TRUE,
    calibration_note = "already_calibrated_upstream",
    units_choice = "g",
    autocalibrated = TRUE,
    autocalibrate_note = "already_calibrated_upstream"
  )

  report <- c(
    "Axivity loader expects resampled CSV already calibrated upstream.",
    "UA did not perform additional calibration in the loader.",
    "Timeline was rebuilt to requested sample_rate by order-preserving reindex.",
    "No X/Y/Z interpolation was performed in UA."
  )

  attr(out, "ua_time_spec") <- spec
  attr(out, "ua_time_report") <- report
  out
}
