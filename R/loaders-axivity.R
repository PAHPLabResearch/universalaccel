#' Read Axivity resampled CSV (already calibrated)
#' @noRd
read_and_calibrate_axivity <- function(file_path, sample_rate = 100, tz = "UTC") {
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
  
  n  <- nrow(df)
  if (!n) stop("No samples after read: ", basename(file_path))
  t0 <- lubridate::floor_date(df$time[1], "second")
  tibble::tibble(time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
                 X = df$X, Y = df$Y, Z = df$Z)
}
