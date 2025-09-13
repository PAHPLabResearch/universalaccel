#' Read + calibrate ActiGraph GT3X with GGIR C++ core (in agcalibrate)
#' @noRd
read_and_calibrate_actigraph <- function(file_path, sample_rate = 100, tz = "UTC") {
  raw <- read.gt3x::read.gt3x(file_path, asDataFrame = TRUE, imputeZeroes = FALSE)
  if (!inherits(raw$time, "POSIXt")) raw$time <- as.POSIXct(raw$time, tz = tz)
  raw <- raw |>
    dplyr::transmute(time = lubridate::force_tz(time, tz),
                     X = as.numeric(X), Y = as.numeric(Y), Z = as.numeric(Z)) |>
    dplyr::arrange(time)
  cal <- agcounts::agcalibrate(raw = raw, verbose = FALSE, tz = tz)
  n  <- nrow(cal)
  t0 <- lubridate::floor_date(cal$time[1], "second")
  tibble::tibble(time = t0 + seq(0, by = 1 / sample_rate, length.out = n),
                 X = as.numeric(cal$X), Y = as.numeric(cal$Y), Z = as.numeric(cal$Z))
}
