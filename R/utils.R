`%||%` <- function(x, y) {
  if (is.null(x) || length(x) == 0 || all(is.na(x))) y else x
}

extract_id <- function(file_path) {
  nm <- basename(file_path)
  nm <- sub("\\..*$", "", nm)
  sub("[-_].*$", "", nm)
}

snap_to_epoch <- function(t, epoch_sec, tz = NULL) {

  t <- as.POSIXct(t, origin = "1970-01-01", tz = attr(t, "tzone") %||% "UTC")
  tz_use <- tz %||% attr(t, "tzone") %||% "UTC"

  out <- as.POSIXct(
    floor(as.numeric(t) / epoch_sec) * epoch_sec,
    origin = "1970-01-01",
    tz = tz_use
  )

  attr(out, "tzone") <- tz_use
  out
}

as_activity_df <- function(df_cal, sample_rate = 100, tz = NULL) {

  tz_use <- tz %||% attr(df_cal$time, "tzone") %||% "UTC"

  attr(df_cal, "start_time") <- min(df_cal$time, na.rm = TRUE)
  attr(df_cal, "stop_time")  <- max(df_cal$time, na.rm = TRUE)
  attr(df_cal, "time_zone")  <- tz_use
  attr(df_cal, "header")     <- list(SampleRate = sample_rate)

  class(df_cal) <- c("activity_df", class(df_cal))
  df_cal
}
