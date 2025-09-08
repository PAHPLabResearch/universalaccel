clean_id <- function(file_path) substr(basename(file_path), 1, 7) %>% gsub("[^A-Za-z0-9]", "", .)
snap_to_epoch <- function(t, epoch_sec) as.POSIXct(floor(as.numeric(t) / epoch_sec) * epoch_sec, origin = "1970-01-01", tz = "UTC")

as_activity_df <- function(df_cal, sample_rate = 100, tz = "UTC") {
  attr(df_cal, "start_time") <- min(df_cal$time)
  attr(df_cal, "stop_time")  <- max(df_cal$time)
  attr(df_cal, "time_zone")  <- tz
  attr(df_cal, "header")     <- list(SampleRate = sample_rate)
  class(df_cal) <- c("activity_df", class(df_cal))
  df_cal
}
