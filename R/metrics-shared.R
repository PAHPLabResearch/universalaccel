#' @export
# ============================== METRICS ==============================

calculate_mims <- function(df_cal, file_path, epoch_sec, dynamic_range = c(-8, 8)) {
  tryCatch({
    message("  [MIMS] epoch=", epoch_sec, "s")
    df_cal %>%
      dplyr::select(time, X, Y, Z) %>%
      dplyr::rename(HEADER_TIME_STAMP = time) %>%
      MIMSunit::mims_unit(
        epoch = paste0(epoch_sec, " sec"),
        dynamic_range = dynamic_range,
        output_mims_per_axis = TRUE
      ) %>%
      dplyr::rename(time = HEADER_TIME_STAMP) %>%
      dplyr::mutate(time = snap_to_epoch(time, epoch_sec),
                    ID   = clean_id(file_path))
  }, error = function(e) NULL)
}

#' @export
# STRICT AI: require exactly `sample_rate` samples in each included second
calculate_ai <- function(df_cal, file_path, epoch_sec, sample_rate = 100) {
  tryCatch({
    message("  [AI] epoch=", epoch_sec, "s")
    df <- df_cal %>%
      dplyr::rename(Index = time) %>%
      dplyr::mutate(Index = lubridate::floor_date(Index, "second"))

    valid_sec <- df %>%
      dplyr::count(sec = Index, name = "n") %>%
      dplyr::filter(n == sample_rate) %>%
      dplyr::pull(sec)
    if (!length(valid_sec)) return(NULL)

    ActivityIndex::computeActivityIndex(
      df %>% dplyr::filter(Index %in% valid_sec),
      sigma0 = ActivityIndex::Sigma0(
        df %>% dplyr::filter(Index %in% valid_sec),
        hertz = sample_rate
      ),
      epoch = epoch_sec,
      hertz = sample_rate
    ) %>%
      dplyr::rename(time = RecordNo) %>%
      dplyr::mutate(time = snap_to_epoch(time, epoch_sec),
                    ID   = clean_id(file_path))
  }, error = function(e) NULL)
}
#' @export
calculate_activity_counts <- function(df_cal, file_path, epoch_sec, sample_rate = 100) {
  tryCatch({
    message("  [COUNTS] epoch=", epoch_sec, "s")
    agcounts::calculate_counts(
      as_activity_df(df_cal, sample_rate = sample_rate),
      epoch = as.numeric(epoch_sec),
      tz = "UTC",
      verbose = FALSE
    ) %>%
      dplyr::mutate(time = snap_to_epoch(time, epoch_sec),
                    ID   = clean_id(file_path))
  }, error = function(e) NULL)
}
# R/metrics_native.R

#' ENMO at native epoch
#' @export
calculate_enmo <- function(df_cal, file_path, epoch_sec) {
  tryCatch({
    message("  [ENMO] epoch=", epoch_sec, "s")
    # assumes df_cal has columns: time, X, Y, Z
    df_cal %>%
      dplyr::mutate(time = snap_to_epoch(time, epoch_sec),
                    mag  = sqrt(.data$X^2 + .data$Y^2 + .data$Z^2),
                    ENMO_inst = pmax(.data$mag - 1, 0) * 1000) %>%
      dplyr::group_by(time) %>%
      dplyr::summarise(ENMO = mean(ENMO_inst, na.rm = TRUE), .groups = "drop") %>%
      dplyr::mutate(ID = clean_id(file_path))
  }, error = function(e) NULL)
}

#' MAD at native epoch
#' @export
calculate_mad <- function(df_cal, file_path, epoch_sec) {
  tryCatch({
    message("  [MAD] epoch=", epoch_sec, "s")
    df_cal %>%
      dplyr::mutate(time = snap_to_epoch(time, epoch_sec),
                    mag  = sqrt(.data$X^2 + .data$Y^2 + .data$Z^2)) %>%
      dplyr::group_by(time) %>%
      dplyr::summarise(MAD = mean(abs(.data$mag - mean(.data$mag, na.rm = TRUE)) * 1000,
                                  na.rm = TRUE),
                       .groups = "drop") %>%
      dplyr::mutate(ID = clean_id(file_path))
  }, error = function(e) NULL)
}

#' @export
calculate_rocam <- function(df_cal, file_path, epoch_sec) {
  tryCatch({
    message("  [ROCAM] epoch=", epoch_sec, "s")
    df1 <- df_cal %>%
      dplyr::arrange(time) %>%
      dplyr::mutate(
        delta_mag = sqrt(
          (X - dplyr::lag(X))^2 +
            (Y - dplyr::lag(Y))^2 +
            (Z - dplyr::lag(Z))^2
        ),
        sec = lubridate::floor_date(time, "second")
      ) %>%
      dplyr::filter(!is.na(delta_mag))
    if (!nrow(df1)) return(NULL)

    rocam_1s <- df1 %>%
      dplyr::group_by(sec) %>%
      dplyr::summarise(ROCAM_1s = stats::median(delta_mag, na.rm = TRUE) * 1000, .groups = "drop")

    rocam_1s %>%
      dplyr::transmute(time = snap_to_epoch(sec, epoch_sec),
                       ROCAM = ROCAM_1s,
                       ID    = clean_id(file_path)) %>%
      dplyr::group_by(time, ID) %>%
      dplyr::summarise(ROCAM = mean(ROCAM, na.rm = TRUE), .groups = "drop")
  }, error = function(e) NULL)
}
