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

calculate_enmo <- function(df_cal, file_path, epoch_sec) {
  tryCatch({
    message("  [ENMO] 1s -> epoch=", epoch_sec, "s")
    df1s <- df_cal %>%
      dplyr::mutate(
        sec = lubridate::floor_date(time, "second"),
        mag = sqrt(X^2 + Y^2 + Z^2)
      ) %>%
      dplyr::group_by(sec) %>%
      dplyr::summarise(
        ENMO_1s = mean(pmax(mag - 1, 0) * 1000, na.rm = TRUE),
        .groups = "drop"
      )
    if (!nrow(df1s)) return(NULL)
    
    df1s %>%
      dplyr::mutate(time = snap_to_epoch(sec, epoch_sec)) %>%
      dplyr::group_by(time) %>%
      dplyr::summarise(ENMO = mean(ENMO_1s, na.rm = TRUE), .groups = "drop") %>%
      dplyr::mutate(ID = clean_id(file_path))
  }, error = function(e) NULL)
}

calculate_mad <- function(df_cal, file_path, epoch_sec) {
  tryCatch({
    message("  [MAD] 1s -> epoch=", epoch_sec, "s")
    df1s <- df_cal %>%
      dplyr::mutate(
        sec = lubridate::floor_date(time, "second"),
        mag = sqrt(X^2 + Y^2 + Z^2)
      ) %>%
      dplyr::group_by(sec) %>%
      dplyr::summarise(
        MAD_1s = mean(abs(mag - mean(mag, na.rm = TRUE)) * 1000, na.rm = TRUE),
        .groups = "drop"
      )
    if (!nrow(df1s)) return(NULL)
    
    df1s %>%
      dplyr::mutate(time = snap_to_epoch(sec, epoch_sec)) %>%
      dplyr::group_by(time) %>%
      dplyr::summarise(MAD = mean(MAD_1s, na.rm = TRUE), .groups = "drop") %>%
      dplyr::mutate(ID = clean_id(file_path))
  }, error = function(e) NULL)
}

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
