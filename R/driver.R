# R/driver.R

#' Summarize accelerometer files by device
#'
#' @param device "actigraph", "axivity", or "geneactiv"
#' @param data_folder Input folder
#' @param output_folder Output folder
#' @param epochs Epoch lengths (sec)
#' @param sample_rate Target Hz for loaders that align to a grid
#' @param dynamic_range c(min, max) in g for MIMS
#' @param apply_nonwear If TRUE, apply Choi non-wear when Vector.Magnitude is available
#' @param nonwear_cfg List of Choi params (min_bout, spike_tol, spike_upper, flank)
#' @param tz Time zone for parsing/output
#' @return Invisibly, vector of written file paths
#' @export
accel_summaries <- function(device, data_folder, output_folder,
                            epochs        = 60,
                            sample_rate   = 100,
                            dynamic_range = c(-8, 8),
                            apply_nonwear = FALSE,
                            nonwear_cfg   = list(min_bout = 90L, spike_tol = 2L, spike_upper = 100L, flank = 30L),
                            tz = "UTC") {
  
  if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)
  
  loader <- switch(tolower(device),
                   "actigraph" = read_and_calibrate_actigraph,
                   "axivity"   = read_and_calibrate_axivity,
                   "geneactiv" = read_and_calibrate_geneactiv,
                   stop("Unknown device: ", device)
  )
  
  pattern <- switch(tolower(device),
                    "actigraph" = "\\.gt3x$",
                    "axivity"   = "\\.resampled\\.csv$|\\.csv(\\.gz)?$",
                    "geneactiv" = "\\.bin$"
  )
  
  files <- list.files(data_folder, pattern = pattern, full.names = TRUE)
  if (!length(files)) { message("[WARN] No input files found"); return(invisible(NULL)) }
  
  for (epoch in epochs) {
    message(paste0("\n[RUN] device=", device, " epoch=", epoch, "s"))
    
    results <- lapply(files, function(fp) {
      message("[FILE] ", basename(fp))
      
      df_cal <- tryCatch(loader(fp, sample_rate = sample_rate, tz = tz),
                         error = function(e) { message("  [ERR] ", e$message); return(NULL) })
      if (is.null(df_cal)) return(NULL)
      
      pieces <- list(
        MIMS  = calculate_mims(df_cal, fp, epoch, dynamic_range = dynamic_range),
        AI    = calculate_ai(df_cal, fp, epoch, sample_rate = sample_rate),
        AC    = calculate_activity_counts(df_cal, fp, epoch, sample_rate = sample_rate),
        ENMO  = calculate_enmo(df_cal, fp, epoch),
        MAD   = calculate_mad(df_cal, fp, epoch),
        ROCAM = calculate_rocam(df_cal, fp, epoch)
      )
      
      # report availability per metric
      message("  [QC] metric availability:")
      for (nm in names(pieces)) {
        di <- pieces[[nm]]
        if (is.null(di) || !nrow(di)) message(sprintf("    - %-5s: NULL/empty"), nm) else
          message(sprintf("    - %-5s: %d rows", nm, nrow(di)))
      }
      
      # keep only non-empty pieces; require at least 2 to proceed
      keep <- Filter(function(d) !is.null(d) && nrow(d) > 0, pieces)
      if (length(keep) < 2L) { message("  [SKIP] one or more metrics empty"); return(NULL) }
      
      # de-dupe keys and join strictly across what's available
      keep <- lapply(keep, function(d) dplyr::distinct(d, time, ID, .keep_all = TRUE))
      joined <- suppressMessages(purrr::reduce(keep, ~ dplyr::inner_join(.x, .y, by = c("time","ID"))))
      if (!nrow(joined)) { message("  [SKIP] join produced 0 rows"); return(NULL) }
      
      out <- dplyr::arrange(joined, time)
      
      # optional Choi non-wear (needs per-minute Vector.Magnitude from AC)
      if (apply_nonwear) {
        if ("Vector.Magnitude" %in% names(out)) {
          message("  [NONWEAR] computing Choi flags from minute Vector.Magnitude")
          
          epoch_sec <- {
            tt <- sort(unique(out$time))
            if (length(tt) >= 2) as.integer(round(stats::median(diff(as.numeric(tt))))) else 60L
          }
          
          if (epoch_sec > 60L) {
            message("  [NONWEAR][SKIP] epoch=", epoch_sec, "s > 60s; requires minute CPM.")
          } else {
            vm_min <- out %>%
              dplyr::mutate(minute = lubridate::floor_date(time, "minute")) %>%
              dplyr::group_by(ID, minute) %>%
              dplyr::summarise(vector_magnitude = sum(.data[["Vector.Magnitude"]], na.rm = TRUE), .groups = "drop")
            
            flags <- tryCatch(
              do.call(compute_choi_nonwear_minutes, c(list(
                data     = vm_min,
                id_col   = "ID",
                time_col = "minute",
                cpm_col  = "vector_magnitude"
              ), nonwear_cfg)),
              error = function(e) { message("  [NONWEAR][ERR] ", e$message); NULL }
            )
            
            if (!is.null(flags) && nrow(flags)) {
              out <- out %>%
                dplyr::mutate(minute = lubridate::floor_date(time, "minute")) %>%
                dplyr::left_join(dplyr::select(flags, ID, minute, choi_nonwear),
                                 by = c("ID" = "ID", "minute" = "minute")) %>%
                dplyr::select(-minute)
            } else {
              message("  [NONWEAR][SKIP] flags empty")
            }
          }
        } else {
          message("  [NONWEAR][SKIP] Vector.Magnitude not found; counts not available.")
        }
      }
      
      out
    })
    
    final_df <- dplyr::bind_rows(results)
    if (!nrow(final_df)) { message("  [SKIP] no rows written"); next }
    
    out_file <- file.path(output_folder, paste0("univ_", tolower(device), "_epoch", epoch, "s_", Sys.Date(), ".csv"))
    readr::write_csv(final_df, out_file)
    message("  [WRITE] ", out_file)
  }
  
  message("\n[DONE] all epochs")
  invisible(NULL)
}
