#' Summarize accelerometer files by device
#' @export
accel_summaries <- function(device, data_folder, output_folder,
                            epochs = c(60),
                            sample_rate = 100,
                            dynamic_range = c(-8, 8),
                            apply_nonwear = FALSE,
                            nonwear_cfg = list(min_bout = 90L, spike_tol = 2L, spike_upper = 100L, flank = 30L),
                            tz = "UTC") {
  
  if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)
  
  loader <- switch(tolower(device),
                   "actigraph" = read_and_calibrate_actigraph,
                   "axivity"   = read_and_calibrate_axivity,
                   "geneactiv" = read_and_calibrate_geneactiv,
                   stop("Unknown device: ", device))
  
  pattern <- switch(tolower(device),
                    "actigraph" = "\\.gt3x$",
                    "axivity"   = "\\.csv(\\.gz)?$",
                    "geneactiv" = "\\.bin$")
  
  files <- list.files(data_folder, pattern = pattern, full.names = TRUE)
  if (!length(files)) {
    message("[WARN] No input files found")
    return(invisible(character(0)))
  }
  
  written <- character(0)
  
  for (epoch in epochs) {
    message(paste0("\n[RUN] device=", device, " epoch=", epoch, "s"))
    
    results <- lapply(files, function(fp) {
      message("[FILE] ", basename(fp))
      df_cal <- tryCatch(loader(fp, sample_rate = sample_rate, tz = tz),
                         error = function(e) { message("  [ERR] ", e$message); NULL })
      if (is.null(df_cal)) return(NULL)
      
      pieces <- list(
        calculate_mims(df_cal, fp, epoch, dynamic_range = dynamic_range),
        calculate_ai(df_cal, fp, epoch, sample_rate = sample_rate),
        calculate_activity_counts(df_cal, fp, epoch, sample_rate = sample_rate),
        calculate_enmo(df_cal, fp, epoch),
        calculate_mad(df_cal, fp, epoch),
        calculate_rocam(df_cal, fp, epoch)
      )
      
      pieces <- Filter(function(d) !is.null(d) && nrow(d) > 0, pieces)
      if (!length(pieces)) return(NULL)
      
      out <- suppressMessages(
        purrr::reduce(pieces, ~ dplyr::inner_join(.x, .y, by = c("time", "ID")))
      )
      if (!nrow(out)) return(NULL)
      
      dplyr::arrange(out, time)
    })
    
    final_df <- dplyr::bind_rows(results)
    if (!nrow(final_df)) {
      message("  [SKIP] no rows written")
      next
    }
    
    out_file <- file.path(
      output_folder,
      paste0("univ_", tolower(device), "_epoch", epoch, "s_", Sys.Date(), ".csv")
    )
    readr::write_csv(final_df, out_file)
    message("  [WRITE] ", out_file)
    written <- c(written, out_file)
  }
  
  message("\n[DONE] all epochs")
  invisible(written)
}
