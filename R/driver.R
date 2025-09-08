#' Summarize accelerometer files by device
#'
#' @param device One of "actigraph", "axivity", or "geneactiv".
#' @param data_folder Folder containing raw/native files for the device.
#' @param output_folder Folder to write per-epoch CSV outputs.
#' @param epochs Numeric vector of epoch lengths in seconds (e.g., c(1, 60)).
#' @param sample_rate Target Hz for loaders that need a grid (default 100).
#' @param dynamic_range Numeric length-2 vector in g, e.g., c(-8, 8).
#' @param apply_nonwear Logical; if TRUE, apply Choi non-wear when VM exists.
#' @param nonwear_cfg List of Choi parameters (min_bout, spike_tol, spike_upper, flank).
#' @param tz Time zone for all time operations (default "UTC").
#' @return (Invisibly) character vector of written file paths.
#' @export
accel_summaries <- function(device, data_folder, output_folder,
                            epochs = c(60),
                            sample_rate = 100,
                            dynamic_range = c(-8, 8),
                            apply_nonwear = FALSE,
                            nonwear_cfg = list(min_bout=90L, spike_tol=2L, spike_upper=100L, flank=30L),
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
    return(invisible(NULL))
  }
  
  for (epoch in epochs) {
    message(paste0("\n[RUN] device=", device, " epoch=", epoch, "s"))
    
    results <- lapply(files, function(fp) {
      message("[FILE] ", basename(fp))
      df_cal <- tryCatch(loader(fp, sample_rate = sample_rate, tz = tz),
                         error = function(e) { message("  [ERR] ", e$message); return(NULL) })
      if (is.null(df_cal)) return(NULL)
      
      pieces <- list(
        calculate_mims(df_cal, fp, epoch, dynamic_range = dynamic_range),
        calculate_ai(df_cal, fp, epoch, sample_rate = sample_rate),
        calculate_activity_counts(df_cal, fp, epoch, sample_rate = sample_rate),
        calculate_enmo(df_cal, fp, epoch),
        calculate_mad(df_cal, fp, epoch),
        calculate_rocam(df_cal, fp, epoch)
      )
      
      if (any(vapply(pieces, is.null, logical(1)))) return(NULL)
      pieces <- lapply(pieces, function(d) dplyr::distinct(d, time, ID, .keep_all = TRUE))
      
      joined <- suppressMessages(
        Reduce(function(x, y) dplyr::inner_join(x, y, by = c("time", "ID")), pieces)
      )
      if (!nrow(joined)) return(NULL)
      
      out <- joined %>%
        dplyr::distinct(time, ID, .keep_all = TRUE) %>%
        dplyr::arrange(time)
      
      if (apply_nonwear && "Vector.Magnitude" %in% names(out)) {
        vm_min <- out %>%
          dplyr::transmute(id = ID, time = lubridate::floor_date(time, "minute"),
                           vector_magnitude = Vector.Magnitude) %>%
          dplyr::distinct(id, time, .keep_all = TRUE)
        vm_flag <- do.call(compute_choi_nonwear_minutes, c(list(vm_min), nonwear_cfg))
        out <- out %>%
          dplyr::left_join(dplyr::select(vm_flag, id, time, choi_nonwear),
                           by = c("ID" = "id", "time" = "time"))
      }
      out
    })
    
    final_df <- dplyr::bind_rows(results)
    if (!nrow(final_df)) {
      message("  [SKIP] no rows written")
      next
    }
    
    out_file <- file.path(output_folder, paste0("univ_", tolower(device),
                                                "_epoch", epoch, "s_", Sys.Date(), ".csv"))
    readr::write_csv(final_df, out_file)
    message("  [WRITE] ", out_file)
  }
  
  message("\n[DONE] all epochs")
}
