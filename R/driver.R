#' Summarize accelerometer files by device
#'
#' Computes MIMS, AI, ActiGraph counts (when available), ENMO, MAD, and ROCAM,
#' then (optionally) applies Choi non-wear if a per-minute Vector.Magnitude
#' column is present (i.e., when epoch = 60s).
#'
#' @param device One of "actigraph", "axivity", "geneactiv".
#' @param data_folder Folder containing raw/native files.
#' @param output_folder Folder to write per-epoch CSV outputs.
#' @param epochs Numeric vector of epoch lengths in seconds.
#' @param sample_rate Target Hz for loaders needing a regular grid.
#' @param dynamic_range Length-2 vector in g, e.g., c(-8, 8).
#' @param apply_nonwear Logical; if TRUE and epoch == 60 and
#'   `Vector.Magnitude` exists, computes Choi non-wear flags.
#' @param nonwear_cfg List with min_bout, spike_tol, spike_upper, flank.
#' @param tz Time zone for time operations.
#' @param debug Logical; print per-metric diagnostics before join.
#' @return Invisibly, character vector of written file paths.
#' @export
accel_summaries <- function(device,
                            data_folder,
                            output_folder,
                            epochs        = 60,
                            sample_rate   = 100,
                            dynamic_range = c(-8, 8),
                            apply_nonwear = FALSE,
                            nonwear_cfg   = list(min_bout=90L, spike_tol=2L, spike_upper=100L, flank=30L),
                            tz            = "UTC",
                            debug         = TRUE) {
  
  if (!dir.exists(output_folder)) dir.create(output_folder, recursive = TRUE)
  
  loader <- switch(
    tolower(device),
    "actigraph" = read_and_calibrate_actigraph,
    "axivity"   = read_and_calibrate_axivity,
    "geneactiv" = read_and_calibrate_geneactiv,
    stop("Unknown device: ", device)
  )
  
  pattern <- switch(
    tolower(device),
    "actigraph" = "\\.gt3x$",
    "axivity"   = "\\.csv(\\.gz)?$",
    "geneactiv" = "\\.bin$"
  )
  
  files <- list.files(data_folder, pattern = pattern, full.names = TRUE)
  if (!length(files)) { message("[WARN] No input files found"); return(invisible(character())) }
  
  # --- tiny helpers for debugging ----
  .dups_count <- function(d) if (!all(c("time","ID") %in% names(d))) NA_integer_ else nrow(d) - nrow(dplyr::distinct(d, time, ID))
  .time_step  <- function(d) {
    if (!"time" %in% names(d)) return(NA_character_)
    tt <- sort(unique(as.numeric(d$time)))
    if (length(tt) < 3) return(NA_character_)
    dt <- diff(tt); dt <- dt[is.finite(dt) & dt > 0]
    if (!length(dt)) return(NA_character_)
    sprintf("median=%.3fs IQR=[%.3f, %.3f]", stats::median(dt), stats::quantile(dt, .25), stats::quantile(dt, .75))
  }
  .na_rate <- function(d) {
    num <- dplyr::select(d, where(is.numeric))
    if (!ncol(num)) return(NA_character_)
    sprintf("%.1f%%", 100 * mean(is.na(as.matrix(num))))
  }
  .zero_cols <- function(d) {
    num <- dplyr::select(d, where(is.numeric))
    if (!ncol(num)) return("")
    zs <- vapply(num, function(v) all(is.finite(v)) && all(v == 0), logical(1))
    paste(names(num)[zs], collapse = ", ")
  }
  
  written <- character(0)
  
  for (epoch in epochs) {
    message(paste0("\n[RUN] device=", device, " epoch=", epoch, "s"))
    
    results <- lapply(files, function(fp) {
      message("[FILE] ", basename(fp))
      
      df_cal <- tryCatch(loader(fp, sample_rate = sample_rate, tz = tz),
                         error = function(e) { message("  [ERR] ", e$message); return(NULL) })
      if (is.null(df_cal)) return(NULL)
      
      # --- compute all metrics (names used for logs/diagnostics) ---
      pieces <- list(
        MIMS   = calculate_mims(df_cal, fp, epoch, dynamic_range = dynamic_range),
        AI     = calculate_ai(df_cal, fp, epoch, sample_rate = sample_rate),
        COUNTS = calculate_activity_counts(df_cal, fp, epoch, sample_rate = sample_rate),
        ENMO   = calculate_enmo(df_cal, fp, epoch),
        MAD    = calculate_mad(df_cal, fp, epoch),
        ROCAM  = calculate_rocam(df_cal, fp, epoch)
      )
      
      # availability print
      if (debug) {
        message("  [QC] metric availability:")
        for (nm in names(pieces)) {
          di <- pieces[[nm]]
          if (is.null(di) || !is.data.frame(di) || nrow(di) == 0) {
            message(sprintf("    - %-7s : NULL/empty", nm))
          } else {
            message(sprintf("    - %-7s : %d rows", nm, nrow(di)))
          }
        }
      }
      
      # keep only non-empty data.frames
      keep <- Filter(function(d) is.data.frame(d) && nrow(d) > 0, pieces)
      if (!length(keep)) { message("  [SKIP] one or more metrics empty"); return(NULL) }
      
      # unique keys per piece
      keep <- lapply(keep, function(d) dplyr::distinct(d, time, ID, .keep_all = TRUE))
      
      # per-metric diagnostics
      if (debug) {
        message("  [QC] per-metric diagnostics:")
        for (nm in names(keep)) {
          di <- keep[[nm]]
          message(sprintf(
            "    - %-7s : rows=%d | dup_keys=%s | NA=%s | step=%s%s",
            nm, nrow(di),
            ifelse(is.na(.dups_count(di)), "NA", .dups_count(di)),
            ifelse(is.na(.na_rate(di)),    "NA", .na_rate(di)),
            ifelse(is.na(.time_step(di)),  "NA", .time_step(di)),
            {zc <- .zero_cols(di); ifelse(nchar(zc), paste0(" | all-zero cols: [", zc, "]"), "")}
          ))
        }
      }
      
      # strict inner join (keep only rows present in all metrics)
      joined <- suppressMessages(
        purrr::reduce(keep, ~ dplyr::inner_join(.x, .y, by = c("time","ID")))
      )
      
      if (!nrow(joined)) { message("  [SKIP] join produced 0 rows"); return(NULL) }
      
      # optional non-wear: only if epoch == 60 and Vector.Magnitude exists
      if (isTRUE(apply_nonwear) && epoch == 60 && "Vector.Magnitude" %in% names(joined)) {
        message("  [NONWEAR] computing Choi flags from minute Vector.Magnitude")
        vm_min <- joined %>%
          dplyr::transmute(id = ID,
                           time = lubridate::floor_date(time, "minute"),
                           vector_magnitude = Vector.Magnitude) %>%
          dplyr::distinct(id, time, .keep_all = TRUE)
        
        # ensure data.table methods are available
        if (!"package:data.table" %in% search()) attachNamespace("data.table")
        
        vm_flag <- tryCatch(
          do.call(compute_choi_nonwear_minutes, c(list(vm_min), nonwear_cfg)),
          error = function(e) { message("  [NONWEAR][ERR] ", e$message); NULL }
        )
        
        if (!is.null(vm_flag) && all(c("id","time","choi_nonwear") %in% names(vm_flag))) {
          joined <- dplyr::left_join(
            joined,
            dplyr::select(vm_flag, id, time, choi_nonwear),
            by = c("ID" = "id", "time" = "time")
          )
        }
      } else if (isTRUE(apply_nonwear)) {
        message("  [NONWEAR] skipped (requires epoch=60s and Vector.Magnitude column).")
      }
      
      joined %>% dplyr::arrange(time)
    })
    
    final_df <- dplyr::bind_rows(results)
    if (!nrow(final_df)) { message("  [SKIP] no rows written"); next }
    
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
