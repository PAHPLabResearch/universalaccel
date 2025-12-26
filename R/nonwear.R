# R/nonwear.R
#' @noRd
# Epoch-aware Choi wear/nonwear wrapper using PhysicalActivity::wearingMarking()
# - Works for any fixed epoch that divides 60 (1,2,3,4,5,6,10,12,15,20,30,60)
# - streamFrame=NULL mirrors package default behavior
# - Returns: id, time, choi_nonwear (logical)
compute_choi_nonwear <- function(data,
                                 id_col    = "id",
                                 time_col  = "time",
                                 cpm_col   = "vector_magnitude",
                                 frame     = 90L,
                                 allowance = 2L,
                                 streamFrame = NULL,
                                 epoch_sec = NULL,
                                 getMinuteMarking = FALSE,
                                 tz        = "UTC") {

  if (!requireNamespace("PhysicalActivity", quietly = TRUE)) {
    stop("Package 'PhysicalActivity' is required. Install it with install.packages('PhysicalActivity').")
  }
  if (!requireNamespace("dplyr", quietly = TRUE) ||
      !requireNamespace("rlang", quietly = TRUE)) {
    stop("Packages 'dplyr' and 'rlang' are required.")
  }

  infer_epoch_sec <- function(tt) {
    tt <- tt[!is.na(tt)]
    if (length(tt) < 10) return(NA_integer_)
    dd <- as.numeric(diff(tt), units = "secs")
    dd <- dd[is.finite(dd) & dd > 0]
    if (!length(dd)) return(NA_integer_)
    as.integer(round(stats::median(dd)))
  }

  df <- data %>%
    dplyr::select(dplyr::all_of(c(id_col, time_col, cpm_col))) %>%
    dplyr::arrange(.data[[id_col]], .data[[time_col]]) %>%
    dplyr::rename(
      ID__   = !!rlang::sym(id_col),
      TIME__ = !!rlang::sym(time_col),
      CPM__  = !!rlang::sym(cpm_col)
    ) %>%
    dplyr::mutate(
      TimeStamp = as.POSIXct(.data$TIME__, tz = tz),
      counts    = as.integer(round(pmax(0, .data$CPM__)))
    )

  marked <- df %>%
    dplyr::group_by(ID__) %>%
    dplyr::group_modify(function(x, key) {

      if (!nrow(x)) {
        return(dplyr::tibble(
          TIME__ = as.POSIXct(character(), tz = tz),
          choi_nonwear = logical()
        ))
      }

      ep <- epoch_sec
      if (is.null(ep)) ep <- infer_epoch_sec(x$TimeStamp)
      if (!is.finite(ep) || ep <= 0L || (60L %% ep) != 0L) {
        stop(sprintf("Invalid epoch_sec for ID=%s: %s (must be a positive divisor of 60).",
                     as.character(key$ID__[1]), as.character(ep)))
      }
      perMinuteCts <- as.integer(60L / ep)

      # IMPORTANT: base data.frame so dataset[, TS] yields a vector (not tibble)
      dat_in <- as.data.frame(x[, c("TimeStamp", "counts")])

      wm <- PhysicalActivity::wearingMarking(
        dataset          = dat_in,
        frame            = as.integer(frame),
        perMinuteCts     = perMinuteCts,
        TS               = "TimeStamp",
        cts              = "counts",
        streamFrame      = streamFrame,          # NULL mirrors package default
        allowanceFrame   = as.integer(allowance),
        newcolname       = "wearing__",
        getMinuteMarking = isTRUE(getMinuteMarking),
        tz               = tz
      )

      wm <- as.data.frame(wm)
      wm$TimeStamp <- as.POSIXct(wm$TimeStamp, tz = tz)

      if (isTRUE(getMinuteMarking)) {
        # Minute-marking output (minute rows); dplyr adds ID__ automatically
        return(dplyr::tibble(
          TIME__       = wm$TimeStamp,
          choi_nonwear = (wm$wearing__ == "nw")
        ))
      }

      # Epoch-by-epoch: align to original timestamps
      out <- merge(
        x = data.frame(TimeStamp = x$TimeStamp, stringsAsFactors = FALSE),
        y = data.frame(TimeStamp = wm$TimeStamp,
                       choi_nonwear = (wm$wearing__ == "nw"),
                       stringsAsFactors = FALSE),
        by = "TimeStamp",
        all.x = TRUE,
        sort = FALSE
      )

      out$choi_nonwear[is.na(out$choi_nonwear)] <- FALSE

      dplyr::tibble(
        TIME__       = as.POSIXct(out$TimeStamp, tz = tz),
        choi_nonwear = out$choi_nonwear
      )
    }) %>%
    dplyr::ungroup() %>%
    dplyr::rename(
      !!id_col   := ID__,
      !!time_col := TIME__
    )

  marked
}
