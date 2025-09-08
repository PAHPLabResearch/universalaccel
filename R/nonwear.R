#' Compute Choi et al. (2011) non-wear on minute-level VM
#' data must be one row per minute per id: columns id, time (POSIXct, minute), vector_magnitude
#' @noRd
compute_choi_nonwear_minutes <- function(data,
                                         id_col   = "id",
                                         time_col = "time",
                                         cpm_col  = "vector_magnitude",
                                         min_bout = 90L,
                                         spike_tol = 2L,
                                         spike_upper = 100L,
                                         flank = 30L) {
  df <- data %>%
    dplyr::select(dplyr::all_of(c(id_col, time_col, cpm_col))) %>%
    dplyr::arrange(.data[[id_col]], .data[[time_col]])
  data.table::setDT(df)
  data.table::setnames(df, c(id_col, time_col, cpm_col), c("ID__", "TIME__", "CPM__"))
  df[, zeros := (CPM__ == 0)]
  df[, low   := (CPM__ > 0 & CPM__ <= spike_upper)]
  
  valid_spike <- function(z, idx, flank) {
    n <- length(z)
    if (idx - flank < 1 || idx + flank > n) return(FALSE)
    all(z[(idx - flank):(idx - 1)]) && all(z[(idx + 1):(idx + flank)])
  }
  
  df[, choi_nonwear := {
    z <- zeros; l <- low; n <- .N; nonwear <- rep(FALSE, n)
    i <- 1L
    while (i <= n) {
      if (z[i]) {
        j <- i; spikes <- 0L
        while (j <= n && (z[j] || (l[j] && spikes < spike_tol && valid_spike(z, j, flank)))) {
          if (l[j]) spikes <- spikes + 1L
          j <- j + 1L
        }
        if ((j - i) >= min_bout) nonwear[i:(j - 1L)] <- TRUE
        i <- j
      } else i <- i + 1L
    }
    nonwear
  }, by = ID__]
  
  out <- tibble::as_tibble(df)[, c("ID__", "TIME__", "choi_nonwear")]
  names(out) <- c(id_col, time_col, "choi_nonwear")
  data %>% dplyr::left_join(out, by = c(id_col, time_col))
}
