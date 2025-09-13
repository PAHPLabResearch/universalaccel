# R/nonwear.R

suppressPackageStartupMessages({
  library(dplyr)
})

# data must be 1 row per minute per id, with: id, time (POSIXct), vector_magnitude
compute_choi_nonwear_minutes <- function(data,
                                         id_col   = "id",
                                         time_col = "time",
                                         cpm_col  = "vector_magnitude",
                                         min_bout = 90L,
                                         spike_tol = 2L,
                                         spike_upper = 100L,
                                         flank = 30L) {
  
  df <- data %>%
    dplyr::select(all_of(c(id_col, time_col, cpm_col))) %>%
    arrange(.data[[id_col]], .data[[time_col]]) %>%
    rename(ID__   = !!id_col,
           TIME__ = !!time_col,
           CPM__  = !!cpm_col)
  
  # Per-ID Choi implementation without data.table
  mark_id <- function(x) {
    z <- (x$CPM__ == 0L)
    l <- (x$CPM__ > 0L & x$CPM__ <= spike_upper)
    n <- length(z)
    nonwear <- rep(FALSE, n)
    
    valid_spike <- function(idx) {
      if (idx - flank < 1L || idx + flank > n) return(FALSE)
      all(z[(idx - flank):(idx - 1L)]) && all(z[(idx + 1L):(idx + flank)])
    }
    
    i <- 1L
    while (i <= n) {
      if (z[i]) {
        j <- i
        spikes <- 0L
        while (j <= n && (z[j] || (l[j] && spikes < spike_tol && valid_spike(j)))) {
          if (l[j]) spikes <- spikes + 1L
          j <- j + 1L
        }
        if ((j - i) >= min_bout) nonwear[i:(j - 1L)] <- TRUE
        i <- j
      } else {
        i <- i + 1L
      }
    }
    nonwear
  }
  
  out <- df %>%
    group_by(ID__) %>%
    group_modify(~ mutate(.x, choi_nonwear = mark_id(.x))) %>%
    ungroup() %>%
    dplyr::select(ID__, TIME__, choi_nonwear)
  
  out %>%
    rename(!!id_col := ID__, !!time_col := TIME__)
}
