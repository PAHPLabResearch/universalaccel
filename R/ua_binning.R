# R/ua_binning.R

#' Assign bins using embedded NHANES IG reference edges (bin_edges)
#'
#' - Binning ONLY. No zone assignment here.
#' - bin_edges currently exist only for epoch_sec = 5 or 60.
#' - Stops if epoch_sec is not 5 or 60.
#'
#' Output columns:
#' metric, bin_i, id, day (if per_day), n, time_bin_min,
#' lower, upper, midpoint, width, log_midpoint, log_time_bin,
#' epoch_sec, location, cm_time_min
#'
#' Notes:
#' - cm_time_min is computed as descending cumulative minutes within (id,day,metric):
#'   minutes at/above the bin (complementary cumulative), which is commonly used in IG.
#' - If per_day=TRUE and DT has no usable 'day', day is computed as dense index of calendar date per id.
#'
#' @export
ua_assign_bins <- function(DT,
                           epoch_sec,
                           location   = "ndw",
                           metrics    = NULL,
                           per_day    = TRUE,
                           id_col     = "id",
                           time_col   = "time",
                           metric_col = "metric",
                           value_col  = "value") {

  if (!requireNamespace("data.table", quietly = TRUE)) stop("Package 'data.table' is required.")
  library(data.table)

  # ------------------------------------------------------------
  # Canonicalize input DT (schema safety)
  # ------------------------------------------------------------
  DT <- as.data.table(DT)
  if (!nrow(DT)) return(data.table())

  if (!(id_col %in% names(DT))) {
    stop("ua_assign_bins(): DT is missing id_col='", id_col, "'. Columns: ", paste(names(DT), collapse=", "))
  }
  if (isTRUE(per_day) && !(time_col %in% names(DT))) {
    stop("ua_assign_bins(): per_day=TRUE requires time_col='", time_col, "'. Columns: ", paste(names(DT), collapse=", "))
  }

  # Create canonical id/time columns used internally (prevents 'id not found')
  DT <- copy(DT)
  DT[, id__ := as.character(get(id_col))]
  if (isTRUE(per_day)) DT[, time__ := get(time_col)]

  # Robust time type
  if (isTRUE(per_day)) {
    if (!inherits(DT$time__, "POSIXt")) {
      # last resort conversion; loader should already provide POSIXct
      DT[, time__ := as.POSIXct(time__, tz = "UTC")]
    }
  }

  # ------------------------------------------------------------
  # Epoch checks
  # ------------------------------------------------------------
  if (!is.numeric(epoch_sec) || length(epoch_sec) != 1L || !is.finite(epoch_sec)) {
    stop("ua_assign_bins(): epoch_sec must be a single finite number.")
  }
  epoch0 <- as.integer(epoch_sec)
  if (!(epoch0 %in% c(5L, 60L))) {
    stop("ua_assign_bins(): NHANES bin_edges exist only for epoch_sec 5 or 60. Got: ", epoch0)
  }
  loc0 <- as.character(location)

  # If DT contains epoch_sec column, enforce consistency (if present & non-missing)
  if ("epoch_sec" %in% names(DT)) {
    uep <- unique(na.omit(as.integer(DT[["epoch_sec"]])))
    if (length(uep) >= 1L && any(uep != epoch0)) {
      stop("ua_assign_bins(): DT contains epoch_sec values that differ from epoch_sec argument.")
    }
  }

  # ------------------------------------------------------------
  # Day handling (dense index per id)
  # ------------------------------------------------------------
  has_day <- isTRUE(per_day)
  if (has_day) {
    if ("day" %in% names(DT) && any(is.finite(DT[["day"]]))) {
      DT[, day__ := as.integer(day)]
    } else {
      DT[, date__ := as.Date(time__)]
      DT[, day__ := as.integer(frank(date__, ties.method = "dense")), by = id__]
      DT[, date__ := NULL]
    }
  } else {
    DT[, day__ := 1L]
  }

  # ------------------------------------------------------------
  # Load bin_edges
  # ------------------------------------------------------------
  if (!exists("bin_edges", inherits = TRUE)) {
    utils::data("bin_edges", package = "universalaccel", envir = environment())
  }
  if (!exists("bin_edges", inherits = TRUE)) {
    stop("ua_assign_bins(): Dataset `bin_edges` not found. For dev testing: load('data/bin_edges.rda').")
  }

  edges_tbl <- as.data.table(get("bin_edges", inherits = TRUE))
  need_cols <- c("epoch_sec","location","metric","edge_value")
  if (!all(need_cols %in% names(edges_tbl))) {
    stop("ua_assign_bins(): bin_edges must contain columns: ", paste(need_cols, collapse = ", "))
  }

  # normalize types
  edges_tbl <- copy(edges_tbl)
  edges_tbl[, `:=`(
    epoch_sec  = as.integer(epoch_sec),
    location   = as.character(location),
    metric     = tolower(as.character(metric)),
    edge_value = suppressWarnings(as.numeric(edge_value))
  )]
  edges_tbl <- edges_tbl[epoch_sec == epoch0 & location == loc0 & is.finite(edge_value)]
  if (!nrow(edges_tbl)) {
    stop("ua_assign_bins(): No bin_edges found for epoch_sec=", epoch0, " and location='", loc0, "'.")
  }

  # Build per-metric break list
  setorder(edges_tbl, metric, edge_value)
  edges_tbl <- edges_tbl[, .(edge_value = unique(edge_value)), by = metric]
  edges_list <- split(edges_tbl$edge_value, edges_tbl$metric)
  metrics_in_edges <- sort(names(edges_list))

  # ------------------------------------------------------------
  # Determine LONG vs WIDE input
  # ------------------------------------------------------------
  is_long <- (metric_col %in% names(DT)) && (value_col %in% names(DT))

  if (!is_long) {
    # WIDE: metrics are column names
    if (is.null(metrics)) {
      metrics <- intersect(tolower(names(DT)), metrics_in_edges)
    }
    metrics <- tolower(as.character(metrics))
    metrics <- intersect(metrics, metrics_in_edges)
    if (!length(metrics)) {
      stop("ua_assign_bins(): No WIDE metric columns found that match bin_edges. ",
           "Either provide metrics=, or pass LONG (metric/value) columns.")
    }
  }

  # ------------------------------------------------------------
  # Core: bin one metric's values using its breaks
  # ------------------------------------------------------------
  bin_one_metric <- function(V, met, brks) {
    brks <- sort(unique(as.numeric(brks)))
    brks <- brks[is.finite(brks)]
    if (length(brks) < 2L) return(NULL)

    V <- as.data.table(V)
    V <- V[is.finite(value)]
    if (!nrow(V)) return(NULL)

    # cut into [lower, upper)
    idx <- suppressWarnings(cut(V$value, breaks = brks,
                                include.lowest = TRUE,
                                right = FALSE,
                                labels = FALSE))
    idx <- as.integer(idx)

    # push values equal/above max edge into top bin (rare but safe)
    top_bin <- length(brks) - 1L
    idx[is.na(idx) & is.finite(V$value) & V$value >= max(brks)] <- top_bin

    V[, bin_i := idx]
    V <- V[!is.na(bin_i)]
    if (!nrow(V)) return(NULL)

    lower    <- brks[-length(brks)]
    upper    <- brks[-1L]
    midpoint <- (lower + upper) / 2
    width    <- upper - lower

    geom <- data.table(
      bin_i    = seq_along(lower),
      lower    = lower,
      upper    = upper,
      midpoint = midpoint,
      width    = width
    )

    # aggregate
    agg <- V[, .(
      n            = .N,
      time_bin_min = .N * (epoch0 / 60)
    ), by = .(id__, day__, bin_i)]

    # attach geometry
    res <- merge(agg, geom, by = "bin_i", all.x = TRUE, sort = FALSE)
    res[, `:=`(
      metric   = met,
      epoch_sec = epoch0,
      location  = loc0
    )]

    # complementary cumulative minutes (minutes at/above bin)
    setorder(res, id__, day__, metric, lower)
    res[, cm_time_min := rev(cumsum(rev(pmax(time_bin_min, 0)))), by = .(id__, day__, metric)]

    # logs (IG)
    eps <- 1e-9
    res[, `:=`(
      log_midpoint = log(pmax(midpoint, eps)),
      log_time_bin = log(pmax(time_bin_min, eps))
    )]

    res[]
  }

  # ------------------------------------------------------------
  # Run all metrics (build V then bin)
  # ------------------------------------------------------------
  out <- list()
  k <- 0L

  if (is_long) {
    DTL <- DT[, .(
      id__  = id__,
      day__ = day__,
      metric = tolower(as.character(get(metric_col))),
      value  = suppressWarnings(as.numeric(get(value_col)))
    )]

    mets <- intersect(sort(unique(DTL$metric)), metrics_in_edges)
    for (met in mets) {
      brks <- edges_list[[met]]
      if (is.null(brks)) next
      V <- DTL[metric == met, .(id__, day__, value)]
      res <- bin_one_metric(V, met, brks)
      if (!is.null(res) && nrow(res)) { k <- k + 1L; out[[k]] <- res }
    }

  } else {
    # WIDE
    nms_l <- tolower(names(DT))
    for (met in metrics) {
      idx <- match(met, nms_l)
      if (is.na(idx)) next
      colname <- names(DT)[idx]

      brks <- edges_list[[met]]
      if (is.null(brks)) next

      V <- data.table(
        id__  = DT[["id__"]],
        day__ = DT[["day__"]],
        value = suppressWarnings(as.numeric(DT[[colname]]))
      )
      res <- bin_one_metric(V, met, brks)
      if (!is.null(res) && nrow(res)) { k <- k + 1L; out[[k]] <- res }
    }
  }

  if (k == 0L) return(data.table())

  bins <- rbindlist(out, use.names = TRUE, fill = TRUE)

  # ------------------------------------------------------------
  # Finalize output schema (canonical names)
  # ------------------------------------------------------------
  setnames(bins, c("id__", "day__"), c("id", "day"))
  if (!has_day) bins[, day := NULL] # in case you truly don't want day output when per_day=FALSE

  # reorder columns to match documented output
  want <- c("metric","bin_i","id","day","n","time_bin_min",
            "lower","upper","midpoint","width",
            "log_midpoint","log_time_bin",
            "epoch_sec","location","cm_time_min")
  have <- names(bins)
  bins <- bins[, intersect(want, have), with = FALSE]

  bins[]
}
