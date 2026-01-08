# ============================================================
# R/ua_run_end_to_end.R
# ============================================================
# Outputs per INPUT FILE (no combining across files):
#   Output1: per-day binned intensity distribution + assigned zones
#   Output2: per-day zone summary (+ "Volume") with NHANES anchor + equated NHANES refs
#   Output3: per-day percentile positioning (Volume + IG) anchored on each metric
#   Output4: daily rows + weekly aggregate rows in ONE file
#   Output5: run report + glossary + run status
#
# Requirements (package functions + data):
#   - load_precomputed_metrics()
#   - ua_assign_bins()
#   - data("ref_metrics_crosswalks"), data("ref_metrics_volume_ig_percentiles")
# ============================================================

ua_run_end_to_end <- function(in_path,
                              out_dir,
                              location = "ndw",
                              make_weekly = TRUE,
                              overwrite = TRUE) {

  stopifnot(requireNamespace("data.table", quietly = TRUE))
  library(data.table)

  # ---------------------------
  # Helpers
  # ---------------------------
  safe_slug <- function(x) {
    x <- as.character(x)
    x <- gsub("\\.[A-Za-z0-9]+$", "", x)
    x <- gsub("[^A-Za-z0-9_-]+", "_", x)
    x <- gsub("_+", "_", x)
    x <- gsub("^_|_$", "", x)
    if (!nzchar(x)) x <- "input"
    x
  }

  ua_write_csv <- function(DT, path) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    if (file.exists(path) && !overwrite) stop("File exists and overwrite=FALSE: ", path, call. = FALSE)
    ok <- tryCatch({ data.table::fwrite(DT, path); TRUE }, error = function(e) FALSE)
    if (ok) return(path)
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.csv$", paste0("_", ts, ".csv"), path)
    data.table::fwrite(DT, path2)
    path2
  }

  ua_write_text <- function(lines, path) {
    dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
    if (file.exists(path) && !overwrite) stop("File exists and overwrite=FALSE: ", path, call. = FALSE)
    ok <- tryCatch({ writeLines(lines, con = path); TRUE }, error = function(e) FALSE)
    if (ok) return(path)
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
    path2 <- sub("\\.txt$", paste0("_", ts, ".txt"), path)
    writeLines(lines, con = path2)
    path2
  }

  canon_metric <- function(x) {
    x <- tolower(trimws(as.character(x)))
    x[x %in% c("mims", "mims_unit")] <- "mims_unit"
    x[x %in% c("ac", "counts", "vm", "vector.magnitude", "vector_magnitude",
               "vector_magnitude_counts", "actigraph counts")] <- "ac"
    x
  }
  canon_anchor <- function(x) canon_metric(x)

  canon_sex <- function(x) {
    s <- tolower(trimws(as.character(x)))
    s[s %in% c("", "na", "n/a", "unknown")] <- NA_character_

    sn <- suppressWarnings(as.numeric(s))
    s[is.finite(sn) & sn == 1] <- "m"
    s[is.finite(sn) & sn == 2] <- "f"

    s[s %in% c("male", "man", "m")] <- "M"
    s[s %in% c("female", "woman", "f")] <- "F"
    s[s %in% c("all", "both", "pooled", "overall")] <- "All"

    s[!(s %in% c("M","F","All"))] <- NA_character_
    s
  }

  ua_stop_epoch <- function(epoch_sec) {
    epoch_sec <- as.integer(epoch_sec)
    if (!(epoch_sec %in% c(5L, 60L))) stop("Only epoch_sec 5 or 60 supported. Got: ", epoch_sec, call. = FALSE)
    epoch_sec
  }

  ua_pick_epoch <- function(DT) {
    if (!("epoch_sec" %in% names(DT))) stop("epoch_sec missing after load_precomputed_metrics().", call. = FALSE)
    ep <- unique(na.omit(as.integer(DT$epoch_sec)))
    if (!length(ep)) stop("epoch_sec missing after load_precomputed_metrics().", call. = FALSE)
    if (length(ep) > 1) stop("Multiple epoch_sec values in file: ", paste(ep, collapse = ", "), call. = FALSE)
    as.integer(ep[1])
  }

  ua_infer_category <- function(age) {
    if (!is.finite(age)) return(NA_character_)
    if (age >= 3  & age <= 11) return("Age-3-11")
    if (age >= 12 & age <= 17) return("Age-12-17")
    if (age >= 18 & age <= 64) return("Age-18-64")
    if (age >= 65) return("Age-65+")
    NA_character_
  }

  ua_weighted_mean <- function(x, w) {
    ok <- is.finite(x) & is.finite(w) & w > 0
    if (!any(ok)) return(NA_real_)
    sum(x[ok] * w[ok]) / sum(w[ok])
  }

  ua_compute_ig_slope <- function(midpoint, minutes) {
    x <- suppressWarnings(as.numeric(midpoint))
    y <- suppressWarnings(as.numeric(minutes))
    ok <- is.finite(x) & is.finite(y) & x > 0 & y > 0
    if (sum(ok) < 3) return(NA_real_)
    fit <- stats::lm(log(y[ok]) ~ log(x[ok]))
    b <- coef(fit)[["log(x[ok])"]]
    if (!is.finite(b)) return(NA_real_)
    unname(b)
  }

  # Zones (fixed 6-band labels from your reference table)
  band6_levels_raw <- c(
    "Minimal intensity zone",
    "Low-to-Moderate intensity zone",
    "Moderate-to-High intensity zone",
    "High-to-Very High intensity zone",
    "Very High-to-Peak intensity zone",
    "Peak intensity zone"
  )

  strip_intensity_zone_phrase <- function(z) {
    z <- as.character(z)
    z[z == "All"] <- "Volume"
    z <- gsub(" intensity zone$", "", z, ignore.case = TRUE)
    z
  }

  get_category_key <- function(category_base, sex) {
    sx <- canon_sex(sex)
    ifelse(sx %in% c("M","F"),
           paste0(as.character(category_base), "_Sex-", sx),
           as.character(category_base))
  }

  # ---------------------------
  # Load embedded datasets
  # ---------------------------
  if (!exists("ref_metrics_crosswalks", inherits = TRUE)) {
    utils::data("ref_metrics_crosswalks", package = "universalaccel", envir = environment())
  }
  if (!exists("ref_metrics_volume_ig_percentiles", inherits = TRUE)) {
    utils::data("ref_metrics_volume_ig_percentiles", package = "universalaccel", envir = environment())
  }
  if (!exists("ref_metrics_crosswalks", inherits = TRUE)) stop("ref_metrics_crosswalks not found.", call. = FALSE)
  if (!exists("ref_metrics_volume_ig_percentiles", inherits = TRUE)) stop("ref_metrics_volume_ig_percentiles not found.", call. = FALSE)

  # ---------------------------
  # Crosswalk normalization + LUT
  # ---------------------------
  normalize_crosswalk <- function(crosswalk_dt, epoch_keep) {
    CW0 <- as.data.table(crosswalk_dt)
    setnames(CW0, tolower(names(CW0)))

    req <- c("anchor","epoch_sec","category","metric","zone",
             "zone_lower","zone_upper","mean_nhanesw","se_nhanesw","total_min")
    miss <- setdiff(req, names(CW0))
    if (length(miss)) stop("ref_metrics_crosswalks missing columns: ", paste(miss, collapse = ", "), call. = FALSE)

    CW <- CW0[, .(
      anchor        = canon_anchor(anchor),
      epoch_sec     = as.integer(epoch_sec),
      category_key  = as.character(category),
      metric        = canon_metric(metric),
      zone          = as.character(zone),
      zone_lower    = suppressWarnings(as.numeric(zone_lower)),
      zone_upper    = suppressWarnings(as.numeric(zone_upper)),
      mean_nhanesw  = suppressWarnings(as.numeric(mean_nhanesw)),
      se_nhanesw    = suppressWarnings(as.numeric(se_nhanesw)),
      total_min_ref = suppressWarnings(as.numeric(total_min))
    )]

    CW[epoch_sec == as.integer(epoch_keep)]
  }

  build_zone_LUTs_for_assignment <- function(CW) {
    Z <- CW[zone %in% band6_levels_raw & metric == anchor]
    Z <- Z[is.finite(zone_lower) & is.finite(zone_upper)]
    setorder(Z, anchor, epoch_sec, category_key, zone)
    Z <- Z[, .SD[1L], by = .(anchor, epoch_sec, category_key, zone)]

    Z[, key := paste0(anchor, "|", epoch_sec, "|", category_key)]
    keys <- unique(Z$key)

    LUT <- setNames(vector("list", length(keys)), keys)
    for (k in keys) {
      zz <- Z[key == k, .(
        band            = zone,
        intensity_lower = zone_lower,
        intensity_upper = zone_upper
      )]
      zz <- zz[match(band6_levels_raw, zz$band)]
      LUT[[k]] <- zz
    }
    LUT
  }

  get_zones_for_group <- function(LUT, anchor, epoch_sec, category_key) {
    LUT[[paste0(anchor, "|", epoch_sec, "|", category_key)]]
  }

  assign_bands_from_zones <- function(x, zones_dt) {
    x <- suppressWarnings(as.numeric(x))
    out <- rep(NA_character_, length(x))
    if (is.null(zones_dt) || !nrow(zones_dt)) return(out)

    rng <- range(c(zones_dt$intensity_lower, zones_dt$intensity_upper), finite = TRUE)
    scale <- max(1, abs(rng[2] - rng[1]))
    tol <- 1e-10 * scale

    zmin <- zones_dt[zones_dt$band == "Minimal intensity zone", ]
    if (nrow(zmin)) {
      lo <- zmin$intensity_lower[1]
      if (is.finite(lo)) out[is.finite(x) & abs(x - lo) <= tol] <- "Minimal intensity zone"
    }

    for (b in band6_levels_raw[band6_levels_raw != "Minimal intensity zone"]) {
      zz <- zones_dt[zones_dt$band == b, ]
      if (!nrow(zz)) next
      lo <- zz$intensity_lower[1]
      hi <- zz$intensity_upper[1]
      idx <- which(is.na(out) & is.finite(x) & (x > lo + tol) & (x <= hi + tol))
      if (length(idx)) out[idx] <- b
    }
    out
  }

  # --------- MINIMAL FIX #1 (prevents Output2 becoming empty) ---------
  # Do NOT drop rows when references are missing. Keep observed rows, add NA refs.
  attach_output2_refs <- function(zone_daily, CW) {
    ZD <- copy(zone_daily)

    # default anchor refs
    ref_default <- CW[metric == anchor & (zone %in% c(band6_levels_raw, "All"))]
    ref_default[, zone_join := zone]
    ref_default[, zone_join := fifelse(zone_join == "All", "All", zone_join)]
    setkey(ref_default, anchor, epoch_sec, category_key, zone_join)

    ZD[, zone_join := fcase(intensity_zone_raw == "Volume", "All", default = intensity_zone_raw)]
    setkey(ZD, anchor, epoch_sec, category_key, zone_join)

    ZD <- merge(
      ZD,
      ref_default[, .(anchor, epoch_sec, category_key, zone_join,
                      nhanes_anchor_zone_lower = zone_lower,
                      nhanes_anchor_zone_upper = zone_upper,
                      nhanes_anchor_mean_nhanesw = mean_nhanesw,
                      nhanes_anchor_se_nhanesw = se_nhanesw,
                      nhanes_anchor_total_min_ref = total_min_ref)],
      by = c("anchor","epoch_sec","category_key","zone_join"),
      all.x = TRUE,
      sort = FALSE
    )

    # peer metric refs (equated columns)
    peers <- CW[metric != anchor & (zone %in% c(band6_levels_raw, "All"))]
    if (nrow(peers)) {
      peers[, zone_join := zone]
      peers[, peer_metric := metric]

      peers_w <- dcast(
        peers,
        anchor + epoch_sec + category_key + zone_join ~ peer_metric,
        value.var = c("mean_nhanesw","se_nhanesw")
      )

      for (m in setdiff(unique(peers$peer_metric), character(0))) {
        if (paste0("mean_nhanesw_", m) %in% names(peers_w))
          setnames(peers_w, paste0("mean_nhanesw_", m), paste0("equated_", m, "_mean_nhanesw"))
        if (paste0("se_nhanesw_", m) %in% names(peers_w))
          setnames(peers_w, paste0("se_nhanesw_", m), paste0("equated_", m, "_se_nhanesw"))
      }

      setkey(peers_w, anchor, epoch_sec, category_key, zone_join)
      ZD <- merge(ZD, peers_w, by = c("anchor","epoch_sec","category_key","zone_join"), all.x = TRUE, sort = FALSE)
    }

    ZD[, zone_join := NULL]
    ZD[]
  }

  # ---------------------------
  # Output3: anchor-centric percentile lookups (Volume + IG)
  # ---------------------------
  build_output3_nhanes_anchor <- function(all_daily, perc_dt) {
    stopifnot(requireNamespace("data.table", quietly = TRUE))
    library(data.table)

    core <- c("ac", "enmo", "mad", "mims_unit")

    A <- as.data.table(all_daily)
    reqA <- c("id","day","metric","epoch_sec","location","sex","age",
              "observed_total_volume_min","observed_mean_intensity","observed_ig")
    missA <- setdiff(reqA, names(A))
    if (length(missA)) stop("Output3: all_daily missing columns: ", paste(missA, collapse=", "), call. = FALSE)

    A[, metric := canon_metric(metric)]
    A <- A[metric %in% core]

    A[, age_i := suppressWarnings(as.integer(round(as.numeric(age))))]
    bad_age_ids <- unique(A[!is.finite(age_i), id])
    if (length(bad_age_ids)) {
      stop(sprintf(
        "Output3 requires valid age for every participant. Missing/invalid age for %d id(s), e.g.: %s",
        length(bad_age_ids),
        paste(head(bad_age_ids, 10), collapse = ", ")
      ), call. = FALSE)
    }

    A[, sex := canon_sex(sex)]
    A[is.na(sex), sex := "All"]

    A[, age_range := fifelse(age_i >= 3 & age_i <= 19, "Age 3-19", "Age 20-80")]
    A[, anchor := canon_anchor(metric)]

    obs_long <- rbindlist(list(
      A[, .(id, day, epoch_sec, location, sex, age = age_i, age_range,
            anchor, stat_lookup = "Average acceleration",
            stat = "Volume",
            anchor_value_observed = as.numeric(observed_mean_intensity))],
      A[, .(id, day, epoch_sec, location, sex, age = age_i, age_range,
            anchor, stat_lookup = "Intensity gradient",
            stat = "Intensity gradient",
            anchor_value_observed = as.numeric(observed_ig))]
    ), use.names = TRUE, fill = TRUE)

    obs_long <- obs_long[is.finite(anchor_value_observed)]
    if (!nrow(obs_long)) stop("Output3: no finite observed values (Volume/IG) found.", call. = FALSE)

    P0 <- as.data.table(perc_dt)
    setnames(P0, tolower(names(P0)))

    sex_col <- if ("sex" %in% names(P0)) "sex" else if ("sex_name" %in% names(P0)) "sex_name" else NA_character_
    if (is.na(sex_col)) stop("ref_metrics_volume_ig_percentiles missing sex column (sex or sex_name).", call. = FALSE)

    reqP <- c("epoch_sec","anchor","metric", sex_col, "age_range","stat","perc","age","value")
    missP <- setdiff(reqP, names(P0))
    if (length(missP)) stop("ref_metrics_volume_ig_percentiles missing columns: ", paste(missP, collapse = ", "), call. = FALSE)

    P <- P0[, .(
      epoch_sec  = as.integer(epoch_sec),
      anchor     = canon_anchor(anchor),
      metric     = canon_metric(metric),
      sex        = canon_sex(get(sex_col)),
      age_range  = as.character(age_range),
      stat       = as.character(stat),
      centile    = suppressWarnings(as.numeric(gsub("[^0-9.]+", "", as.character(perc)))),
      age_i      = suppressWarnings(as.integer(age)),
      value_ref  = suppressWarnings(as.numeric(value))
    )]

    P <- P[is.finite(epoch_sec) & is.finite(centile) & is.finite(age_i) & is.finite(value_ref)]
    P <- P[anchor %in% core & metric %in% core]
    P <- P[sex %in% c("M","F","All")]
    if (!nrow(P)) stop("Output3: reference percentile table has 0 usable rows after cleaning.", call. = FALSE)

    setkey(P, anchor, metric, epoch_sec, sex, age_range, stat, age_i)

    nearest_centile_anchor <- function(anc, ep, sx, ar, st, ag, vobs) {
      S <- P[.(anc, anc, ep, sx, ar, st, as.integer(ag))]
      if (!nrow(S)) return(NA_real_)
      S$centile[which.min(abs(S$value_ref - vobs))]
    }

    value_at_centile <- function(anc, met, ep, sx, ar, st, ag, cent_target) {
      if (!is.finite(cent_target)) return(list(value_ref = NA_real_, centile_used = NA_real_))
      S <- P[.(anc, met, ep, sx, ar, st, as.integer(ag))]
      if (!nrow(S)) return(list(value_ref = NA_real_, centile_used = NA_real_))
      ex <- S[centile == cent_target]
      if (nrow(ex)) return(list(value_ref = ex$value_ref[1], centile_used = cent_target))
      j <- which.min(abs(S$centile - cent_target))
      list(value_ref = S$value_ref[j], centile_used = S$centile[j])
    }

    out_list <- vector("list", nrow(obs_long))
    for (i in seq_len(nrow(obs_long))) {
      r <- obs_long[i]
      anc <- r$anchor
      ep  <- r$epoch_sec
      sx  <- r$sex
      ar  <- r$age_range
      stL <- r$stat_lookup
      ag  <- r$age
      vobs <- r$anchor_value_observed

      p_anc <- nearest_centile_anchor(anc, ep, sx, ar, stL, ag, vobs)

      row_out <- as.list(r[, .(id, day, epoch_sec, location, sex, age, age_range, anchor, stat)])
      row_out$anchor_value_observed <- as.numeric(vobs)
      row_out$anchor_nearest_percentile <- as.numeric(p_anc)

      for (m in core) {
        vm <- value_at_centile(anc, m, ep, sx, ar, stL, ag, p_anc)
        row_out[[paste0("nhanes_", m, "_value_at_anchor_percentile")]] <- as.numeric(vm$value_ref)
        row_out[[paste0("nhanes_", m, "_percentile_used")]] <- as.numeric(vm$centile_used)
      }

      out_list[[i]] <- as.data.table(row_out)
    }

    out <- rbindlist(out_list, use.names = TRUE, fill = TRUE)
    setorder(out, id, day, anchor, stat)
    out[]
  }

  # --------- MINIMAL FIX #2 (Output4 was failing due to wrong column names) ---------
  # Output4 must use observed_* columns from all_daily (not old total_min_day/overall_intensity/ig_slope).
  ua_make_daily_plus_weekly <- function(all_daily) {
    stopifnot(requireNamespace("data.table", quietly = TRUE))
    library(data.table)

    D <- as.data.table(all_daily)
    req <- c("id","day","metric","epoch_sec","location","sex","age","category",
             "observed_total_volume_min","observed_mean_intensity","observed_ig")
    miss <- setdiff(req, names(D))
    if (length(miss)) stop("Output4: all_daily missing columns: ", paste(miss, collapse=", "), call. = FALSE)

    D[, day := as.integer(day)]
    D[, week := as.integer((day - 1L) %/% 7L) + 1L]

    day_rows <- D[, .(
      period_type = "day",
      period_id   = day,
      n_days_in_period = 1L,
      observed_total_minutes = as.numeric(observed_total_volume_min),
      observed_mean_intensity = as.numeric(observed_mean_intensity),
      observed_ig = as.numeric(observed_ig)
    ), by = .(id, metric, epoch_sec, location, sex, age, category)]

    week_rows <- D[, .(
      period_type = "week",
      period_id   = week,
      n_days_in_period = .N,
      observed_total_minutes = sum(as.numeric(observed_total_volume_min), na.rm = TRUE),
      observed_mean_intensity = mean(as.numeric(observed_mean_intensity), na.rm = TRUE),
      observed_ig = mean(as.numeric(observed_ig), na.rm = TRUE)
    ), by = .(id, metric, epoch_sec, location, sex, age, category, week)]

    week_rows[, week := NULL]
    out <- rbindlist(list(day_rows, week_rows), use.names = TRUE, fill = TRUE)
    setorder(out, id, metric, period_type, period_id)
    out[]
  }

  # ============================================================
  # Run ONE FILE (internal)
  # ============================================================
  run_one_file <- function(file_path) {
    dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
    run_date <- format(Sys.Date(), "%Y-%m-%d")
    ts <- format(Sys.time(), "%Y%m%d_%H%M%S")

    status <- list(
      output1 = list(ok = FALSE, path = NA_character_, msg = ""),
      output2 = list(ok = FALSE, path = NA_character_, msg = ""),
      output3 = list(ok = FALSE, path = NA_character_, msg = ""),
      output4 = list(ok = FALSE, path = NA_character_, msg = ""),
      output5 = list(ok = FALSE, path = NA_character_, msg = "")
    )

    DT <- as.data.table(load_precomputed_metrics(
      file_path,
      apply_valid_filters = TRUE,
      valid_day_hours = 16,
      valid_week_days = 0
    ))

    loader_meta <- attr(DT, "ua_loader_meta")
    if (!is.list(loader_meta)) loader_meta <- list()

    if (!("id" %in% names(DT))) stop("No 'id' column after load_precomputed_metrics().", call. = FALSE)
    if (!("time" %in% names(DT))) stop("No 'time' column after load_precomputed_metrics().", call. = FALSE)

    # STRICT: require AGE
    if (!("age" %in% names(DT))) {
      stop("Age is required. Add an 'age' column (integer 3–80).", call. = FALSE)
    }
    DT[, age := suppressWarnings(as.integer(round(as.numeric(age))))]
    if (any(!is.finite(DT$age))) {
      bad_ids <- unique(DT[!is.finite(age), id])
      stop(sprintf(
        "Age is required for every participant. Missing/invalid age for %d id(s), e.g.: %s",
        length(bad_ids),
        paste(head(bad_ids, 10), collapse = ", ")
      ), call. = FALSE)
    }

    # sex optional; default pooled
    if (!("sex" %in% names(DT))) DT[, sex := "All"]
    DT[, sex := canon_sex(sex)]
    DT[is.na(sex), sex := "All"]
    DT[, sex := as.character(sex)]


    epoch_sec <- ua_stop_epoch(ua_pick_epoch(DT))

    input_slug <- safe_slug(basename(file_path))
    run_root <- file.path(out_dir, "UA_runs")
    run_folder <- file.path(run_root, sprintf("%s_epoch%ss_%s", input_slug, epoch_sec, ts))
    dir.create(run_folder, recursive = TRUE, showWarnings = FALSE)

    # Always attempt Output5 at end (even if Output3 fails)
    on.exit({
      `%||%` <- function(x, y) if (is.null(x)) y else x

      metrics_line <- paste(loader_meta$detected_metrics_num %||% character(0), collapse = ", ")
      if (!nzchar(metrics_line)) metrics_line <- "(none detected)"

      out5_path <- file.path(run_folder, sprintf("UA_Output5_LOGISTICS_epoch%ss_%s.txt", epoch_sec, run_date))

      out5_lines <- c(
        "universalaccel — end-to-end run report",
        paste0("Run date: ", run_date),
        paste0("Input file: ", file_path),
        paste0("Output folder (this run): ", run_folder),
        paste0("Epoch length: ", epoch_sec, " seconds"),
        paste0("Location: ", location),
        "",
        "============================================================",
        "WHAT YOU PROVIDED",
        "============================================================",
        paste0("Rows / Columns: ", (loader_meta$n_rows %||% nrow(DT)), " / ", (loader_meta$n_cols %||% ncol(DT))),
        paste0("Participants (unique IDs): ", (loader_meta$n_unique_id %||% uniqueN(DT$id))),
        paste0("Time span (UTC): ", (loader_meta$time_min_utc %||% NA_character_), " to ", (loader_meta$time_max_utc %||% NA_character_)),
        "",
        "Metrics detected in your file:",
        paste0("  • ", metrics_line),
        "",
        "============================================================",
        "WHAT THIS TOOL DID",
        "============================================================",
        "  1) Binned each day into intensity levels (minutes per bin).",
        "  2) Grouped bins into 6 movement zones (plus a full-day 'Volume' row).",
        "  3) Added NHANES reference values for population context.",
        "  4) Created two NHANES-based comparison views:",
        "       • Output2: zone-based (anchor zones + equated NHANES references for other metrics)",
        "       • Output3: percentile-based (anchor percentile + NHANES values at that percentile)",
        "",
        "Important limitations:",
        "  • The file must contain a single epoch length (5s or 60s). Mixed epochs are not supported.",
        "  • Age is required for Output3 (age-specific NHANES lookup).",
        "",
        "============================================================",
        "HOW TO INTERPRET THE OUTPUTS",
        "============================================================",
        "",
        "OUTPUT 1 (BINS: daily intensity distribution):",
        "  • Each row is one person × day × metric × intensity bin.",
        "  • bin_lower / bin_upper : bin edges in the metric's native units.",
        "  • midpoint              : bin midpoint used for summaries (native units).",
        "  • time_bin_min          : observed minutes accumulated in that bin (YOUR data).",
        "  • cm_time_min           : descending cumulative minutes within (id, day, metric).",
        "  • intensity_zone        : zone label assigned using NHANES normative zone bounds.",
        "  • log_midpoint / log_time_bin_min (if present): natural-log transforms used for IG-type summaries.",
        "",
        "OUTPUT 2 (DAILY SUMMARY: time in zones + NHANES references):",
        "  • Each row is one person × day × metric × zone (plus a 'Volume' row).",
        "  • observed_* columns are computed from YOUR data.",
        "  • nhanes_anchor_* columns are NHANES reference values for the anchor metric/zone panel (same anchor, epoch, category_key).",
        "  • equated_<metric>_* columns are NHANES reference mean/SE for other metrics, mapped onto the same anchor-defined zone.",
        "",
        "OUTPUT 3 (PERCENTILES: anchor-conditioned, not zone-based):",
        "  • Each row is one person × day × anchor × stat (Volume or Intensity gradient).",
        "  • Step A: find the nearest NHANES percentile for your observed anchor value (same anchor panel).",
        "  • Step B: at that same percentile, look up NHANES values for ALL metrics under the same anchor panel.",
        "  • nhanes_* columns in Output3 are NHANES reference values (not your observed values for those other metrics).",
        "",
        "OUTPUT 4 (DAILY + WEEKLY):",
        "  • One file containing both day-level rows and week-level rows.",
        "  • period_type indicates whether the row is a day summary or a weekly rollup.",
        "  • Weekly values are averages across the observed days in that week (within id × metric).",
        "",
        "============================================================",
        "OUTPUT FILES (WHAT EACH FILE IS FOR)",
        "============================================================",
        "",
        "OUTPUT 1 — UA_Output1_BINS_*.csv",
        "  Detailed daily intensity distributions (minutes across bins + assigned zones).",
        "",
        "OUTPUT 2 — UA_Output2_DAILY_SUMMARY_*.csv",
        "  Daily zone summaries (including 'Volume') with NHANES anchor references and equated NHANES references.",
        "",
        "OUTPUT 3 — UA_Output3_PERCENTILES_*.csv",
        "  Daily percentile positioning for Volume and Intensity gradient (anchor percentile + NHANES values at that percentile).",
        "",
        "OUTPUT 4 — UA_Output4_DAILY_PLUS_WEEKLY_*.csv ",
        "  Day-level rows plus week-level rollups in a single file (period_type=day/week).",
        "",
        "============================================================",
        "HOW TO READ KEY TERMS / SUFFIXES",
        "============================================================",
        "metric                    : metric being summarized (ac, enmo, mad, mims_unit, ...)",
        "anchor                    : anchor metric defining the NHANES lookup panel",
        "observed_*                : computed from YOUR file (your data)",
        "nhanes_*                  : NHANES reference values (population context)",
        "nhanesw                   : NHANES survey-weighted estimate (reference build used weights/PSU/strata)",
        "mean_nhanesw              : survey-weighted mean (NHANES reference)",
        "se_nhanesw                : standard error of the survey-weighted mean (NHANES reference)",
        "equated_<metric>_*        : NHANES reference values for another metric mapped to the anchor-defined zone (Output2)",
        "category / category_key   : NHANES-style age category (and sex-specific variant if applicable)",
        "intensity_zone            : zone label (Volume, Minimal, ... Peak)",
        "observed_minutes_in_zone  : minutes in zone from YOUR data (Output2)",
        "observed_total_volume_min : total observed minutes in the day (Output4; and used to create the 'Volume' row)",
        "observed_mean_intensity   : time-weighted mean intensity in native units (sometimes called 'volume' in plain language outputs)",
        "observed_ig               : intensity gradient slope computed from your daily distribution",
        "",
        "============================================================",
        "RUN STATUS (DEBUG)",
        "============================================================",
        sprintf("Output1 ok=%s  %s  %s", status$output1$ok, status$output1$path, status$output1$msg),
        sprintf("Output2 ok=%s  %s  %s", status$output2$ok, status$output2$path, status$output2$msg),
        sprintf("Output3 ok=%s  %s  %s", status$output3$ok, status$output3$path, status$output3$msg),
        sprintf("Output4 ok=%s  %s  %s", status$output4$ok, status$output4$path, status$output4$msg),
        "",
        "============================================================",
        "END",
        "============================================================"
      )

      try(ua_write_text(out5_lines, out5_path), silent = TRUE)
      status$output5$ok <- TRUE
      status$output5$path <- out5_path
    }, add = TRUE)

    # demographics per id
    DT[, category := vapply(age, ua_infer_category, character(1))]
    demo_id <- DT[, .(
      sex      = sex[1],
      age      = age[1],
      category = category[1]
    ), by = .(id)]
    demo_id[, sex := as.character(sex)]
    demo_id[, category_key := get_category_key(category, sex)]

    # prep references
    CW <- normalize_crosswalk(ref_metrics_crosswalks, epoch_keep = epoch_sec)
    LUT <- build_zone_LUTs_for_assignment(CW)

    # -----------------------------
    # Output1: bins
    # -----------------------------
    bins <- as.data.table(ua_assign_bins(
      DT        = DT,
      epoch_sec = epoch_sec,
      location  = location,
      per_day   = TRUE,
      id_col    = "id",
      time_col  = "time"
    ))
    if (!nrow(bins)) stop("ua_assign_bins() returned 0 rows.", call. = FALSE)

    bins <- merge(bins, demo_id[, .(id, sex, age, category, category_key)], by = "id", all.x = TRUE, sort = FALSE)
    bins[, metric := canon_metric(metric)]
    bins[, anchor := canon_anchor(metric)]

    bins[, intensity_zone_raw := {
      zdt <- get_zones_for_group(LUT, anchor[1], epoch_sec[1], category_key[1])
      if (is.null(zdt) || !nrow(zdt)) rep(NA_character_, .N) else assign_bands_from_zones(midpoint, zdt)
    }, by = .(id, day, metric, epoch_sec, anchor, category_key)]

    status$output1 <- tryCatch({
      p <- ua_write_csv(bins, file.path(run_folder, sprintf("UA_Output1_BINS_epoch%ss_%s.csv", epoch_sec, run_date)))
      list(ok = TRUE, path = p, msg = "")
    }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
    if (!status$output1$ok) stop(status$output1$msg, call. = FALSE)

    # -----------------------------
    # Daily stats (for Outputs 2–4)
    # -----------------------------
    all_daily <- bins[, .(
      observed_total_volume_min = sum(time_bin_min, na.rm = TRUE),
      observed_mean_intensity   = ua_weighted_mean(midpoint, time_bin_min),
      observed_ig               = ua_compute_ig_slope(midpoint, time_bin_min)
    ), by = .(id, day, metric, epoch_sec, location)]
    all_daily <- merge(all_daily, demo_id[, .(id, sex, age, category, category_key)], by = "id", all.x = TRUE, sort = FALSE)
    all_daily[, metric := canon_metric(metric)]
    all_daily[, anchor := canon_anchor(metric)]

    # -----------------------------
    # Output2: DAILY SUMMARY (zones + Volume)
    # -----------------------------
    zone_obs <- bins[!is.na(intensity_zone_raw),
                     .(
                       observed_minutes_in_zone = sum(time_bin_min, na.rm = TRUE),
                       observed_mean_intensity  = ua_weighted_mean(midpoint, time_bin_min)
                     ),
                     by = .(id, day, metric, epoch_sec, location, anchor, category_key, intensity_zone_raw)
    ]
    zone_obs <- merge(zone_obs, demo_id[, .(id, sex, age, category)], by = "id", all.x = TRUE, sort = FALSE)

    keys2 <- unique(all_daily[, .(id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category)])
    zone_shell <- keys2[, .(intensity_zone_raw = band6_levels_raw),
                        by = .(id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category)]

    zone_daily <- merge(
      zone_shell,
      zone_obs[, .(id, day, metric, epoch_sec, location, anchor, category_key, intensity_zone_raw,
                   observed_minutes_in_zone, observed_mean_intensity)],
      by = c("id","day","metric","epoch_sec","location","anchor","category_key","intensity_zone_raw"),
      all.x = TRUE, sort = FALSE
    )
    zone_daily[is.na(observed_minutes_in_zone), observed_minutes_in_zone := 0]

    vol <- all_daily[, .(
      id, day, metric, epoch_sec, location, anchor, category_key, sex, age, category,
      intensity_zone_raw = "Volume",
      observed_minutes_in_zone = observed_total_volume_min,
      observed_mean_intensity  = observed_mean_intensity
    )]

    zone_daily2 <- rbindlist(list(zone_daily, vol), use.names = TRUE, fill = TRUE)

    # (FIXED) keep observed rows even if NHANES refs don't match
    zone_daily2 <- attach_output2_refs(zone_daily2, CW)

    zone_daily2[, intensity_zone := strip_intensity_zone_phrase(intensity_zone_raw)]
    zone_daily2[, intensity_zone_raw := NULL]

    zone_order_levels <- c("Volume", strip_intensity_zone_phrase(band6_levels_raw))
    zone_daily2[, zone_order := match(intensity_zone, zone_order_levels)]
    setorder(zone_daily2, id, day, metric, zone_order)
    zone_daily2[, zone_order := NULL]

    keep_front <- c(
      "id","day","epoch_sec","location",
      "metric","anchor",
      "sex","age","category","category_key",
      "intensity_zone",
      "observed_minutes_in_zone","observed_mean_intensity",
      "nhanes_anchor_zone_lower","nhanes_anchor_zone_upper",
      "nhanes_anchor_mean_nhanesw","nhanes_anchor_se_nhanesw","nhanes_anchor_total_min_ref"
    )
    equated_cols <- grep("^equated_", names(zone_daily2), value = TRUE)
    out2 <- zone_daily2[, c(keep_front, equated_cols), with = FALSE]

    status$output2 <- tryCatch({
      p <- ua_write_csv(out2, file.path(run_folder, sprintf("UA_Output2_DAILY_SUMMARY_epoch%ss_%s.csv", epoch_sec, run_date)))
      list(ok = TRUE, path = p, msg = "")
    }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
    if (!status$output2$ok) stop(status$output2$msg, call. = FALSE)

    # -----------------------------
    # Output3: PERCENTILES (Volume + IG)
    # -----------------------------
    status$output3 <- tryCatch({
      out3 <- build_output3_nhanes_anchor(all_daily, ref_metrics_volume_ig_percentiles)
      p <- ua_write_csv(out3, file.path(run_folder, sprintf("UA_Output3_PERCENTILES_epoch%ss_%s.csv", epoch_sec, run_date)))
      list(ok = TRUE, path = p, msg = "")
    }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))

    # -----------------------------
    # Output4: DAILY + WEEKLY (default TRUE)
    # -----------------------------
    status$output4 <- list(ok = TRUE, path = NA_character_, msg = "skipped (make_weekly=FALSE)")
    if (isTRUE(make_weekly)) {
      status$output4 <- tryCatch({
        out4_dt <- ua_make_daily_plus_weekly(all_daily)
        p <- ua_write_csv(out4_dt, file.path(run_folder, sprintf("UA_Output4_Week_Summary_epoch%ss_%s.csv", epoch_sec, run_date)))
        list(ok = TRUE, path = p, msg = "wrote daily+weekly rollups")
      }, error = function(e) list(ok = FALSE, path = NA_character_, msg = conditionMessage(e)))
    }

    invisible(status)
  }

  # ============================================================
  # FILE or FOLDER ENTRYPOINT (NO COMBINING)
  # ============================================================
  if (!is.character(in_path) || length(in_path) != 1L || !nzchar(in_path)) {
    stop("in_path must be a single non-empty file or folder path.", call. = FALSE)
  }
  in_path <- tryCatch(normalizePath(in_path, winslash = "/", mustWork = FALSE),
                      error = function(e) in_path)

  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  if (dir.exists(in_path)) {
    files <- list.files(in_path, full.names = TRUE, recursive = FALSE)
    files <- files[grepl("\\.(csv|txt|xlsx|xls|rds)$", files, ignore.case = TRUE)]
    if (!length(files)) stop("Folder contains no supported files (csv/txt/xlsx/xls/rds): ", in_path, call. = FALSE)

    out <- lapply(files, function(f) {
      message("[UA] processing file: ", basename(f))
      run_one_file(f)
    })
    names(out) <- basename(files)
    return(invisible(out))
  }

  if (!file.exists(in_path)) stop("in_path does not exist: ", in_path, call. = FALSE)
  invisible(run_one_file(in_path))
}
