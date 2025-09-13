# ===============================
# Shiny App: Day-level only • BA • MAPE • Corr • Equivalence
# ===============================

options(shiny.maxRequestSize = 200*1024^2)

suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(DT)
  library(dplyr)
  library(tidyr)
  library(purrr)
  library(readr)
  library(ggplot2)
  library(scales)
  library(gridExtra)
  library(equivalence)
  library(officer)
  library(flextable)
  
})

# ---------------------------------
# tiny helpers
# ---------------------------------
`%||%` <- function(a, b) if (!is.null(a)) a else b

guess_id_col <- function(df) {
  nms <- names(df)
  cand <- c("ID","Id","id",
            grep("^(participant|subject).*id$|^(participant|subject)$|^studyid$",
                 nms, ignore.case = TRUE, value = TRUE))
  intersect(cand, nms)[1] %||% NA_character_
}

# keep only public IDs (F3BA..., FL4...)
public_subset <- function(df, id_col = guess_id_col(df)) {
  if (is.na(id_col)) return(df)
  keep <- grepl("^(F3BA|FL4)", df[[id_col]])
  df[keep %in% TRUE, , drop = FALSE]
}

# -------------------------------
# Portable data loader
# -------------------------------

data_file <- system.file("shiny/calibrateddata/devicefinaldataday.csv",
                         package = "universal_accel")
if (identical(data_file, "") || !file.exists(data_file)) {
  data_file <- "devicefinaldataday.csv"  # works when you run from inst/shiny/calibrateddata
}
DF_DAY <- readr::read_csv(data_file, show_col_types = FALSE)
DF_DAY_PUBLIC <- public_subset(DF_DAY) 

# -------------------------------
# Metric lists & helpers
# -------------------------------
metric_names <- c("AC","AI","ENMO","MAD","MIMS","ROCAM")  # alphabetical

metric_cols <- list(
  AC   = list(nd = c("AG_AC(Nd)","AX_AC(Nd)","GA_AC(Nd)"),
              d  = c("AG_AC(D)","AX_AC(D)","GA_AC(D)")),
  AI   = list(nd = c("AG_AI(Nd)","AX_AI(Nd)","GA_AI(Nd)"),
              d  = c("AG_AI(D)","AX_AI(D)","GA_AI(D)")),
  ENMO = list(nd = c("AG_ENMO(Nd)","AX_ENMO(Nd)","GA_ENMO(Nd)"),
              d  = c("AG_ENMO(D)","AX_ENMO(D)","GA_ENMO(D)")),
  MAD  = list(nd = c("AG_MAD(Nd)","AX_MAD(Nd)","GA_MAD(Nd)"),
              d  = c("AG_MAD(D)","AX_MAD(D)","GA_MAD(D)")),
  MIMS = list(nd = c("AG_MIMS(Nd)","AX_MIMS(Nd)","GA_MIMS(Nd)"),
              d  = c("AG_MIMS(D)","AX_MIMS(D)","GA_MIMS(D)")),
  ROCAM= list(nd = c("AG_ROCAM(Nd)","AX_ROCAM(Nd)","GA_ROCAM(Nd)"),
              d  = c("AG_ROCAM(D)","AX_ROCAM(D)","GA_ROCAM(D)"))
)

prep_df <- function(data, cols) {
  keep <- intersect(cols, names(data))
  if (!length(keep)) return(NULL)
  dplyr::select(data, dplyr::all_of(keep))  # keep NAs; pairwise cor handles it
}

# -------------------------------
# MAPE from day-level data
# -------------------------------
compute_total_mape_from_day <- function(df) {
  make_pairs <- function(metric) {
    tibble(
      A  = c(paste0("AG_",metric,"(Nd)"), paste0("AG_",metric,"(D)")),
      BX = c(paste0("AX_",metric,"(Nd)"), paste0("AX_",metric,"(D)")),
      BG = c(paste0("GA_",metric,"(Nd)"), paste0("GA_",metric,"(D)")),
      Side = c("Non-Dominant","Dominant")
    )
  }
  metrics_readable <- c(AC="Activity Counts", AI="Activity Index", ENMO="ENMO", MAD="MAD", MIMS="MIMS", ROCAM="ROCAM")
  out <- list()
  for (metric in names(metric_cols)) {
    pairs <- make_pairs(metric)
    for (i in seq_len(nrow(pairs))) {
      for (rhs in c("BX","BG")) {
        a <- pairs$A[i]; b <- pairs[[rhs]][i]
        if (!(a %in% names(df) && b %in% names(df))) next
        tmp <- df %>% filter(!is.na(.data[[a]]) & !is.na(.data[[b]]))
        if (!nrow(tmp)) next
        tmp <- tmp %>%
          mutate(
            MAE      = abs(.data[[a]] - .data[[b]]),
            MAPE_raw = abs((.data[[a]] - .data[[b]]) / pmax(1, .data[[a]])) * 100
          )
        out[[length(out)+1]] <- tmp %>%
          summarise(
            Total_Minutes = n(),
            MeanDevice1   = mean(.data[[a]], na.rm = TRUE),
            MeanDevice2   = mean(.data[[b]], na.rm = TRUE),
            MeanDifference= MeanDevice1 - MeanDevice2,
            Total_MAE     = mean(MAE,      na.rm = TRUE),
            Total_MAPE    = mean(MAPE_raw, na.rm = TRUE),
            SD_MAPE       = sd(MAPE_raw,   na.rm = TRUE),
            .groups = "drop"
          ) %>%
          mutate(
            Comparison = paste(metrics_readable[[metric]], "-", tolower(sub("_.*","",a)), "and", tolower(sub("_.*","",b))),
            Side = pairs$Side[i]
          )
      }
    }
  }
  bind_rows(out)
}

# -------------------------------
# Bland–Altman (2×2) — desired layout
# TL: AG D vs AX D,  TR: AG Nd vs GA Nd,
# BL: AG Nd vs AX Nd, BR: AG D vs GA D
# -------------------------------
ba_pairs_for_metric <- function(metric) {
  list(
    c(sprintf("AG_%s(D)",  metric), sprintf("AX_%s(D)",  metric)),  # TL
    c(sprintf("AG_%s(Nd)", metric), sprintf("GA_%s(Nd)", metric)),  # TR
    c(sprintf("AG_%s(Nd)", metric), sprintf("AX_%s(Nd)", metric)),  # BL
    c(sprintf("AG_%s(D)",  metric), sprintf("GA_%s(D)",  metric))   # BR
  )
}

ba_data_for_metric <- function(df, metric) {
  pairs <- ba_pairs_for_metric(metric)
  desired_levels <- c(
    sprintf("AG_%s(D) vs AX_%s(D)",   metric, metric),   # top-left
    sprintf("AG_%s(Nd) vs GA_%s(Nd)", metric, metric),   # top-right
    sprintf("AG_%s(Nd) vs AX_%s(Nd)", metric, metric),   # bottom-left
    sprintf("AG_%s(D) vs GA_%s(D)",   metric, metric)    # bottom-right
  )
  d <- dplyr::bind_rows(lapply(pairs, function(p) {
    a <- p[1]; b <- p[2]
    if (!(a %in% names(df) && b %in% names(df))) return(NULL)
    dd <- dplyr::filter(df, !is.na(.data[[a]]) & !is.na(.data[[b]]))
    if (!nrow(dd)) return(NULL)
    diffs <- dd[[a]] - dd[[b]]
    tibble::tibble(
      Mean       = (dd[[a]] + dd[[b]]) / 2,
      Difference = diffs,
      Comparison = sprintf("%s vs %s", a, b),
      Bias = mean(diffs, na.rm = TRUE),
      ULoA = mean(diffs, na.rm = TRUE) + 1.96 * stats::sd(diffs, na.rm = TRUE),
      LLoA = mean(diffs, na.rm = TRUE) - 1.96 * stats::sd(diffs, na.rm = TRUE)
    )
  }))
  if (is.null(d) || !nrow(d)) return(NULL)
  present <- intersect(desired_levels, unique(d$Comparison))
  d$Comparison <- factor(d$Comparison, levels = present)  # facet_wrap fills row-wise
  d
}

ba_plot <- function(d, metric_title = NULL) {
  ref <- d %>%
    dplyr::group_by(Comparison) %>%
    dplyr::summarise(
      Bias = dplyr::first(Bias),
      ULoA = dplyr::first(ULoA),
      LLoA = dplyr::first(LLoA),
      .groups = "drop"
    )
  ggplot2::ggplot(d, ggplot2::aes(x = Mean, y = Difference)) +
    ggplot2::geom_point(alpha = 0.75, size = 1.8) +
    ggplot2::geom_smooth(method = "lm", se = FALSE, linetype = "dashed", linewidth = 1) +
    ggplot2::geom_hline(data = ref, ggplot2::aes(yintercept = Bias), linewidth = 1) +
    ggplot2::geom_hline(data = ref, ggplot2::aes(yintercept = ULoA), linetype = "dashed", linewidth = 0.9) +
    ggplot2::geom_hline(data = ref, ggplot2::aes(yintercept = LLoA), linetype = "dashed", linewidth = 0.9) +
    ggplot2::facet_wrap(~ Comparison, ncol = 2, scales = "free") +
    ggplot2::labs(
      title = paste0("Bland–Altman (", metric_title %||% "", ")"),
      x = "Mean of Measurements", y = "Difference"
    ) +
    ggplot2::theme_minimal(base_size = 16) +
    ggplot2::theme(
      plot.title   = ggplot2::element_text(face = "bold", hjust = 0.5, size = 18),
      strip.text   = ggplot2::element_text(size = 14, face = "bold"),
      panel.spacing = grid::unit(1.2, "lines"),
      # border around the whole 2×2 to “box” each metric in the 3×2 grid
      plot.background = ggplot2::element_rect(color = "grey70", fill = NA, linewidth = 0.8),
      # light border around each facet panel
      panel.border   = ggplot2::element_rect(color = "grey85", fill = NA, linewidth = 0.6)
    )
}

ba_all_metrics_grid <- function(df) {
  plots <- lapply(metric_names, function(m) {
    dd <- ba_data_for_metric(df, m)
    if (is.null(dd) || !nrow(dd)) {
      ggplot2::ggplot() + ggplot2::theme_void() +
        ggplot2::labs(title = paste("No data for", m)) +
        ggplot2::theme(plot.background = ggplot2::element_rect(color = "grey70", fill = NA))
    } else {
      ba_plot(dd, m)
    }
  })
  gridExtra::grid.arrange(grobs = plots, ncol = 2)  # 3 rows × 2 cols
}

# -------------------------------
# Correlation tiles (2×2)
# -------------------------------
corr_tile <- function(df, title_text) {
  if (is.null(df) || ncol(df) < 2L || nrow(df) < 3L) return(NULL)
  ag_cols    <- grep("^AG_", names(df), value = TRUE)
  other_cols <- grep("^(AX|GA)_", names(df), value = TRUE)
  if (!length(ag_cols) || !length(other_cols)) return(NULL)
  
  pairs <- tidyr::expand_grid(Y = ag_cols, X = other_cols) %>%
    rowwise() %>%
    mutate(r = suppressWarnings(cor(df[[X]], df[[Y]], use = "pairwise.complete.obs", method = "pearson"))) %>%
    ungroup() %>%
    mutate(
      X = factor(X, levels = unique(other_cols)),
      Y = factor(Y, levels = rev(unique(ag_cols)))
    )
  
  ggplot(pairs, aes(x = X, y = Y, fill = r)) +
    geom_tile(color = "white") +
    geom_text(aes(label = sprintf("%.2f", r)), size = 4.5) +
    scale_fill_gradient2(low = "blue", mid = "white", high = "red",
                         limits = c(-1, 1), midpoint = 0, name = "r",
                         oob = scales::squish) +
    labs(title = title_text, x = "Comparison Device Metrics", y = "ActiGraph Metrics") +
    theme_minimal(base_size = 16) +
    theme(
      plot.title = element_text(hjust = 0.5, face = "bold", size = 18),
      axis.text.x = element_text(angle = 45, hjust = 1, size = 11),
      axis.text.y = element_text(size = 11),
      legend.position = "none"
    )
}

corr_four_panel <- function(df) {
  groups <- list(
    list(title = "Non-Dominant: ActiGraph vs Axivity",
         cols  = c("AG_AC(Nd)","AX_AC(Nd)","AG_AI(Nd)","AX_AI(Nd)","AG_ENMO(Nd)","AX_ENMO(Nd)","AG_MAD(Nd)","AX_MAD(Nd)","AG_MIMS(Nd)","AX_MIMS(Nd)","AG_ROCAM(Nd)","AX_ROCAM(Nd)")),
    list(title = "Dominant: ActiGraph vs GENEActiv",
         cols  = c("AG_AC(D)","GA_AC(D)","AG_AI(D)","GA_AI(D)","AG_ENMO(D)","GA_ENMO(D)","AG_MAD(D)","GA_MAD(D)","AG_MIMS(D)","GA_MIMS(D)","AG_ROCAM(D)","GA_ROCAM(D)")),
    list(title = "Non-Dominant: ActiGraph vs GENEActiv",
         cols  = c("AG_AC(Nd)","GA_AC(Nd)","AG_AI(Nd)","GA_AI(Nd)","AG_ENMO(Nd)","GA_ENMO(Nd)","AG_MAD(Nd)","GA_MAD(Nd)","AG_MIMS(Nd)","GA_MIMS(Nd)","AG_ROCAM(Nd)","GA_ROCAM(Nd)")),
    list(title = "Dominant: ActiGraph vs Axivity",
         cols  = c("AG_AC(D)","AX_AC(D)","AG_AI(D)","AX_AI(D)","AG_ENMO(D)","AX_ENMO(D)","AG_MAD(D)","AX_MAD(D)","AG_MIMS(D)","AX_MIMS(D)","AG_ROCAM(D)","AX_ROCAM(D)"))
  )
  plots <- lapply(groups, function(g) {
    dff <- prep_df(DF_DAY, g$cols)
    p   <- corr_tile(dff, g$title)
    if (is.null(p)) ggplot() + theme_void() + labs(title = g$title) else p
  })
  gridExtra::grid.arrange(grobs = plots, ncol = 2)
}

# -------------------------------
# Equivalence (TOST) — single epsilon%
# -------------------------------
all_cols_groups <- list(
  ENMO = c("AG_ENMO(Nd)","AG_ENMO(D)","AX_ENMO(Nd)","AX_ENMO(D)","GA_ENMO(Nd)","GA_ENMO(D)"),
  MAD  = c("AG_MAD(Nd)","AG_MAD(D)","AX_MAD(Nd)","AX_MAD(D)","GA_MAD(Nd)","GA_MAD(D)"),
  MIMS = c("AG_MIMS(Nd)","AG_MIMS(D)","AX_MIMS(Nd)","AX_MIMS(D)","GA_MIMS(Nd)","GA_MIMS(D)"),
  `Activity Index` = c("AG_AI(Nd)","AG_AI(D)","AX_AI(Nd)","AX_AI(D)","GA_AI(Nd)","GA_AI(D)"),
  `Activity Counts`= c("AG_AC(Nd)","AG_AC(D)","AX_AC(Nd)","AX_AC(D)","GA_AC(Nd)","GA_AC(D)"),
  ROCAM= c("AG_ROCAM(Nd)","AG_ROCAM(D)","AX_ROCAM(Nd)","AX_ROCAM(D)","GA_ROCAM(Nd)","GA_ROCAM(D)")
)
generate_pairs <- function(columns) combn(columns, 2, simplify = FALSE)
all_pairs <- unlist(lapply(all_cols_groups, generate_pairs), recursive = FALSE)

run_tost_pair <- function(df, pair, eps_pct) {
  ref <- df[[pair[1]]]; test <- df[[pair[2]]]
  ok  <- complete.cases(ref, test)
  ref <- ref[ok]; test <- test[ok]
  if (length(ref) < 3 || length(test) < 3) return(NULL)
  eps_val <- (eps_pct/100) * mean(ref, na.rm = TRUE)
  res <- tryCatch(equivalence::tost(ref, test, epsilon = eps_val, paired = TRUE), error = function(e) NULL)
  if (is.null(res)) return(NULL)
  tibble(
    Variable1 = pair[1], Variable2 = pair[2], EpsilonPct = eps_pct,
    MeanDiff = as.numeric(res$estimate),
    PValue   = as.numeric(res$tost.p.value),
    CILower  = as.numeric(res$tost.interval[1]),
    CIUpper  = as.numeric(res$tost.interval[2]),
    EquLower = -eps_val, EquUpper = eps_val
  )
}

run_tost_all <- function(df, pairs, eps_pct) {
  out <- lapply(pairs, function(p) run_tost_pair(df, p, eps_pct))
  bind_rows(out)
}

prep_eq_plot <- function(res, scale_ac = TRUE) {
  res %>%
    mutate(
      Comparison = paste(Variable1, "and", Variable2),
      MetricType = case_when(
        grepl("AC",   Variable1) ~ "Activity Counts",
        grepl("AI",   Variable1) ~ "Activity Index",
        grepl("ENMO", Variable1) ~ "ENMO",
        grepl("MAD",  Variable1) ~ "MAD",
        grepl("MIMS", Variable1) ~ "MIMS",
        grepl("ROCAM",Variable1) ~ "ROCAM",
        TRUE ~ "Other"
      ),
      StatisticallyEquivalent = ifelse(PValue < 0.05, "Equivalent", "Not Equivalent")
    ) %>%
    mutate(across(c(MeanDiff,CILower,CIUpper,EquLower,EquUpper), as.numeric)) %>%
    {
      if (scale_ac)
        mutate(., across(c(MeanDiff,CILower,CIUpper,EquLower,EquUpper),
                         ~ ifelse(MetricType == "Activity Counts", .x/20, .x)))
      else .
    }
}

eq_plot <- function(df) {
  ggplot(df, aes(x = MeanDiff, y = Comparison)) +
    geom_errorbarh(aes(xmin = CILower, xmax = CIUpper),
                   height = 0.25, linewidth = 0.9, color = "#D55E00") +
    geom_errorbarh(aes(xmin = EquLower, xmax = EquUpper),
                   height = 0.25, linewidth = 0.9, color = "#0072B2", linetype = "dashed") +
    geom_point(aes(color = StatisticallyEquivalent), size = 1.8) +
    scale_color_manual(values = c("Equivalent" = "#009E73", "Not Equivalent" = "#3C3C3C")) +
    labs(title = "Equivalence (TOST) at selected epsilon (%)",
         x = "Mean Difference", y = NULL) +
    theme_minimal(base_size = 16) +
    theme(plot.title = element_text(face = "bold", hjust = 0.5, size = 18),
          legend.position = "bottom") +
    facet_wrap(~ MetricType, scales = "free", ncol = 2)
}

# ===============================
# UI
# ===============================
app_theme <- bs_theme(version = 5, bootswatch = "cosmo")

ui <- page_fluid(
  theme = app_theme,
  title = "Temporally Matched Accelerometry Metrics — Day-level Results",
  layout_sidebar(
    sidebar = sidebar(
      h4("Controls"),
      sliderInput("epsilon", "Epsilon (% of reference mean)", min = 0, max = 100, value = 10, step = 1),
      checkboxInput("scale_ac", "Scale Activity Counts (÷20) in equivalence", TRUE),
      hr(),
      downloadButton("dl_ba_pdf",   "Download BA (PDF)"),
      downloadButton("dl_mape_pdf", "Download MAPE (PDF)"),
      downloadButton("dl_corr_pdf", "Download Correlation (PDF)"),
      downloadButton("dl_eq_pdf",   "Download Equivalence Plot (PDF)"),
      downloadButton("dl_eq_docx",  "Download Equivalence Results (DOCX)")
    ),
    navset_card_pill(
      nav_panel("Overview",
                p("This app provides graphical results agreement of acceleration summary metrics from research-grade activity trackers."),
                p("The evaluation used three devices; ActiGraph, Axivity, and GENEActiv with the ActiGraph as the reference in most comparisons"),
                p("All figures are computed from a day-level dataset (see the Data tab).")
      ),
      nav_panel("Bland–Altman (All six metrics)",
                card(card_header("Plot"), plotOutput("ba_all_plot", height = 1650))
      ),
      nav_panel("MAPE (Totals)",
                card(card_header("Plot"), plotOutput("mape_plot", height = 500))
      ),
      nav_panel("Correlation tiles",
                card(card_header("Plots (2×2)"), plotOutput("corr_plot", height = 800))
      ),
      nav_panel("Equivalence (TOST)",
                navset_tab(
                  nav_panel("Results (table)",
                            card(card_header("Table"), DTOutput("eq_table"))
                  ),
                  nav_panel("Plot",
                            card(card_header("Plot"), plotOutput("eq_plot", height = 800))
                  )
                )
      ),
      nav_panel("Data",
                card(
                  card_header("devicefinaldataday"),
                  p("This extract shows only a few rows for demonstration "),
                  div(style = "display:flex; gap:8px; margin-bottom:8px;",
                      downloadButton("dl_data_csv", "Download CSV (public)"),
                      downloadButton("dl_data_rds", "Download RDS (public)")
                  ),
                  DTOutput("day_table")
                )
      )
    )
  )
)

# ===============================
# Server
# ===============================
server <- function(input, output, session) {
  # ---------------- BA (all six) ----------------
  output$ba_all_plot <- renderPlot({
    ba_all_metrics_grid(DF_DAY)
  })
  output$dl_ba_pdf <- downloadHandler(
    filename = function() "BA_all_metrics.pdf",
    content = function(file) {
      grDevices::cairo_pdf(file, width = 11, height = 9)
      for (m in metric_names) {
        dd <- ba_data_for_metric(DF_DAY, m)
        if (!is.null(dd) && nrow(dd) > 0) print(ba_plot(dd, m)) else plot.new()
      }
      dev.off()
    }
  )
  
  # ---------------- MAPE ----------------
  totals_df <- reactive({ compute_total_mape_from_day(DF_DAY) })
  output$mape_plot <- renderPlot({
    stats <- totals_df(); validate(need(nrow(stats) > 0, "MAPE: no rows to plot."))
    metric_order <- c("AC","AI","ENMO","MAD","MIMS","ROCAM")
    mape_total_avg <- stats %>%
      separate(Comparison, into = c("Metric", "DeviceComparison"), sep = " - ", extra = "merge") %>%
      mutate(Metric = recode(Metric, "Activity Counts" = "AC", "Activity Index" = "AI"),
             Metric = factor(Metric, levels = metric_order)) %>%
      group_by(Metric) %>%
      summarise(Total_MAPE = mean(Total_MAPE, na.rm = TRUE),
                SD_MAPE    = mean(SD_MAPE,    na.rm = TRUE), .groups = "drop")
    
    ggplot(mape_total_avg, aes(x = Metric, y = Total_MAPE, fill = Metric)) +
      geom_col(width = 0.6, color = "gray30") +
      geom_errorbar(aes(ymin = Total_MAPE, ymax = pmin(100, Total_MAPE + SD_MAPE)),
                    width = 0.2, linewidth = 0.7, color = "gray20") +
      scale_y_continuous(breaks = seq(0, 100, by = 10), limits = c(0, 100)) +
      scale_fill_brewer(palette = "Set2") +
      labs(title = "Average Total MAPE by Metric", x = "Metric", y = "Total MAPE (%)") +
      theme_minimal(base_size = 16) +
      theme(plot.title = element_text(size = 18, face = "bold", hjust = 0.5),
            legend.position = "none",
            panel.grid.major.y = element_line(color = "gray90", linewidth = 0.3))
  })
  output$dl_mape_pdf <- downloadHandler(
    filename = function() "MAPE_by_Metric_Total.pdf",
    content = function(file) {
      stats <- totals_df()
      metric_order <- c("AC","AI","ENMO","MAD","MIMS","ROCAM")
      mape_total_avg <- stats %>%
        separate(Comparison, into = c("Metric", "DeviceComparison"), sep = " - ", extra = "merge") %>%
        mutate(Metric = recode(Metric, "Activity Counts" = "AC", "Activity Index" = "AI"),
               Metric = factor(Metric, levels = metric_order)) %>%
        group_by(Metric) %>%
        summarise(Total_MAPE = mean(Total_MAPE, na.rm = TRUE),
                  SD_MAPE    = mean(SD_MAPE,    na.rm = TRUE), .groups = "drop")
      
      p <- ggplot(mape_total_avg, aes(x = Metric, y = Total_MAPE, fill = Metric)) +
        geom_col(width = 0.6, color = "gray30") +
        geom_errorbar(aes(ymin = Total_MAPE, ymax = pmin(100, Total_MAPE + SD_MAPE)),
                      width = 0.2, linewidth = 0.7, color = "gray20") +
        scale_y_continuous(breaks = seq(0, 100, by = 10), limits = c(0, 100)) +
        scale_fill_brewer(palette = "Set2") +
        labs(title = "Average Total MAPE by Metric", x = "Metric", y = "Total MAPE (%)") +
        theme_minimal(base_size = 16) + theme(legend.position = "none")
      ggsave(file, plot = p, width = 8, height = 6, dpi = 300, device = cairo_pdf)
    }
  )
  
  # ---------------- Correlation ----------------
  output$corr_plot <- renderPlot({ corr_four_panel(DF_DAY) })
  output$dl_corr_pdf <- downloadHandler(
    filename = function() "Correlation_Tile_Plots.pdf",
    content = function(file) {
      grDevices::cairo_pdf(file, width = 14, height = 10)
      corr_four_panel(DF_DAY)
      dev.off()
    }
  )
  
  # ---------------- Equivalence (TOST) ----------------
  eq_results <- reactive({ run_tost_all(DF_DAY, all_pairs, eps_pct = input$epsilon) })
  output$eq_table <- renderDT({
    res <- eq_results(); validate(need(!is.null(res) && nrow(res) > 0, "TOST produced no results."))
    datatable(res, options = list(pageLength = 10, scrollX = TRUE))
  })
  output$eq_plot <- renderPlot({
    res <- prep_eq_plot(eq_results(), scale_ac = isTRUE(input$scale_ac))
    eq_plot(res)
  })
  output$dl_eq_pdf <- downloadHandler(
    filename = function() sprintf("equivalence_epsilon_%02d.pdf", input$epsilon),
    content = function(file) {
      res <- prep_eq_plot(eq_results(), scale_ac = isTRUE(input$scale_ac))
      p <- eq_plot(res); ggsave(file, plot = p, width = 15, height = 10, device = cairo_pdf)
    }
  )
  output$dl_eq_docx <- downloadHandler(
    filename = function() sprintf("TOST_Results_epsilon_%02d.docx", input$epsilon),
    content = function(file) {
      res <- eq_results()
      doc <- read_docx() %>%
        body_add_par(sprintf("TOST Analysis — epsilon = %d%% of reference mean", input$epsilon), style = "heading 1") %>%
        body_add_flextable(flextable(res))
      print(doc, target = file)
    }
  )
  
  # ---------------- Data Tab (PUBLIC VIEW) ----------------
  output$day_table <- renderDT({
    datatable(DF_DAY_PUBLIC, options = list(pageLength = 10, scrollX = TRUE))
  })
  output$dl_data_csv <- downloadHandler(
    filename = function() "devicefinaldataday_public.csv",
    content  = function(file) { readr::write_csv(DF_DAY_PUBLIC, file) }
  )
  output$dl_data_rds <- downloadHandler(
    filename = function() "devicefinaldataday_public.rds",
    content  = function(file) { saveRDS(DF_DAY_PUBLIC, file) }
  )
}

# ===============================
# Run
# ===============================
shinyApp(ui, server)
