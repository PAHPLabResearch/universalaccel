#!/usr/bin/env Rscript
suppressPackageStartupMessages({
  library(universalaccel)
  library(yaml)
})

args <- commandArgs(trailingOnly = TRUE)
cfg_path <- if (length(args)) args[1] else "config.yml"
cfg <- yaml::read_yaml(cfg_path)

accel_summaries(
  device        = cfg$device,
  data_folder   = cfg$data_folder,
  output_folder = cfg$output_folder,
  epochs        = cfg$epochs,
  sample_rate   = cfg$sample_rate,
  dynamic_range = cfg$dynamic_range,
  apply_nonwear = isTRUE(cfg$nonwear$enable),
  nonwear_cfg   = list(
    min_bout    = cfg$nonwear$min_bout %||% 90L,
    spike_tol   = cfg$nonwear$spike_tol %||% 2L,
    spike_upper = cfg$nonwear$spike_upper %||% 100L,
    flank       = cfg$nonwear$flank %||% 30L
  )
)
