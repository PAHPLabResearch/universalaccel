# universalaccel R workflow

**universalaccel (UA)** is a unified R pipeline for computing harmonized accelerometer raw data preprocessing/summary metrics including: 

1. ENMO (Euclidean Norm Minus One): acceleration magnitude above 1g after removing the static component (autocalibration applied).
2. MAD (Mean Amplitude Deviation): mean deviation of acceleration magnitude within an epoch.
3. AI (Activity Index): variability-based activity metric computed from raw acceleration.
4. MIMS-units (Monitor-Independent Movement Summary unit): standardized activity summary intended to be comparable across devices.
5. ROCAM (Rate of Change Acceleration Movement): change-based activity metric derived from successive acceleration movement samples.
6. AC (ActiGraph counts): traditional “counts”.

These summary metrics can be derived from raw ActiGraph, Axivity, and GENEActiv devices proprietary outputs in UA, but there is also an option to process raw data from other (i.e. generic) devices. 

The UA provides stable, reproducible epoch-level outputs and a downstream analysis workflow for daily intensity distributions interpretation using NHANES (National Health and Nutrition Examination Survey) references.

## 🚀 Installation and quick start
```r
install.packages("remotes")
remotes::install_github("PAHPLabResearch/universalaccel")

# Step 1. Summarize raw accelerometer data

library(universalaccel)
in_dir  <- "C:/Users/..."
out_dir <- "C:/Users/..."
if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

universalaccel::accel_summaries(
  device        = "actigraph", # can take axivity, geneactiv, or generic
  data_folder   = in_dir,
  output_folder = out_dir,
  metrics       = c("MIMS","COUNTS","AI", "ENMO", "MAD", "ROCAM"),
  epochs        = c(60,30,15,5,1),
  sample_rate   = 50, 
  dynamic_range = c(-8, 8),
  apply_nonwear = TRUE,
  autocalibrate = TRUE
)

# Step 2. Calculate intensity gradient and MX metrics (minimum acceleration above which most active minutes were accumulated)

library(universalaccel)
analysis <- ua_run(
  in_path     = "C:/Users/...", # Accept outputs from step 1
  out_dir     = "C:/Users/...",
  location    = "ndw",
  make_weekly = TRUE,
  overwrite   = TRUE
)
```

## 📘 Consult the UA manual for technical details

[![PDF Manual](https://img.shields.io/badge/UA%20Manual-PDF-blue)](https://github.com/PAHPLabResearch/universalaccel/blob/main/data/UA_Manual.pdf)

## Citation

Duhamahoro, J., Hibbing, P. R., Lamoureux, N. R., Berg, E., & Welk, G. J. (2026). Do Movement Summary Metrics Produce Comparable Outputs Across Different Accelerometer Brands in Free Living? Journal for the Measurement of Physical Behaviour, 9(1). https://doi.org/10.1123/jmpb.2025-0024
