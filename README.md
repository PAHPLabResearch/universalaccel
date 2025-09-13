# universalaccel

**Unified R pipeline for computing summary metrics from raw accelerometer data across ActiGraph, Axivity, and GENEActiv devices.**

`universalaccel` supports reproducible workflows with harmonized outputs across devices. It includes validated implementations of:

- ENMO, MAD, AI, MIMS-unit, ROCAM, and Activity Counts
- Device-specific calibration and non-wear detection
- CLI and Shiny app interfaces for flexible deployment

---

## 🚀 Installation

```r
install.packages("remotes")
remotes::install_github("PAHPLabResearch/universalaccel")
```

## 📖 Learn More
• `?accel_summaries()` for function-level help
• Shiny App Demo https://pahplab.shinyapps.io/Raw_Acceleration_Summary_Metrics/


## 🧪 Developer Tools
Run "source("tools/preflight.R")" to validate package structure and sample data.

## 📜 License & Citations
See Licence.md and inst/CITATION for usage and attribution.
