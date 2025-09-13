# universal-accel

**A unified R pipeline for computing summary metrics from raw accelerometer data across research-grade devices.**

`universal-accel` provides a reproducible framework for loading, calibrating, and summarizing raw accelerometry data.  
It supports **ActiGraph, Axivity, and GENEActiv** formats, with validated implementations of widely used metrics including:  

- **ENMO** (Euclidean Norm Minus One)  
- **MAD** (Mean Amplitude Deviation)  
- **AI** (Activity Index via ActivityIndex)  
- **MIMS-unit** (Monitor-Independent Movement Summary Unit via MIMSunit)  
- **ROCAM** (Rate of Change of Acceleration Magnitude)  
- **AC** (Activity Counts via agcounts)  

---

## Quick Start

```r
# Install from GitHub
install.packages("remotes")
remotes::install_github("JimmyDhr/universalaccel")

# Run the pipeline with a config file
Rscript inst/cli/accel_summaries.R --config config.yml

#Calibration Notes
ActiGraph (.gt3x): processed using agcalibrate.
GENEActiv (.bin): processed using GENEAread recalibration.
Axivity (.resampled.csv): assumed calibrated using OMGui software.

⚙ Features

Device-agnostic outputs: Consistent per-epoch CSVs across supported devices.
Transparent + reproducible: Code-driven pipeline, suitable for research and collaboration.
Non-wear detection: Apply choi algorithm (optional)


## Example Usage ## 

library(universalaccel)

# Run on the included ActiGraph example
accel_summaries(
  device = "actigraph",
  data_folder   = system.file("extdata/actigraph", package = "universalaccel"),
  output_folder = tempdir(),
  epochs        = c(60)
  sample_rate   = 100,       # override if a sampling rate differs
  dynamic_range = c(-8, 8),  # override if the dynamic range is different.
  apply_nonwear =   TRUE
)
# Other configurations
selectable Metrics with argument: metrics = c("MIMS","AI","COUNTS","ENMO","MAD","ROCAM"),
change timezone with  tz = "UTC")
Selectable epochs 

# Example output file
list.files(tempdir(), pattern = "univ_actigraph", full.names = TRUE)
This produces a CSV (e.g., univ_actigraph_epoch60s_YYYY-MM-DD.csv) containing synchronized MIMS, AI, AC, ENMO, MAD, and ROCAM metrics.


## Shiny App

You can launch the example Shiny app included in this package after installation:

shiny::runApp(system.file("shiny/calibrateddata", package = "universalaccel"))

<https://Accel_Summaries_PAHPLab.shinyapps.io/accelerationsummaries/>

📚 References & Citations

Bai, J., Di, C., Xiao, L., Evenson, K. R., LaCroix, A. Z., Crainiceanu, C. M., & Buchner, D. M. (2016). An Activity Index for Raw Accelerometry Data and Its Comparison with Other Activity Metrics. PLOS ONE, 11(8), e0160644. https://doi.org/10.1371/journal.pone.0160644
Helsel, B. C., Hibbing, P. R., Montgomery, R. N., Vidoni, E. D., Ptomey, L. T., Clutton, J., & Washburn, R. A. (2024). agcounts: An R Package to Calculate ActiGraph Activity Counts From Portable Accelerometers. Journal for the Measurement of Physical Behaviour, 7(1). https://doi.org/10.1123/jmpb.2023-0037
John, D., Tang, Q., Albinali, F., & Intille, S. (2019). An Open-Source Monitor-Independent Movement Summary for Accelerometer Data Processing. Journal for the Measurement of Physical Behaviour, 2(4), 268–281. https://doi.org/10.1123/jmpb.2018-0068
Karas, M., Muschelli, J., Leroux, A., Urbanek, J. K., Wanigatunga, A. A., Bai, J., Crainiceanu, C. M., & Schrack, J. A. (2022). Comparison of Accelerometry-Based Measures of Physical Activity: Retrospective Observational Data Analysis Study. JMIR mHealth and uHealth, 10(7), e38077. https://doi.org/10.2196/38077
Neishabouri, A., Nguyen, J., Samuelsson, J., Guthrie, T., Biggs, M., Wyatt, J., Cross, D., Karas, M., Migueles, J. H., Khan, S., & Guo, C. C. (2022). Quantification of Acceleration as Activity Counts in ActiGraph Wearable. Scientific Reports, 12(1), 11958. https://doi.org/10.1038/s41598-022-16003-x
Tsanas, A. (2022). Investigating Wrist-Based Acceleration Summary Measures Across Different Sample Rates Towards 24-Hour Physical Activity and Sleep Profile Assessment. Sensors, 22(16), 6152. https://doi.org/10.3390/s22166152
Wanigatunga, A. A., Wang, H., An, Y., Simonsick, E. M., Tian, Q., Davatzikos, C., Urbanek, J. K., Zipunnikov, V., Spira, A. P., Ferrucci, L., Resnick, S. M., & Schrack, J. A. (2021). Association Between Brain Volumes and Patterns of Physical Activity in Community-Dwelling Older Adults. The Journals of Gerontology: Series A, 76(8), 1504–1511. https://doi.org/10.1093/gerona/glaa294

📜 License

See LICENSE.md for details.

