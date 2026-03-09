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

## 🚀 Installation
```r
install.packages("remotes")
remotes::install_github("PAHPLabResearch/universalaccel")
```
## 📘 Consult the UA manual for detailed specifications and data processing steps

[![PDF Manual](https://img.shields.io/badge/UA%20Manual-PDF-blue)](https://github.com/PAHPLabResearch/universalaccel/blob/main/data/UA_Manual.pdf)

## Citation

Duhamahoro, J., Hibbing, P. R., Lamoureux, N. R., Berg, E., & Welk, G. J. (2026). Do Movement Summary Metrics Produce Comparable Outputs Across Different Accelerometer Brands in Free Living? Journal for the Measurement of Physical Behaviour, 9(1). https://doi.org/10.1123/jmpb.2025-0024
