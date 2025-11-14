#import "@preview/diatypst:0.8.0": *

#show: slides.with(
  title: "Vulnerability of urban street networks to floods", // Required
  subtitle: "A case study in São Paulo, Brazil",
  date: "2025-11-14",
  authors: ("Pedro Augusto Borges dos Santos"),
  ratio: 16/9,
  toc: false
)

= Introduction

== Objectives

- Investigate the vulnerability of urban street network based on topology and the occurrence of floods.

- Explore the relationship between rainfall, river water level, and flood occurrence, as well as the relationship between topological properties and flood occurrence in the studied area.

- Identify the most vulnerable street sections. 

== Context

- Resilience of street networks: its capacity to resist, absorb, adapt and transform in the face of environmental impacts @sharifiResilientUrbanForms2019. 

- Studying and analyzing the vulnerability of street networks helps in prioritizing planning, budgeting, and maintenance, as well as preparing effective emergency response plans @balijepalliMeasuringVulnerabilityRoad2014.

= Material and methods

== Study area and data collection stations

#grid(
  columns: 2,
  image("plot/study_area_map.png"),
  [
    - Tamanduateí (TTI) river basin inside the city of São Paulo

    - Origin/destination (OD) zones that intersected the TTI river microbasins

    - Five data collection stations, with rainfall and water level data every 10 minutes, between 2022-01 and 2025-04
  ]
)

== Flood and network data

#grid(
  columns: 2,
  image("plot/network_flood_map.png"),
  [
    - Street network from 2025-11 @boeingModelingAnalyzingUrban2025

    - Flood points between 2022-01 and 2025-04
  ]
)

== Graph topology

Calculation of:

- Node degree

$ k_i = sum_j a_(i j) $

- Node clustering coefficient

$ c_i = (2L_i)/(k_i (k_i - 1) ) $

- Edge betweenness centrality (EBC) (vulnerability proxy)

$ c_B(e) = sum_(s, t in V) (sigma(s, t | e))/(sigma(s, t)) $

== Floods x rainfall x level

- Rainfall processing: 
  - Calculation of daily values (sum)
  - spearman correlation between stations
  - mean daily value between stations, 
  - distribution between flood x non-flood days

- Water level processing: 
  - Calculation of daily values (mean)
  - Spearman correlation between stations
  - Mean $z$-score daily values between stations
  - distribution between flood x non-flood days

== Floods x Vulnerability

- Comparison between EBC values and flood counts per edge (spatial proximity join)

- Highlight of higher values cases - values above the median, for EBC and flood counts values

= Preliminary results

== Rainfall

#grid(
  columns: 2,
  image("plot/spearman_correlogram.png"),
  image("plot/mean_rain_time_series.png")
)

== Water level

#grid(
  columns: 2,
  image("plot/spearman_correlogram_level.png"),
  image("plot/zscore_mean_time_series.png")
)

== Rainfall x water level
#figure(
  image("plot/zscore_rain_correlation.png")
)

== Rainfall x water level x flood

#grid(
  columns: 2,
  image("plot/rain_flood_violin.png"),
  image("plot/zscore_flood_violin.png")
)

== Node degree and clustering

#figure(
  image("plot/network_scatter.png")
)

== Edge betweenness centrality

#figure(
  image("plot/network_ebc_map.png")
)

== EBC x Floods

#figure(
  image("plot/ebc_flood_scatter.png"),
) 

== Vulnerability highlight

#figure(
  image("plot/network_ebc_flood_highlight.png")
)

= Bibliography

== Bibliography

#bibliography("refs.bib", style: "apa")