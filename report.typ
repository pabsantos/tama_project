#import "@preview/rubber-article:0.5.0": *

#show: article.with(
    page-paper: "a4",
    lang: "en"
)


#maketitle(
  title: "TAMA Project - TBD",
  authors: ("Pedro Augusto Borges dos Santos",),
  date: "2025-11-14",
)

= Introduction

// What everyone already knows about the topic: Floods, urban street networks, resilience, etc.

// What is still not clear: How flooding is impacting different parts of the network based on its vulnerability; how rainfall and level data are related to flooding and to network centrality metrics.

// Objective: Observe the resilience of urban street network based on topology and the occurrence of floods.

// Similar studies in the literature

// Manuscript structure: Introduction, Material and methods, Results, Conclusion, Bibliography.


= Material and methods

// Study scenario: Where and when - Sao Paulo, Tamanduatei microbasins, 2022, OD zones

  // Show the study area map

// Data: street graph, rain, floods, pcd stations, level data

  // Show the street network map with flood points

// Graphs: definition and centrality metrics: degree, clustering, ebc, power law fit

// Rainfall: Daily values, correlation between pcds, mean daily values

// Water level data: daily values, correlation between pcds, mean z-score

// Analysis: distribution of rainfall and level data between flood and non-flood days, EBC vs flood count scatter, network EBC and flood highlight map, rainfall and level correlation

= Results

// Graph results: degree x clustering, ebc, power law fit

// Rainfall: correlogram, mean daily values, flood and non-flood days distribution

// Water level data: correlogram, mean daily values, flood and non-flood days distribution

// Rainfall x water level

// Flood x non-flood days distribution

// EBC vs flood count scatter

// Network EBC and flood highlight map 

= Conclusion

// Remember the objectives

// Present the main results

// Whats next? Limitations, perspectives, future work

= Bibliography