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

#set par(first-line-indent: 0em, spacing: 1.0em)

= Introduction

// What everyone already knows about the topic: Floods, urban street networks, resilience, etc.

Street networks are vulnerable to natural disasters, such as floods, which can cause immense damage to the infrastructure and adversely affect travel on the degraded network. After such events, certain places often become less accessible. Studying and analyzing the vulnerability of street networks helps in prioritizing planning, budgeting, and maintenance, as well as preparing effective emergency response plans. Not all street sections (or blocks) in a network are equally critical to its functioning, since some have a greater impact on network flows than others, making it essential to assess vulnerability while considering the relative importance of different streets @balijepalliMeasuringVulnerabilityRoad2014.

// What is still not clear: How flooding is impacting different parts of the network based on its vulnerability; how rainfall and level data are related to flooding and to network centrality metrics.

Although the literature on street network vulnerability has advanced in proposing metrics and methodologies, it is still important to advance in understanding how flooding events are impacting different parts of the network based on its vulnerability. Each city has its own characteristics of street design, intersection density, and mobility patterns, which can directly influence results. Understanding how topological properties and flood occurrence are related is essential to reveal potential particularities that may guide local policies for planning and managing street infrastructure.

// Objective: Observe the resilience of urban street network based on topology and the occurrence of floods.

In this scenario, the objective of this study is to observe the vulnerability of urban street network based on topology and the occurrence of floods. Using parts of the city of Sao Paulo, Brazil, as a case study, this manuscript will explore the relationship between rainfall, river water level, and flood occurrence, as well as the relationship between topological properties and flood occurrence in the studied area.

// Similar studies in the literature

Previous studies have also investigated the vulnerability of transport and street networks using graph-based approaches and the analysis of flood occurrence. To #cite(<sharifiResilientUrbanForms2019>, form: "prose"), street design and network topology are the two main factors that influence the resilience of street networks, hence, its capacity to resist, absorb, adapt and transform in the face of environmental impacts. Focusing on topological properties. #cite(<morelliMeasuringUrbanRoad2021>, form: "prose") focused on resilience and vulnerability in urban road systems, highlighting methodological aspects to evaluate structural robustness under different flood scenarios. More recently, #cite(<santosVulnerabilityAnalysisComplex2023>, form: "prose") applied complex network theory to analyze vulnerability to flooding in rural road networks in the state of Santa Catarina, Brazil, offering insights into large-scale network behavior and its implications for regional transport planning.

// Manuscript structure: Introduction, Material and methods, Results, Conclusion, Bibliography.

This manuscript is organized as follows: Section 2 describes the study area, data, and methods used in this research. Section 3 presents the results of the analysis. Section 4 discusses the findings and provides insights into the implications of the research, concludes the manuscript, and suggests future research directions.


= Material and methods

// Study scenario: Where and when - Sao Paulo, Tamanduatei microbasins, 2022, OD zones

  // Show the study area map

#figure(
  image("plot/study_area_map.png", width: 70%),
  caption: "Study scenario"
)

// Data: street graph, rain, floods, pcd stations, level data

  // Show the street network map with flood points

#figure(
  image("plot/network_flood_map.png", width: 70%),
  caption: "Street network and flood points"
)

// Graphs: definition and centrality metrics: degree, clustering, ebc, power law fit

// Rainfall: Daily values, correlation between pcds, mean daily values

// Water level data: daily values, correlation between pcds, mean z-score

// Analysis: distribution of rainfall and level data between flood and non-flood days, EBC vs flood count scatter, network EBC and flood highlight map, rainfall and level correlation

// Computation methods: packages, repository

= Results

// Graph results: degree x clustering, ebc, power law fit

#figure(
  image("plot/network_scatter.png", width: 100%),
  caption: [$k_i$ and $c_i$ network values]
)

#figure(
  image("plot/network_ebc_map.png", width: 100%),
  caption: "Network EBC results"
)

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

#bibliography("refs.bib", style: "apa")