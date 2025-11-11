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
#set math.equation(numbering: "(1)")

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

== Data and study area

// Study scenario: Where and when - Sao Paulo, Tamanduatei microbasins, 2022, OD zones, pcd stations

The area investigated included parts of the Tamanduateí (TTI) river basin inside the city of São Paulo, which are the following microbasins: _Ribeirão do Oratório_, _Montante do Ribeirão do Oratório_, _Córrego Ourives/Ribeirão dos Couros_, _Ribeirão dos Meninos_, and _Área de Contribuição Direta de Escoamento Difuso - Meninos/Tamanduateí_ (CITATION). Inside this area, 5 stations of data collection (SDC) were considered when collecting rainfall and river level data, with the following IDs: 275, 283, 413 563 and 629 (CITATION). The time period considered in this work ranged from 2022-01 to 2025-04, considering the available data from the respective sources.

To select the street network, a better area division is the origin/destination (OD) zones from the city of São Paulo. These OD zones were developed to count and to manage traffic in the city @metrospPesquisaOrigemDestino2023. Contrary to the TTI microbasins limits, OD zones are ideal to select the street network, avoiding the cut or separation of street blocks when establishing the scenario. All the OD zones that intersected the previous selected TTI microbasins were considered in this work. In total, XX OD zones were considered. The following map in @fig-area shows the considered TTI microbasins, OD zones and SDCs.

  // Show the study area map

#figure(
  image("plot/study_area_map.png", width: 70%),
  caption: "Study scenario"
) <fig-area>

From the selected OD zones it was possible to load the street network graph using the `osmnx` python package (CITATION). The graph object considered the network from November 2025. The flood occurrence locations were also loaded as point objects, from the DIRECT CITATION. @fig-flood presents the loaded street network and flood locations.

// Data: street graph, rain, floods,  level data

  // Show the street network map with flood points

#figure(
  image("plot/network_flood_map.png", width: 70%),
  caption: "Street network and flood points"
) <fig-flood>

The street network presented in this work is represented by a graph, where each intersection is a node and each street segment is an edge. Graphs are structures used to model the relations between objects *(Barabási and Pósfai 2016)*. A graph $G$ can be defined as:

$ G = (V, E); $

where $V$ is a set of vertices (or nodes) and $E$ is a set of edges (or links) that connects pairs of nodes (or connects a node to itself).

== Graph analysis

// Graphs: definition and centrality metrics: degree, clustering, ebc, power law fit

Three graph characterization measures were calculated. Two for nodes - degree ($k_i$) and centrality ($c_i$) - and one for edges, the edge betweenness centrality ($?$). The degree of a node $i$, hence $k_i$, is the number of edges connected to that node. This is known as adjacency ($a_(i j)$). When two nodes $i$ and $j$ are said to be neighbors, then $a_(i j) != 0$. The following @eq-ki shows the computation of $k_i$ *(Costa et al. 2007)*:

$ k_i = sum_j a_(i j) $ <eq-ki>

The clustering coefficient ($c_i$) is calculated by the following Equation:

$ c_i = (2L_i)/(k_i (k_i - 1) );  $ <eq-ci>

where $L_i$ is the number of edges between the neighbors of node $i$ and $k_i$ is the degree of node $i$. $c_i$ can vary between 0 and 1.

The edge ($e$) betweenness centrality ($c_B(e)$) is calculated by the sum of the fraction of all-pairs shortest paths that pass through that edge, as the following Equation shows:

$ c_B(e) = sum_(s, t in V) (sigma(s, t | e))/(sigma(s, t));  $ <eq-ebc>

where $V$ is the set of nodes, $sigma (s, t)$ is the number of shortest $(s, t)$-paths, and $sigma(s, t | e)$ is the number of those paths passing through edge $e$ (CITACAO).

== Rainfall and water level data processing

// Rainfall: Daily values, correlation between pcds, mean daily values (how daily values were calculated)


Both rainfall and water level values were originally loaded in a 10-minute interval. The rainfall daily values were calculated by summing the values of the 10-minute intervals. The water level daily values were calculated by taking the mean of the values of the 10-minute intervals. Regarding rainfall data similaties between SDCs, the Spearman correlation was calculated. Finally, the mean daily rainfall value was calculated to be used in the flood occurrence analysis.

// Water level data: daily values, correlation between pcds, mean z-score (how daily values were calculated)

The river water level data was also compared between SDCs, using the Spearman correlation. To calculate the mean value of water level, first a $z$-score was calculated for each SDC, using the mean and standard deviation of the daily values. Then, the mean $z$-score of the daily values was calculated and considered in the flood occurrence analysis.

== Flood events

// Analysis: distribution of rainfall and level data between flood and non-flood days, EBC vs flood count scatter, network EBC and flood highlight map, rainfall and level correlation

The mean daily rainfall and water level $z$-score mean values were compared between flood and non-flood days, to check if there are visible differences in the distribution of the data between these two groups. Also, a spearman correlation was calculated between the mean daily rainfall and the mean daily water level $z$-score to check if there is a relationship between these two variables.

Regarding vulnerability, the EBC of the street network was compared to the flood count in each edge. Finally, edges with EBC values above the median and with flood count above the median were highlighted in a map, to check which street sections are more vulnerable to flooding. The count of flood occurrences in each edge was calcuted using a nearest spatial join approach, where the nearest edge to each flood point was considered.

== Computational tools

// Computation methods: packages (osmnx, networkx, geopandas), repository, python version

The analysis was performed using the Python programming language, version 3.12.12. The following packages were mainly used: `osmnx` (CITATION), `networkx` (CITATION), and `geopandas` (CITATION). The code was packaged and is available in the following repository: CITATION.

= Results

== Street network

// Graph results: degree x clustering, ebc

The loaded street network graph has 21,045 nodes and 48,769 edges. @fig-power shows the scatter plot of the degree ($k_i$) and clustering coefficient ($c_i$) of the street network. It is possible to observe that higher values of $k_i$ are followed by lower values of $c_i$, implying that the graph is not purely random.

#figure(
  image("plot/network_scatter.png", width: 70%),
  caption: [$k_i$ and $c_i$ network values]
) <fig-power>

The map in @fig-ebcmap shows the EBC of the street network. Most of street sections have low EBC values (zero or near zero). Only a few street sections have higher EBC values, indicating that they are more vulnerable to events that impact serviceability.

#figure(
  image("plot/network_ebc_map.png", width: 80%),
  caption: "Network EBC results"
) <fig-ebcmap>

== Rainfall and river water level

// Rainfall: correlogram, mean daily values

The correlogram in @fig-rain-corr shows the Spearman correlation between the daily rainfall values of the different SDCs. It is possible to observe that the correlation is high (> 0.7).

#figure(
  image("plot/spearman_correlogram.png", width: 70%),
  caption: "Daily rainfall correlation between stations"
) <fig-rain-corr>

@fig-rain-mean shows the timeseries of the mean daily rainfall values. Most of daily values are 0 mm, and the highest values are around 60 mm.

#figure(
  image("plot/mean_rain_time_series.png", width: 80%),
  caption: "Mean daily rainfall values"
) <fig-rain-mean>

// Water level data: correlogram, mean daily values

@fig-level-corr shows the Spearman correlation between the daily water level values of the different SDCs. It is possible to observe that the correlation is medium, ranging from 0.5 to 0.7.

#figure(
  image("plot/spearman_correlogram_level.png", width: 70%),
  caption: [Daily water level $z$-score correlation between stations]
) <fig-level-corr>


@fig-level-mean shows the timeseries of the mean daily water level $z$-score values. Most of daily values 0 or below 0.

#figure(
  image("plot/zscore_mean_time_series.png", width: 80%),
  caption: [Mean daily water level $z$-score values]
) <fig-level-mean>

// Rainfall x water level

The spearman correlation between mean daily rainfall and mean daily water level $z$-score is shown in @fig-zscore-rain-corr, with a result of 0.67.

#figure(
  image("plot/zscore_rain_correlation.png", width: 80%),
  caption: "Spearman correlation between mean z-score and mean daily rainfall"
) <fig-zscore-rain-corr>

// Flood x non-flood days distribution

@fig-rain-flood-violin shows the distribution of the mean daily rainfall values between flood and non-flood days. It is possible to observe that the median value in flood days (near 18 mm) is higher than in non-flood days (0 mm).

#figure(
  image("plot/rain_flood_violin.png", width: 80%),
  caption: "Rainfall x flood x non-flood days distribution"
) <fig-rain-flood-violin>

@fig-flood-violin shows the distribution of the mean daily water level $z$-score values between flood and non-flood days. It is possible to observe that the median value in flood days (near 1) is higher than in non-flood days (near -0.2).

#figure(
  image("plot/zscore_flood_violin.png", width: 80%),
  caption: "Flood x non-flood days distribution"
) <fig-flood-violin>

== Network vulnerability

// EBC vs flood count scatter

@fig-ebc-flood-scatter presents the scatter plot of the EBC and the flood count in each edge. Most of edges have low EBC values (zero or near zero) and zero flood count. The median flood count is zero and the median EBC is near zero.

#figure(
  image("plot/ebc_flood_scatter.png", width: 80%),
  caption: "EBC vs flood count scatter"
) <fig-ebc-flood-scatter>

// Network EBC and flood highlight map

The map in @fig-ebc-flood-highlight-map shows the street network graph highlighting edges with both EBC and flood count above their respective medians, reaching a total of xxxx edges. It is possible to observe that most of the highlighted edges are located in northern part of the map, some closer to the Tiete river, indicating that these areas are more vulnerable to flooding.

#figure(
  image("plot/network_ebc_flood_highlight.png", width: 80%),
  caption: "Network EBC and flood highlight map"
) <fig-ebc-flood-highlight-map>

= Conclusion

// Remember the objectives

// Present the main results

// Whats next? Limitations, perspectives, future work

#bibliography("refs.bib", style: "apa")