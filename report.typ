#import "@preview/rubber-article:0.5.0": *

#show: article.with(
    page-paper: "a4",
    lang: "en"   
)


#maketitle(
  title: "Vulnerability of urban road networks to floods - A case study in São Paulo, Brazil",
  authors: ("Pedro Augusto Borges dos Santos",),
  date: "2025-12-05",
)

#set par(first-line-indent: 0em, spacing: 1.0em)
#set math.equation(numbering: "(1)")

= Introduction

Road networks are vulnerable to natural disasters, such as floods, which can cause damage to the infrastructure and adversely affect travel and/or local population on the degraded network. After such events, certain places often become less accessible. Studying and analyzing the vulnerability of road networks helps in prioritizing planning, budgeting, and maintenance, as well as preparing effective emergency response plans. Not all road sections (or street blocks) in a network are equally critical to its functioning, since some have a greater impact on network flows than others, making it essential to assess vulnerability while considering the relative importance of different roads @balijepalliMeasuringVulnerabilityRoad2014.

Although the literature on road network vulnerability has advanced in proposing metrics and methodologies, it is still important to investigate and understand how flooding events are impacting different parts of the network based on its vulnerability. Each city has its own characteristics of road design, intersection density, mobility patterns, and population distribuition, which can directly influence results. Understanding how topological properties and flood occurrence are related is essential to reveal potential particularities that may guide local policies for planning and managing road infrastructure.

In this scenario, the objective of this study is to observe the vulnerability of urban road network based on topology and the occurrence of floods. Using parts of the city of Sao Paulo, Brazil, as a case study, this manuscript will explore the relationship between topological properties, flood occurrence and populational distribuition in the studied area. For this work, vulnerability is defined as the degree to which a road network is susceptible to performance degradation when exposed to adverse events.

Previous studies have also investigated the vulnerability of transport and road networks using graph-based approaches and the analysis of flood occurrence. To #cite(<sharifiResilientUrbanForms2019>, form: "prose"), road design and network topology are the two main factors that influence the resilience of road networks. Focusing on topological properties, #cite(<morelliMeasuringUrbanRoad2021>, form: "prose") focused on vulnerability in urban road systems, highlighting methodological aspects to evaluate structural robustness under different flood scenarios. More recently, #cite(<santosVulnerabilityAnalysisComplex2023>, form: "prose") applied complex network theory to analyze vulnerability to flooding in rural road networks in the state of Santa Catarina, Brazil, offering insights into large-scale network behavior and its implications for regional transport planning.

This manuscript is organized as follows: Section 2 describes the study area, data, and methods used in this research. Section 3 presents the results of the analysis. Section 4 discusses the findings and provides insights into the implications of the research, concludes the manuscript, and suggests future research directions.


= Material and methods

== Study area

The area investigated included five parts of the Tamanduateí (TTI) river basin inside the city of São Paulo, which are the following: _Ribeirão do Oratório_, _Montante do Ribeirão do Oratório_, _Córrego Ourives/Ribeirão dos Couros_, _Ribeirão dos Meninos_, and _Área de Contribuição Direta de Escoamento Difuso - Meninos/Tamanduateí_ @prefeituradesaopauloPlanoDiretorDrenagem2024.

To select the road network, a better area division is the origin/destination (OD) zones from the city of São Paulo. These OD zones were developed to count and to manage traffic in the city @metrospPesquisaOrigemDestino2023. Contrary to the TTI section limits, OD zones are ideal to select the road network, avoiding the cut or separation of street blocks when establishing the scenario. All the OD zones that intersected the previous selected TTI sections were considered in this work. In total, 92 OD zones were considered (from a total of 527). The following map in @fig-area shows the considered TTI sections and OD zones.

#figure(
  image("plot/study_area_map.png", width: 70%),
  caption: "Study scenario"
) <fig-area>

From the selected OD zones it was possible to load the road network graph using the `osmnx` python package @boeingModelingAnalyzingUrban2025. The graph object considered the network from November 2025. The flood occurrence locations were loaded as point objects, from the @cge-spAlagamentos2025, between 2022-01 and 2025-04. @fig-flood presents the loaded road network and flood locations.

#figure(
  image("plot/network_flood_map.png", width: 70%),
  caption: "Road network and flood points"
) <fig-flood>


== Graph analysis

The road network presented in this work is represented by a graph, where each intersection is a node and each road segment is an edge. Graphs are structures used to model the relations between objects @barabasiNetworkScience2016. A graph $G$ can be defined as:

$ G = (V, E); $

where $V$ is a set of vertices (or nodes) and $E$ is a set of edges (or links) that connects pairs of nodes (or connects a node to itself).

To investigate vulnerability, the edge ($e$) betweenness centrality ($c_B(e)$) is calculated by the sum of the fraction of all-pairs shortest paths that pass through that edge, as the following Equation shows:

$ c_B(e) = sum_(s, t in V) (sigma(s, t | e))/(sigma(s, t));  $ <eq-ebc>

where $V$ is the set of nodes, $sigma (s, t)$ is the number of shortest $(s, t)$-paths, and $sigma(s, t | e)$ is the number of those paths passing through edge $e$ @brandesFasterAlgorithmBetweenness2001.

Also, the node degree ($k_i$) was calculated, which is the number of edges connected to that node. This is known as adjacency ($a_(i j)$). When two nodes $i$ and $j$ are said to be neighbors, then $a_(i j) != 0$. The following @eq-ki shows the computation of $k_i$ @costaCharacterizationComplexNetworks2007:

$ k_i = sum_j a_(i j) $ <eq-ki>

Regarding vulnerability, the edge betweenness centrality (EBC) of the road network was compared to the flood count in each edge. The count of flood events in each edge was calcuted using a nearest spatial join approach, where the nearest edge to each flood point was considered.

== Population data

The population data was collected from the Brazilian Demographic Census of 2022 @ibgeCensoDemografico20222022. The available data is aggregated by census tracts, and only the ones inside the study area were selected, by intersection to the OD zones.

After selecting the census tracts, two steps were necessary to migrate population data from tracts to the network edges. First, a spatial join between tracts and nodes was performed. The population was equally divided to each node $i$ inside the same census tract. Then, for each edge $e$ between nodes $(u, v)$, the population ($P_e(u, v)$) was calculared on the sum of the population of node $u$ ($P_u$) divided by its degree ($k_u$) plus the population of node $v$ ($P_v$) divided by its degree ($k_v$)

$ P_e(u, v) = P_u/k_u + P_v/k_v $ <eq-pop>

== Computational tools

The analysis and most of computational methods were performed using the Python programming language, version 3.12.12. The following packages were mainly used: `osmnx` @boeingModelingAnalyzingUrban2025, `networkx` @hagbergExploringNetworkStructure2008, and `geopandas` @jorisvandenbosscheGeopandasGeopandasV1112025. The code was packaged and is available in the following repository: #cite(<santosTamaproject2025>, form: "prose").

= Results

== Road network

The loaded road network graph has 21,045 nodes and 48,769 edges. The map in @fig-ebcmap shows the EBC of the road network. Most of road sections have low EBC values (zero or near zero). Only a few road sections have higher EBC values, indicating that they are more vulnerable to events that impact serviceability.

#figure(
  image("plot/degree_distribution.png", width: 80%),
  caption: "Node degree distribution"
) <fig-power>

#figure(
  image("plot/network_ebc_map.png", width: 80%),
  caption: "Network EBC results"
) <fig-ebcmap>

== Population

#figure(
  image("plot/population_map.png", width: 80%),
  caption: "Population per census tracts"
) <fig-popcensus>

#figure(
  image("plot/edges_population_map.png", width: 80%),
  caption: "Population per network edge"
) <fig-popedge>

== Network vulnerability

@fig-ebc-flood-scatter presents the scatter plot of the EBC and the flood count in each edge. Most of edges have low EBC values (zero or near zero) and zero flood count. The median flood count is zero and the median EBC is near zero.

#figure(
  image("plot/ebc_flood_scatter.png", width: 80%),
  caption: "EBC and normalized flood per edge"
) <fig-ebc-flood-scatter>

#figure(
  image("plot/ebc_population_scatter.png", width: 80%),
  caption: "EBC and normalized population per edge"
) <fig-ebc-pop-scatter>

= Conclusion

// Remember the objectives

// Present the main results

// Whats next? Limitations, perspectives, future work

#bibliography("refs.bib", style: "apa")