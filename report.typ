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

Road networks are vulnerable to natural disasters, such as floods, which can cause damage to infrastructure and adversely affect travel and/or local populations on degraded networks. After such events, certain places often become less accessible. Studying and analyzing the vulnerability of road networks helps prioritize planning, budgeting, and maintenance, as well as prepare effective emergency response plans. Not all road sections (or street blocks) in a network are equally critical to its functioning, since some have a greater impact on network flows than others, making it essential to assess vulnerability while considering the relative importance of different roads @balijepalliMeasuringVulnerabilityRoad2014.

Although the literature on road network vulnerability has advanced in proposing metrics and methodologies, it is still important to investigate and understand how flooding events impact different parts of the network based on their vulnerability. Each city has its own characteristics of road design, intersection density, mobility patterns, and population distribution, which can directly influence results. Understanding how topological properties and flood occurrence are related is essential to reveal potential particularities that may guide local policies for planning and managing road infrastructure.

In this scenario, the objective of this study is to assess the vulnerability of urban road networks based on topology and the occurrence of floods. Using parts of the city of São Paulo, Brazil, as a case study, this manuscript explores the relationship between topological properties, flood occurrence, and population distribution in the studied area. For this work, vulnerability is defined as the degree to which a road network is susceptible to performance degradation when exposed to adverse events.

Previous studies have also investigated the vulnerability of transport and road networks using graph-based approaches and the analysis of flood occurrence. To #cite(<sharifiResilientUrbanForms2019>, form: "prose"), road design and network topology are the two main factors that influence the resilience of road networks. Focusing on topological properties, #cite(<morelliMeasuringUrbanRoad2021>, form: "prose") focused on vulnerability in urban road systems, highlighting methodological aspects to evaluate structural robustness under different flood scenarios. More recently, #cite(<santosVulnerabilityAnalysisComplex2023>, form: "prose") applied complex network theory to analyze vulnerability to flooding in rural road networks in the state of Santa Catarina, Brazil, offering insights into large-scale network behavior and its implications for regional transport planning.

This manuscript is organized as follows: Section 2 describes the study area, data, and methods used in this research. Section 3 presents the results of the analysis. Section 4 discusses the findings and provides insights into the implications of the research, concludes the manuscript, and suggests future research directions.


= Material and methods

== Study area

The area investigated included five parts of the Tamanduateí (TTI) river basin inside the city of São Paulo, which are the following: _Ribeirão do Oratório_, _Montante do Ribeirão do Oratório_, _Córrego Ourives/Ribeirão dos Couros_, _Ribeirão dos Meninos_, and _Área de Contribuição Direta de Escoamento Difuso - Meninos/Tamanduateí_ @prefeituradesaopauloPlanoDiretorDrenagem2024.

To select the road network, a more appropriate area division is the origin/destination (OD) zones from the city of São Paulo. These OD zones were developed to count and manage traffic in the city @metrospPesquisaOrigemDestino2023. Unlike the TTI section limits, OD zones are ideal for selecting the road network, avoiding the cut or separation of street blocks when establishing the scenario. All OD zones that intersected the previously selected TTI sections were considered in this work. In total, 92 OD zones were considered (from a total of 527). The map in @fig-area shows the considered TTI sections and OD zones.

#figure(
  image("plot/study_area_map.png", width: 70%),
  caption: "Study scenario"
) <fig-area>

From the selected OD zones, it was possible to load the road network graph using the `osmnx` Python package @boeingModelingAnalyzingUrban2025. The graph object considered the network from November 2025. The flood occurrence locations were loaded as point objects from @cge-spAlagamentos2025, between 2022-01 and 2025-04. @fig-flood presents the loaded road network and flood locations.

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

where $V$ is the set of nodes, $sigma(s, t)$ is the number of shortest $(s, t)$-paths, and $sigma(s, t | e)$ is the number of those paths passing through edge $e$ @brandesFasterAlgorithmBetweenness2001.

Also, the node degree ($k_i$) was calculated, which is the number of edges connected to that node. This is known as adjacency ($a_(i j)$). When two nodes $i$ and $j$ are said to be neighbors, then $a_(i j) != 0$. The following @eq-ki shows the computation of $k_i$ @costaCharacterizationComplexNetworks2007:

$ k_i = sum_j a_(i j) $ <eq-ki>

Regarding vulnerability, the edge betweenness centrality (EBC) of the road network was compared to the flood count for each edge. The count of flood events for each edge was calculated using a nearest spatial join approach, where the nearest edge to each flood point was considered.

== Population data

The population data was collected from the Brazilian Demographic Census of 2022 @ibgeCensoDemografico20222022. The available data is aggregated by census tracts, and only the ones inside the study area were selected, by intersection to the OD zones.

After selecting the census tracts, two steps were necessary to migrate population data from tracts to the network edges. First, a spatial join between tracts and nodes was performed. The population was equally divided among each node $i$ within the same census tract. Then, for each edge $e$ between nodes $(u, v)$, the population ($P_e(u, v)$) was calculated as the sum of the population of node $u$ ($P_u$) divided by its degree ($k_u$) plus the population of node $v$ ($P_v$) divided by its degree ($k_v$)

$ P_e(u, v) = P_u/k_u + P_v/k_v $ <eq-pop>

== Computational tools

The analysis and most computational methods were performed using the Python programming language, version 3.12.12. The following packages were mainly used: `osmnx` @boeingModelingAnalyzingUrban2025, `networkx` @hagbergExploringNetworkStructure2008, and `geopandas` @jorisvandenbosscheGeopandasGeopandasV1112025. The code was packaged and is available in the following repository: #cite(<santosTamaproject2025>, form: "prose").

= Results

== Road network

The loaded road network graph has 21,045 nodes and 48,769 edges. @fig-power shows the probability of node degree per degree value. There is a high probability of degree values between 2 and 6. The values vary between 1 and 10. The network does not strictly follow a power-law behavior.


#figure(
  image("plot/degree_distribution.png", width: 80%),
  caption: "Node degree distribution"
) <fig-power>

The map in @fig-ebcmap shows the EBC of the road network. Most road sections have low EBC values (zero or near zero). Only a few road sections have higher EBC values, indicating that they are more vulnerable to events that impact serviceability.

#figure(
  image("plot/network_ebc_map.png", width: 80%),
  caption: "Network EBC results"
) <fig-ebcmap>

== Population

The 7,329 census tracts loaded and selected are presented in @fig-popcensus, showing the population value of each area unit. The map shows some urban voids in the study area and some high-density areas, with population values above 2,000. In total, the area has a population of 2,803,723 inhabitants.

#figure(
  image("plot/population_map.png", width: 80%),
  caption: "Population per census tract"
) <fig-popcensus>

The map in @fig-popedge presents the estimated population for all 48,769 graph edges, with a total population of 2,137,552. Approximately 95% of the edges have population values below 100. 

#figure(
  image("plot/edges_population_map.png", width: 80%),
  caption: "Population per network edge"
) <fig-popedge>

== Network vulnerability

@fig-ebc-flood-scatter shows the normalized flood count, which is the flood value divided by the total count of edges in each range of EBC values (bin size of 0.01), per EBC value. Edges in the ranges of 0.06-0.07 and 0.12-0.13 show the highest values of normalized flood count, which indicates parts of the road network that are considerably more vulnerable compared to other parts of the network.

#figure(
  image("plot/ebc_flood_scatter.png", width: 80%),
  caption: "EBC and normalized flood per edge"
) <fig-ebc-flood-scatter>

Finally, @fig-ebc-pop-scatter shows the relationship between the normalized population (population value per count of edges in each range of EBC) and EBC values. There is roughly an inverted relationship: edges with higher EBC values present lower values of normalized population. 

#figure(
  image("plot/ebc_population_scatter.png", width: 80%),
  caption: "EBC and normalized population per edge"
) <fig-ebc-pop-scatter>

= Conclusion


In this work, it was possible to analyze the vulnerability of urban roads in São Paulo, considering the area of the city within parts of the Tamanduateí river basin, based on data of flood events, population, and network topology.

Mainly, the results showed that the urban road network distribution of degree values did not follow a power-law behavior. It was observed that only a few road sections showed relatively high EBC values, with approximately half of the edges presenting values close to zero.

Some edges with EBC values between 0.06-0.07 and 0.12-0.13 presented high flood count values, normalized by the quantity of edges in each EBC range. This indicates locations where the network is more vulnerable and, therefore, more impacted by the effects of flooding events. Performing the same comparison with the population data for each edge, it was possible to observe that edges in the lower ranges of EBC presented higher values of normalized population. Therefore, roads with higher topological vulnerability are probably impacting less local population.

Every flood event was considered as of equal impact on the network, but the original data has an attribute that indicates the level of severity (passable or unpassable flood event) that was not considered in this work. Also, future work should apply a vulnerability index to better assess the vulnerability of the network. Adding new exposure data, like traffic flow, and applying graph theory methods weighted by population, flow and physical distance could improve the precision of the analysis.

#bibliography("refs.bib", style: "apa")