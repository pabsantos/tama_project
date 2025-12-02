#import "@preview/diatypst:0.8.0": *

#show: slides.with(
  title: "Vulnerability of urban road networks to floods", // Required
  subtitle: "A case study in São Paulo, Brazil",
  date: "2025-12-05",
  authors: ("Pedro Augusto Borges dos Santos"),
  ratio: 16/9,
  toc: false
)

= Introduction

== Objectives

- Assess the vulnerability of urban road networks based on topology and the occurrence of floods.

- Explore the relationship between topological properties, flood occurrence, and population distribution in the studied area.

== Context

- Not all road sections in a network are equally critical to its functioning, since some have a greater impact on network flows than others.

- Studying and analyzing the vulnerability of road networks helps prioritize planning, budgeting, and maintenance, as well as prepare effective emergency response plans @balijepalliMeasuringVulnerabilityRoad2014.

- Vulnerability: the degree to which a road network is susceptible to performance degradation and user impact when exposed to adverse events.

= Material and methods

== Study area

#grid(
  columns: 2,
  image("plot/study_area_map.png"),
  [
    - Tamanduateí (TTI) river basin inside the city of São Paulo @prefeituradesaopauloPlanoDiretorDrenagem2024

      - Ribeirão do Oratório

      - Montante do Ribeirão do Oratório

      - Córrego Ourives/Ribeirão dos Couros

      - Ribeirão dos Meninos

      - Área de Contribuição Direta de Escoamento Difuso - Meninos/Tamanduateí 

    - Origin/destination (OD) zones that intersected the TTI basin parts: 92 zones out of a total of 527 @metrospPesquisaOrigemDestino2023.
  ]
)

== Flood and network data

#grid(
  columns: 2,
  image("plot/network_flood_map.png"),
  [
    - Road network from 2025-11 @boeingModelingAnalyzingUrban2025

    - Flood points between 2022-01 and 2025-04: 556 flood events @cge-spAlagamentos2025

    - Spatial proximity join to edges
  ]
)

== Graph topology

- Graph representation @barabasiNetworkScience2016

$ G = (V, E); $

Calculation of:

- Node degree @costaCharacterizationComplexNetworks2007

$ k_i = sum_j a_(i j) $

- Edge betweenness centrality (EBC) (vulnerability proxy) @brandesFasterAlgorithmBetweenness2001

$ c_B(e) = sum_(s, t in V) (sigma(s, t | e))/(sigma(s, t)) $


== Population

- Brazilian Demographic Census of 2022 @ibgeCensoDemografico20222022

- Data aggregated by census tracts

- Spatial join between census tracts and nodes

- Population is equally divided among each node $i$ within the same census tract

- For each edge $e$ between nodes $(u, v)$, the population ($P_e(u, v)$) was calculated as the sum of the population of node $u$ ($P_u$) divided by its degree ($k_u$) plus the population of node $v$ ($P_v$) divided by its degree ($k_v$)

$ P_e(u, v) = P_u/k_u + P_v/k_v $ 


= Results

== Road network degree

- 21,045 nodes

- 48,769 edges

#figure(
  image("plot/degree_distribution.png", width: 60%)
)

== Edge betweenness centrality

#figure(
  image("plot/network_ebc_map.png")
)

== Population

#grid(
  columns: 2,
  image("plot/population_map.png"),
  [
    - 7,329 census tracts, with a total of 1,679,145 inhabitants.
  ]
)

== Population

#grid(
  columns: 2,
  image("plot/edges_population_map.png"),
  [
    - 7,329 census tracts, with a total of 1,679,145 inhabitants

    - Approximately 95% of the edges have population values below 100
  ]
)



== Network vulnerability

- Flood value divided by the total count of edges in each range of EBC values (bin size of 0.01)

- Edges in the ranges of 0.06-0.07 and 0.12-0.13 show the highest values of normalized flood count

#figure(
  image("plot/ebc_flood_scatter.png", width: 60%)
)

== Network vulnerability

- Population divided by the total count of edges in each range of EBC values (bin size of 0.01)

- There is roughly an inverted relationship: edges with higher EBC values present lower values of normalized population

#figure(
  image("plot/ebc_population_scatter.png", width: 55%)
)

= Conclusion

== Conclusion

- Edges in the lower ranges of EBC presented higher values of normalized population

- Parts of the network are more vulnerable regarding topology and flood events, impacting overall performance

- Next steps: vulnerability index, new exposure data (traffic flow), weighted graph analysis by population, flow and physical distance

= Bibliography

== Bibliography

#bibliography("refs.bib", style: "apa")