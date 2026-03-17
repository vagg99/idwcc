# Temporal Triangle Counting on Historical Graphs

This repository contains the codebase used in my diploma thesis on triangle counting in historical graphs.
The project was initially cloned from the public repository `TariqAbughofa/incremental_distributed_wcc`,
which presents an Apache Spark and Scala implementation for incremental distributed community detection
based on Weighted Community Clustering (WCC). In the present work, that codebase is retained for reference and
reproducibility,
but the main research contribution is concentrated in the newly added `data` and `add_ons` components.

The core focus of this repository is no longer the original IDWCC workflow.
Instead, the thesis studies triangle counting under temporal constraints, where each
edge is associated with a validity interval and triangles are examined with respect to a query time window.

## Where the thesis contribution is located

The most important thesis-specific material is concentrated in the following two directories:

- `data/`: input datasets and generated temporal graph files.
- `add_ons/`: the algorithms and utilities developed for the thesis.

If a reader wants to move directly to the thesis-related implementation, these are the first locations to inspect.

## Quick guide

`GraphUtils.scala`: The main utility module and entry point that defines each algorithm's behavior.

The main temporal triangle-counting variants implemented in this repository are:

- `Brute_Force.scala` (`Tr-Intervals`)
- `TriangleCount_GraphFiltering.scala` (`Tr-Filtering`)
- `Thresholding.scala` (`Tr-Thresholding`)

## Original reference

Original repository used as the starting point: `https://github.com/TariqAbughofa/incremental_distributed_wcc`

