# Distributed Outlier Detection with PySpark

This project implements both exact and approximate methods for detecting spatial outliers in a large dataset of 2D points using **PySpark**. It applies **distance-based outlier detection** algorithms with configurable parameters and leverages the **MapReduce** paradigm to enable scalable computation on large datasets.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Usage](#usage)
- [Parameters](#parameters)
- [Methods](#methods)
  - [Exact Algorithm](#exact-algorithm)
  - [MapReduce Approximate Algorithm](#mapreduce-approximate-algorithm)
- [Requirements](#requirements)
- [Performance](#performance)
- [Notes](#notes)

## Overview

Given a dataset of 2D points, the goal is to identify **(D, M)-outliers**:
- A point is a **(D, M)-outlier** if it has fewer than **M** neighboring points within a distance **D**.
- The program supports identifying and printing up to **K** of these outliers or outlier cells.

## Features

- Implements **exact outlier detection** for small datasets.
- Implements **MapReduce-based approximate outlier detection** for large datasets.
- Efficient **distance computation** using `numpy` and fallback strategies for large datasets.
- Supports **RDD partitioning** for distributed processing.
- Logs **execution time** for performance monitoring.

## Usage

To run the program via command line with Spark:

```bash
spark-submit G055HW1.py <file_path> <D> <M> <K> <L>
```

### Example:

```bash
spark-submit G055HW1.py data/points.csv 0.5 20 10 4
```

## Parameters

| Parameter     | Description                                                            |
|---------------|------------------------------------------------------------------------|
| `<file_path>` | Path to the CSV file containing 2D points (x, y) per line.             |
| `<D>`         | Distance threshold (float) for neighborhood consideration.             |
| `<M>`         | Minimum number of neighbors for a point not to be an outlier (integer).|
| `<K>`         | Number of top outliers/cells to print (integer).                       |
| `<L>`         | Number of partitions for Spark RDD (integer).                          |

## Methods

### Exact Algorithm

- For datasets with fewer than 200,000 points.
- Computes pairwise Euclidean distances.
- Identifies outliers with fewer than **M** neighbors within distance **D**.
- Uses optimized `numpy` arrays with adaptive precision (`float64` or `float16`) depending on dataset size.

### MapReduce Approximate Algorithm

- For large datasets.
- Divides 2D space into **cells** based on a grid (cell size determined by **D**).
- Aggregates point counts per cell using RDD transformations:
  - `flatMap`, `mapPartitions`, `groupByKey`, `mapValues`
- Computes **R3** and **R7** neighborhoods:
  - R3: Immediate neighbor cells
  - R7: Extended neighborhood cells
- Classifies cells as **sure outliers**, **uncertain**, or **non-outliers** based on local density.

## Requirements

- Python 3.x
- Apache Spark
- Libraries: `numpy`, `pandas`, `math`, `sys`, `os`

## Performance

- Exact method is **precise but memory-intensive**; suitable for smaller datasets.
- Approximate method is **scalable and efficient** for large datasets but introduces uncertainty.
- Execution times are printed after each method completes.

## Notes

- For very large datasets, the exact method may fail due to memory limits.
- All computations assume input data is properly formatted as `x,y` per line.
- This project was developed for educational purposes in distributed systems and big data processing.
