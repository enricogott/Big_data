# MRFFT & MRApproxOutliers with PySpark

This project implements a **distributed k-center clustering** method using the **Furthest-First Traversal (FFT)** algorithm and an **approximate outlier detection** technique using **MapReduce** in PySpark. It is designed to efficiently handle large-scale 2D point datasets by leveraging parallel computation with Apache Spark.

---

##  Overview

The main goals of this project are:

1. **MRFFT**: Identify `k` cluster centers using a two-round MapReduce version of the FFT algorithm.
2. **MRApproxOutliers**: Approximate the detection of outliers based on local cell densities using the computed cluster radius.

---

##  How It Works

### 1. MRFFT (MapReduce Furthest-First Traversal)

- Round 1: Apply Sequential FFT locally in each partition.
- Round 2: Collect local centers and apply Sequential FFT globally.
- Round 3: Broadcast final centers and compute the **maximum radius** (distance to furthest point).

### 2. MRApproxOutliers

- Divide the 2D space into a grid using a cell size based on radius `D`.
- Count the number of points in each cell and its neighborhood:
  - **R3**: 3×3 grid (local neighborhood)
  - **R7**: 7×7 grid (extended neighborhood)
- Classify:
  - **Sure outliers**: low density in R7
  - **Uncertain points**: low density in R3 but not in R7

---

##  Usage

### Spark Submit Command

```bash
spark-submit G055HW2.py <file_path> <M> <K> <L>
```

### Parameters

- `<file_path>`: Path to the CSV file with 2D points (e.g., `data/points.csv`)
- `<M>`: Threshold for outlier classification (minimum neighborhood size)
- `<K>`: Number of cluster centers for MRFFT
- `<L>`: Number of RDD partitions

### Example

```bash
spark-submit G055HW2.py data/points.csv 20 10 4
```

---

##  Input Format

The input CSV file should have one 2D point per line in the format:

```
x,y
```

Example:
```
1.0,2.5
3.2,4.8
...
```

---

##  Output

The script prints:

- The total number of points.
- Execution time for each round of MRFFT.
- Final cluster radius.
- Number of sure outliers and uncertain points.
- Execution time for the MRApproxOutliers algorithm.

---

##  Requirements

- Python 3.x
- Apache Spark
- Required libraries:
  - `numpy`
  - `math`
  - `os`
  - `sys`
  - `pyspark`

Install packages using:

```bash
pip install numpy pyspark
```

---

##  Notes

- The code is optimized for **large datasets** and **distributed computation**.
- All clustering and outlier operations are performed using **RDDs**, not DataFrames.
- Execution time may vary based on data size and number of partitions.

---

##  License

This project was developed as part of a university coursework on distributed systems and big data processing using PySpark.
