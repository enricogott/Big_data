# Streaming Frequent Item Detection with PySpark

This project implements three algorithms to identify frequent items in a data stream using PySpark Streaming:

- **Exact Counting**
- **Reservoir Sampling**
- **Sticky Sampling**

Each algorithm approximates or exactly identifies items that occur frequently in the stream.

---

##  How It Works

### Algorithms:

1. **Exact Algorithm**
   - Maintains a complete frequency count using a hash table.
   - Identifies all items whose relative frequency ≥ `φ` (phi).

2. **Reservoir Sampling**
   - Maintains a fixed-size sample of the stream.
   - Estimates frequent items based on sampled data.

3. **Sticky Sampling**
   - Maintains a dynamic hash table of items sampled probabilistically.
   - Offers a space-efficient approximate frequency count.

---

##  Usage

### Run with Spark Submit

```bash
spark-submit G055HW3.py <n> <phi> <epsilon> <delta> <port>
```

### Parameters

- `<n>`: Number of stream items to process
- `<phi>`: Minimum frequency threshold (e.g., 0.01)
- `<epsilon>`: Error bound (e.g., 0.001)
- `<delta>`: Probability of failure (e.g., 0.01)
- `<port>`: Port from which the socket stream is read

### Example

```bash
spark-submit G055HW3.py 10000 0.01 0.001 0.01 9999
```

---

##  Input Format

This program listens to a **socket stream** where each line is expected to be an **integer item**.

---

##  Output

The script prints three sections of results:

1. **Exact Algorithm**
   - Number of tracked items
   - True frequent items

2. **Reservoir Sampling**
   - Size of the reservoir
   - Estimated frequent items and whether they are true positives

3. **Sticky Sampling**
   - Hash table size
   - Estimated frequent items and their correctness

---

##  Requirements

- Python 3.x
- Apache Spark with PySpark Streaming
- Required Python packages:
  - `numpy`

Install with:

```bash
pip install numpy
```

---

##  Notes

- Batch interval is **0.01s**, which assumes a high-speed stream.
- Spark Streaming works asynchronously; we use a **threading event** to gracefully stop once `n` items are received.
- Frequent items are defined as those with frequency ≥ `φ`.

---

##  License

This project was developed for educational purposes in streaming data processing using PySpark.
