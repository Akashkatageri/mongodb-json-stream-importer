# MongoDB JSON Stream Importer

## Overview

The `json_stream_importer.py` script is a high-performance Python utility designed to efficiently stream extremely large JSON files into a MongoDB database.

It leverages the **`ijson`** library to avoid memory exhaustion during parsing, while utilizing MongoDB's optimized **`insert_many`** bulk operations for rapid, high-throughput data ingestion. This script is built for resilience, supporting **automatic resume functionality** and robust error handling crucial for terabyte-scale imports.

## Features ✨

  * **Memory Efficient:** Uses `ijson` to stream data, supporting files far larger than available RAM.
  * **Bulk Insertion:** Employs `pymongo`'s bulk writes for maximum insertion speed.
  * **Resume Capability:** Start the import from a specific document count, crucial for interrupted jobs.
  * **Unordered Writes:** Uses `ordered=False` to maximize throughput and ensures the import continues even if individual documents fail (e.g., due to schema validation or other minor errors).
  * **Dynamic Prefix Detection:** Automatically detects if the JSON is a top-level array (`[...]`) or nested (`{"data": [...]}`).
  * **Connection Resilience:** Configurable, extended timeouts for connection and socket operations.

## Prerequisites

1.  **Python 3.6+**
2.  **A running MongoDB instance** (local or remote).

### Installation

Install the required Python packages using pip:

```bash
pip install ijson pymongo
```

-----

## Configuration

All configurable parameters are located at the top of the `json_stream_importer.py` file. **You must review and update these values** to match your environment and requirements.

### Connection & Target

| Variable | Description | Default Value |
| :--- | :--- | :--- |
| `MONGO_URI` | Your MongoDB connection string. | `"mongodb://localhost:27017/"` |
| `DATABASE_NAME` | The name of the target database. | `"massive_data"` |
| `COLLECTION_NAME` | The name of the collection to insert data into. | `"imported_documents"` |
| `FILE_PATH` | **MANDATORY:** Absolute or relative path to your large JSON file. | `"./data/large_dataset.json"` |

**Note:** The `FILE_PATH` variable must be updated to the correct location of your specific JSON file before running the script.

### Operational Controls

| Variable | Description | Default Value |
| :--- | :--- | :--- |
| `RESUME_COUNT` | Document index to start from. Set to the last successfully **processed** document count to resume. | `0` |
| `MAX_DOCS_LIMIT` | Stops processing after this many documents (`0` to process the entire file). | `0` |

### Scaling & Optimization (Advanced)

| Variable | Description | Default Value |
| :--- | :--- | :--- |
| `MONGO_BATCH_SIZE` | Number of documents to collect before performing a bulk `insert_many`. **Increase this for faster imports.** | `15000` |
| `MONGO_ORDERED` | If `False`, MongoDB skips batch errors and continues (recommended for speed). | `False` |
| `SOCKET_TIMEOUT_MS` | Network socket timeout (in milliseconds). Extended for large writes. | `1800000` (30 min) |

-----

## Usage

1.  **Initial Setup:** Review and update the connection details in the **Configuration** section of `json_stream_importer.py`, paying special attention to **`FILE_PATH`**.

2.  **Run:** Execute the script from your terminal:

    ```bash
    python json_stream_importer.py
    ```

### Resuming a Failed Import

If the script stops unexpectedly, look at the final log output to find the last `Processed` count:

```
Streaming: Processed 10,500,000 | Inserted: 10,480,000 (in this run) | Rate: 2,500.1 docs/sec
```

1.  Set **`RESUME_COUNT`** in `json_stream_importer.py` to the last **Processed** number (e.g., `10500000`).
2.  Restart the script. It will skip the first 10,500,000 documents and resume insertion stability.

-----

## JSON Prefix Detection

The script automatically detects the array's path within your JSON file, printing the result at startup.

| JSON Structure | Detected Prefix |
| :--- | :--- |
| `[{"key": "value"}, ...]` | `item` |
| `{"data": [{"key": "value"}, ...], "meta": {}}` | `data.item` |
