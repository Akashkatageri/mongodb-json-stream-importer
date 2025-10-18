# ==============================================================================
# json_stream_importer.py
#
# A high-performance Python script designed to stream large JSON files into 
# MongoDB using ijson for memory efficiency and Bulk Write operations for speed.
# This script handles resuming, error handling, and automatically detects the 
# JSON array prefix.
#
# Prerequisites:
#   pip install ijson pymongo
#
# IMPORTANT: This script assumes the JSON file contains a flat array of objects
#            or a root object containing an array (e.g., `{"data": [...]}`).
# ==============================================================================

import ijson
import sys
import gc
from datetime import datetime
import time 
import os
import argparse
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from typing import Optional, Dict, Any, List

# --- CONFIGURATION (Modify these settings) ------------------------------------

# MongoDB Connection
MONGO_URI: str = "mongodb://localhost:27017/"
DATABASE_NAME: str = "massive_data"
COLLECTION_NAME: str = "imported_documents"

# File Path (Absolute or relative path to your large JSON file)
# Best practice is to pass this via command line arguments, but it's set here
# as a default for quick setup.
FILE_PATH: str = "./data/large_dataset.json"

# Operational Parameters
# Set to the last known 'Processed' count to resume the file stream accurately.
RESUME_COUNT: int = 0         # Start from the beginning (0)
# Maximum number of documents to process before stopping (0 = no limit, process all)
MAX_DOCS_LIMIT: int = 0       

# Scale & Optimization Parameters
MONGO_BATCH_SIZE: int = 15000  # Number of documents per bulk insert operation
MONGO_WRITE_CONCERN_W: int = 1 # w=1 is necessary for reliable tracking/consistency
MONGO_ORDERED: bool = False    # False is CRITICAL for maximizing throughput, 
                               # as it allows the server to skip batch errors and continue.
SOCKET_TIMEOUT_MS: int = 1800000 # 30 minutes (1,800,000 ms)
CONNECT_TIMEOUT_MS: int = 1800000 # 30 minutes

# --- JSON Prefix Detection ----------------------------------------------------

def detect_json_prefix(file_path: str) -> str:
    """
    Detects the correct ijson prefix by reading the start of the file.
    Common prefixes are 'item' (for a top-level array `[...]`) or 
    'data.item' (for an array nested under a key `{"data": [...]}`).
    """
    try:
        if not os.path.exists(file_path):
            print(f"Error: File not found at '{file_path}'", file=sys.stderr)
            sys.exit(1)

        with open(file_path, 'rb') as f:
            # Read enough bytes to detect common root keys
            first_bytes = f.read(4096).decode('utf-8', errors='ignore')
            
            # Check for top-level array: `[`
            if first_bytes.strip().startswith('['):
                return 'item'
            
            # Check for common nested array keys
            if '"data"' in first_bytes or '"items"' in first_bytes:
                # If we detect a key, assume the array is nested under it
                print("Warning: Detected root key (e.g., 'data' or 'items'). Using 'data.item' as prefix.")
                return 'data.item'
            
            # Default to 'item' for safety
            return 'item'
            
    except Exception as e:
        print(f"Warning: Failed to detect JSON prefix. Defaulting to 'item'. Error: {e}")
        return 'item'

# --- Main Stream and Import Function ------------------------------------------

def stream_json_to_mongodb():
    """
    Streams the JSON file, batches documents, and inserts them into MongoDB.
    """
    client: Optional[MongoClient] = None
    total_docs_processed: int = RESUME_COUNT
    total_docs_inserted: int = 0  # Only tracks NEW insertions during this run
    current_mongo_batch: List[Dict[str, Any]] = []
    start_time: datetime = datetime.now()

    # Determine the ijson prefix required for streaming
    JSON_PREFIX: str = detect_json_prefix(FILE_PATH)
    
    try:
        # --- Initialization & Logging ---
        print("="*70)
        print("MongoDB JSON Stream Importer")
        print(f"File Path: {FILE_PATH}")
        print(f"Target DB: {DATABASE_NAME}.{COLLECTION_NAME}")
        print(f"JSON Prefix: '{JSON_PREFIX}'")
        print(f"Starting FROM: {RESUME_COUNT:,} documents")
        if MAX_DOCS_LIMIT > 0:
             print(f"LIMITING TO: {MAX_DOCS_LIMIT:,} documents total")
        print(f"Batch Size: {MONGO_BATCH_SIZE:,}")
        print(f"Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)

        # MongoDB connection setup
        client = MongoClient(
            MONGO_URI,
            maxPoolSize=50,
            socketTimeoutMS=SOCKET_TIMEOUT_MS,
            connectTimeoutMS=CONNECT_TIMEOUT_MS,
            w=MONGO_WRITE_CONCERN_W 
        )
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        
        # NOTE: No unique index is created here, fulfilling the user request.
        
        # --- Stream JSON file ---
        with open(FILE_PATH, 'rb') as f:
            objects = ijson.items(f, JSON_PREFIX)
            
            # --- RESUME LOGIC: Skip documents for stable resume ---
            if RESUME_COUNT > 0:
                print(f"Skipping {RESUME_COUNT:,} documents to resume stable stream...")
                
                count = 0
                for _ in objects: 
                    count += 1
                    if count >= RESUME_COUNT: 
                        break
                    
                    if count % 5000000 == 0: 
                        print(f"Skipping... {count:,}/{RESUME_COUNT:,} complete.", end='\r')
                        gc.collect() 

                print(f"\nSkipping complete. Inserting documents starting from position {RESUME_COUNT + 1:,}")
            # --- END RESUME LOGIC ---
            
            for obj in objects:
                total_docs_processed += 1
                
                # Check for termination limit
                if MAX_DOCS_LIMIT > 0 and total_docs_processed > MAX_DOCS_LIMIT:
                    print(f"\nMaximum limit of {MAX_DOCS_LIMIT:,} processed reached. Stopping stream.")
                    break
                
                # Basic validation: Skip non-dictionary objects (i.e., ensure it's a document)
                if not isinstance(obj, dict):
                    print(f"Skipping item at position {total_docs_processed:,}: Not a dictionary.")
                    continue
                
                # Remove potentially conflicting _id field from source JSON
                if "_id" in obj:
                    del obj["_id"] 

                current_mongo_batch.append(obj)

                # --- Progress tracking (Single-line update) ---
                if total_docs_processed % 50000 == 0:
                    elapsed = datetime.now() - start_time
                    # Use total_docs_inserted for rate calculation since it only tracks
                    # documents inserted in the current run (after RESUME_COUNT)
                    rate = total_docs_inserted / max(elapsed.total_seconds(), 1)
                    print(f"Streaming: Processed {total_docs_processed:,} | Inserted: {total_docs_inserted:,} (in this run) | Rate: {rate:.1f} docs/sec", end='\r')

                # --- Batch insertion ---
                if len(current_mongo_batch) >= MONGO_BATCH_SIZE:
                    try:
                        result = collection.insert_many(
                            current_mongo_batch,
                            ordered=MONGO_ORDERED # Allows errors (e.g., from network/timeout) to be skipped
                        )
                        
                        successful_inserts = len(result.inserted_ids)
                        total_docs_inserted += successful_inserts
                        current_mongo_batch = []
                        
                        elapsed = datetime.now() - start_time
                        rate = total_docs_inserted / max(elapsed.total_seconds(), 1)
                        print(f"\nBatch inserted. Total new documents: {total_docs_inserted:,}. Rate: {rate:.1f} docs/sec")
                        
                    except BulkWriteError as bwe:
                        # Since we removed the unique index, this error is less likely 
                        # to be a duplicate error, but is still the correct way to handle
                        # bulk operation failures (e.g., validation/schema errors)
                        error_count = len(bwe.details.get("writeErrors", []))
                        successful_inserts = len(current_mongo_batch) - error_count
                        total_docs_inserted += successful_inserts
                        current_mongo_batch = []
                        print(f"\n⚠️ Batch failed with {error_count} write errors. Inserted {successful_inserts:,} docs successfully.")
                        
                    except Exception as e:
                        # Generic catch for network issues, connection loss, etc.
                        print(f"\nFATAL: Insertion failed, likely network/connection issue: {e}", file=sys.stderr)
                        print("Skipping batch and attempting to continue...")
                        current_mongo_batch = []

                # --- Memory management (Simple GC only) ---
                if total_docs_processed % 1000000 == 0:
                    gc.collect()

            # --- Final batch insertion ---
            if current_mongo_batch:
                print(f"Inserting final batch of {len(current_mongo_batch):,} documents...")
                try:
                    result = collection.insert_many(
                        current_mongo_batch, 
                        ordered=MONGO_ORDERED
                    )
                    total_docs_inserted += len(result.inserted_ids)
                except BulkWriteError as bwe:
                    error_count = len(bwe.details.get("writeErrors", []))
                    successful_inserts = len(current_mongo_batch) - error_count
                    total_docs_inserted += successful_inserts
                    print(f"⚠️ Final batch had {error_count} errors. Inserted {successful_inserts:,} docs.")
                except Exception as e:
                    print(f"\nFATAL: Final batch insertion failed: {e}", file=sys.stderr)


        # --- Final Summary and Verification ---
        end_time = datetime.now()
        duration = end_time - start_time
        
        print("\nWaiting 5 seconds for final database commitment...")
        time.sleep(5)
        
        # Get the total count from the DB for verification
        final_db_count = collection.count_documents({}) 

        avg_rate = total_docs_inserted / max(duration.total_seconds(), 1)
        
        print("\n" + "="*70)
        print("PROCESS COMPLETE")
        print(f"Total documents processed from file: {total_docs_processed:,}")
        print(f"Documents INSERTED in this run: {total_docs_inserted:,}")
        print(f"Total documents IN COLLECTION (DB Count): {final_db_count:,}")
        print(f"Duration: {duration}")
        print(f"Average Insertion Rate (New Data): {avg_rate:.1f} docs/sec")
        print("="*70)

    except Exception as e:
        print(f"\nFATAL ERROR (Uncaught): {e}", file=sys.stderr)
        raise
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    # Note: If you want to use argparse to handle command line inputs for
    # FILE_PATH, MONGO_URI, etc., this is where you would integrate it.
    stream_json_to_mongodb()
