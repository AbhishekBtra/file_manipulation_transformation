import concurrent.futures
import pandas as pd
import os

# Path to the large CSV file
LARGE_CSV_FILE = "user_data.csv"
TEMP_DIR = "./temp_chunks"  # Directory to store temporary chunk files

def process_chunk(file_path):
    """
    Process a chunk of the CSV file to calculate activities per user.
    """
    # Read the chunk back from the file
    chunk = pd.read_csv(file_path)
    # Group by 'user_id' and count activities
    user_activity_counts = chunk.groupby("user_id")["activity"].count()
    return user_activity_counts

def main():
    # Define the chunk size for reading the large CSV file
    chunk_size = 100000  # Adjust this based on available memory and file size

    # Ensure the temporary directory exists
    os.makedirs(TEMP_DIR, exist_ok=True)

    # Create temporary chunk files
    temp_files = []
    for idx, chunk in enumerate(pd.read_csv(LARGE_CSV_FILE, chunksize=chunk_size)):
        temp_file = os.path.join(TEMP_DIR, f"chunk_{idx}.csv")
        chunk.to_csv(temp_file, index=False)
        temp_files.append(temp_file)

    results = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Submit tasks to process each chunk file
        futures = {executor.submit(process_chunk, temp_file): temp_file for temp_file in temp_files}

        # Collect results from all chunks
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Error processing file {futures[future]}: {e}")

    # Combine results from all chunks
    final_result = pd.concat(results).groupby("user_id").sum()
    print(final_result)

    # Cleanup temporary files
    for temp_file in temp_files:
        os.remove(temp_file)

if __name__ == "__main__":
    main()
