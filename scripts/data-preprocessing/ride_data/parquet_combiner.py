"""
Optimized script to merge multiple Parquet files from a specified folder into a single parquet file using Polars, with schema standardization, streaming, and chunk-wise processing.
"""

from pathlib import Path
import polars as pl

input_folder = Path("data/raw/ride_data_parquet")
output_file = Path("data/processed/ride_data/merged_parquet_file.parquet")

def merge_parquet_in_chunks(folder_path: Path, output_file: Path, chunk_size: int = 5) -> None:
    """
    Efficiently merges Parquet files in chunks to reduce memory usage and prevent system crashes.
    
    Args:
        folder_path (Path): Path to the folder containing Parquet files.
        output_file (Path): Path where the merged file will be saved.
        chunk_size (int): Number of files to process in each chunk.
    """
    parquet_files = list(folder_path.glob("*.parquet"))

    if not parquet_files:
        print(f"No Parquet files found in: {folder_path}")
        return

    print(f"Found {len(parquet_files)} Parquet files. Starting merge in chunks of {chunk_size}...")

    # Ensure temp directory exists
    temp_dir = Path("data/temp")
    temp_dir.mkdir(parents=True, exist_ok=True)

    chunked_frames = []
    for i in range(0, len(parquet_files), chunk_size):
        files_chunk = parquet_files[i:i + chunk_size]
        if not files_chunk:  # Safeguard for empty chunks
            print(f"Skipping empty chunk at index {i // chunk_size + 1}.")
            continue

        print(f"Processing chunk {i // chunk_size + 1}: {len(files_chunk)} files...")

        # Load files with streaming enabled and standardized schema
        lazy_frames = [
            pl.scan_parquet(file).with_columns(
                pl.col("PULocationID").cast(pl.Int64),
                pl.col("DOLocationID").cast(pl.Int64)
            ) for file in files_chunk
        ]

        # Collect chunk using updated engine argument
        collected_chunk = pl.concat(lazy_frames).collect(engine="streaming")
        
        # Save intermediate chunk to disk before final merge
        chunk_file = temp_dir / f"processed_chunk_{i}.parquet"
        collected_chunk.write_parquet(chunk_file, compression="zstd")
        
        chunked_frames.append(collected_chunk)
        print(f"Finished processing chunk {i // chunk_size + 1}. Saved intermediate file: {chunk_file}")

    # Merge all chunks
    print("Merging all intermediate chunk files into a final DataFrame...")
    merged_df = pl.concat(chunked_frames)

    # Save final merged data with compression
    merged_df.write_parquet(output_file, compression="zstd")
    print(f"Merged Parquet file saved successfully at: {output_file}")

merge_parquet_in_chunks(input_folder, output_file, chunk_size=5)
