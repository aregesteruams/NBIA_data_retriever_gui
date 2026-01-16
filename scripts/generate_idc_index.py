#!/usr/bin/env python3
"""Generate IDC Index Subset - Extract specific columns as compressed Parquet"""

import logging
import sys
from pathlib import Path

import pandas as pd
from idc_index import IDCClient

try:
    import importlib.metadata
    get_version = lambda pkg: importlib.metadata.version(pkg)
except ImportError:
    # Fallback for Python < 3.8
    import pkg_resources
    get_version = lambda pkg: pkg_resources.get_distribution(pkg).version

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Target columns and output config
COLUMNS = [
    'collection_id', 'PatientID', 'StudyInstanceUID',
    'SeriesInstanceUID', 'series_size_MB', 'series_aws_url'
]
COMPRESSION = 'zstd'

def main():
    try:
        # Get idc-index-data version for provenance (the actual data package)
        data_version = get_version('idc-index-data')
        output_file = f'idc-index-subset-v{data_version}.parquet'
        logger.info(f"Using idc-index-data version: {data_version}")

        logger.info("Initializing IDC client")
        client = IDCClient.client()

        logger.info("Accessing IDC index dataframe")
        df = client.index
        logger.info(f"Index shape: {df.shape}")

        # Validate columns exist
        missing_cols = set(COLUMNS) - set(df.columns)
        if missing_cols:
            raise KeyError(f"Missing columns: {missing_cols}")

        # Extract subset
        logger.info(f"Extracting {len(COLUMNS)} columns")
        df_subset = df[COLUMNS].copy()
        logger.info(f"Subset shape: {df_subset.shape}")

        # Check for nulls (warning only)
        null_counts = df_subset.isnull().sum()
        if null_counts.any():
            logger.warning(f"Null values:\n{null_counts[null_counts > 0]}")

        # Export to Parquet
        logger.info(f"Exporting to {output_file}")
        df_subset.to_parquet(
            output_file,
            engine='pyarrow',
            compression=COMPRESSION,
            index=False
        )

        # Validate output
        output_path = Path(output_file)
        if not output_path.exists():
            raise FileNotFoundError(f"Output not created: {output_file}")

        file_size_mb = output_path.stat().st_size / 1024**2
        logger.info(f"File size: {file_size_mb:.2f} MB")

        # Quick validation read
        df_test = pd.read_parquet(output_file)
        logger.info(f"Validated: {len(df_test)} rows, {len(df_test.columns)} cols")
        logger.info("✓ Completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
