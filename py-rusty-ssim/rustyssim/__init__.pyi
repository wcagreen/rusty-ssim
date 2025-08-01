import polars as pl
from typing import Optional

def split_ssim_to_dataframes(
        file_path: str,
        batch_size: int = 10000
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Parse SSIM file into Polars DataFrames (types 2, 3, 4).

    Args:
        file_path (str): Path to the SSIM file.
        batch_size (int, optional): Batch size for streaming. Defaults to 10000.

    Returns:
        Tuple of DataFrames (record_type2, record_type3, record_type4).

    Example:
        >>> record_type2, record_type3, record_type4 = split_ssim_to_dataframes("path/to/ssim_file.ssim")
        >>> record_type2, record_type3, record_type4 = split_ssim_to_dataframes("path/to/ssim_file.ssim", batch_size=5000)
    """
    ...

def parse_ssim_to_dataframe(
        file_path: str,
        batch_size: int = 10000
) -> pl.DataFrame:
    """
    Parse SSIM file into a single Polars DataFrame containing Record Type 3 and 4.

    Args:
        file_path (str): Path to the SSIM file.
        batch_size (int, optional): Batch size for streaming. Defaults to 10000.

    Returns:
        polars.DataFrame: DataFrame containing Record Type 3 and 4 info.

    Example:
        >>> ssim_dataframe = parse_ssim_to_dataframe("path/to/ssim_file.ssim")
        >>> ssim_dataframe = parse_ssim_to_dataframe("path/to/ssim_file.ssim", batch_size=5000)
    """
    ...

def parse_ssim_to_file(
        file_path: str,
        output_path: str,
        file_type: str,
        compression: Optional[str] = None,
        batch_size: int = 10000,
) -> None:
    """
    Parse SSIM file and write directly to file.

    Args:
        file_path (str): Path to the SSIM file.
        output_path (str): Output Path for the ssim file.
        file_type (str): Output file type either csv or parquet.
        compression (str, optional): Parquet Compression Options are "snappy", "gzip", "lz4", "zstd", or "uncompressed".
                                   Defaults to "snappy" for parquet files, ignored for CSV.
        batch_size (int, optional): Batch size for streaming. Defaults to 10000.

    Returns:
        None: File is written to disk.

    Example:
        >>> parse_ssim_to_file("path/to/ssim_file.ssim", "output/path/output_file.parquet", "parquet")
        >>> parse_ssim_to_file("path/to/ssim_file.ssim", "output/path/output_file.csv", "csv")
        >>> parse_ssim_to_file("path/to/ssim_file.ssim", "output/path/output_file.parquet", "parquet", "zstd", 5000)
    """
    ...