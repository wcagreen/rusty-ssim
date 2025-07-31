import polars as pl
from typing import Optional

def split_ssim_to_dataframes(
    file_path: str, streaming: Optional[bool], batch_size: Optional[int] = 10000
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Parse SSIM file into Polars DataFrames (types 2, 3, 4).

    Args:
        file_path (str)~ Path to the SSIM file.
        streaming (bool)~ Streaming flag in order to handle large files.
        batch_size (int)~ Batch size for streaming.

    Returns:
        Tuple of DataFrames (record_type2, record_type3, record_type4).

    Example:
        >>> record_type2, record_type3, record_type4 = split_ssim_to_dataframes("path/to/ssim_file.ssim")
    """
    ...

def parse_ssim_to_dataframe(
    file_path: str, streaming: Optional[bool], batch_size: Optional[int] = 10000
) -> pl.DataFrame:
    """
    Parse SSIM file into a single Polars DataFrame containing Record Type 3 and 4.

    Args:
        file_path (str)~ Path to the SSIM file.
        streaming (bool)~ Streaming flag in order to handle large files.
        batch_size (int)~ Batch size for streaming.

    Returns:
        polars.DataFrame:DataFrame containing Record Type 3 and 4 info.

    Example:
        >>> ssim_dataframe = parse_ssim_to_dataframe("path/to/ssim_file.ssim")
    """
    ...

def parse_ssim_to_file(
        file_path: str,
        output_path: str,
        file_type: str,
        compression: Optional[str] = "snappy",
        batch_size: Optional[int] = 10000,
) -> pl.DataFrame:
    """
    Parse SSIM file into a single Polars DataFrame containing Record Type 3 and 4.

    Args:
        file_path (str)~ Path to the SSIM file.
        output_path (str)~ Output Path for the ssim file.
        file_type (str)~ Output file type either csv or parquet.
        compression (str)~ Parquet Compression Options are "snappy", "gzip", "lz4", "zstd", or "uncompressed"
        batch_size (int)~ Batch size for streaming. The default batch_size is 10,000

    Returns:
        Either CSV or Parquet dataframe.

    Example:
        >>> parse_ssim_to_file("path/to/ssim_file.ssim", "output/path/output_file.parquet", "parquet", "snappy", batch_size=10000)

    """
    ...

