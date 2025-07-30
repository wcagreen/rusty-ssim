import polars as pl
from typing import Optional

def split_ssim_to_dataframes(
    file_path: str, streaming: Optional[bool], batch_size: Optional[int]
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
    file_path: str, streaming: Optional[bool], batch_size: Optional[int]
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
