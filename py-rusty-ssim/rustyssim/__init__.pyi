import polars as pl
from typing import Optional

def split_ssim_to_dataframes(
        file_path: str,
        batch_size: int = 10000,
        buffer_size: int = 8192
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Parse SSIM file into Polars DataFrames (types 2, 3, 4).

    Args:
        file_path (str): Path to the SSIM file.
        batch_size (int, optional): Batch size for streaming. Defaults to 10000.
        buffer_size (int, optional): Buffer size in bytes for file reading. Defaults to 8192 (8KB).
            For larger files, consider using 131072 (128KB) for better performance.

    Returns:
        Tuple of DataFrames (record_type2, record_type3, record_type4).

    Example:
        >>> record_type2, record_type3, record_type4 = split_ssim_to_dataframes("path/to/ssim_file.ssim")
        >>> record_type2, record_type3, record_type4 = split_ssim_to_dataframes("path/to/ssim_file.ssim", batch_size=5000)
        >>> record_type2, record_type3, record_type4 = split_ssim_to_dataframes("path/to/ssim_file.ssim", buffer_size=128 * 1024)
    """
    ...

def parse_ssim_to_dataframe(
        file_path: str,
        batch_size: int = 10000,
        buffer_size: int = 8192,
        condense_segments: bool = False
) -> pl.DataFrame:
    """
    Parse SSIM file into a single Polars DataFrame containing Record Type 3 and 4.

    Args:
        file_path (str): Path to the SSIM file.
        batch_size (int, optional): Batch size for streaming. Defaults to 10000.
        buffer_size (int, optional): Buffer size in bytes for file reading. Defaults to 8192 (8KB).
            For larger files, consider using 131072 (128KB) for better performance.
        condense_segments (bool, optional): If True, condense multiple segments for the same flight into a single column/row in list of json format.
            Defaults to False. This can reduce number of rows and improve performance as well file size.

    Returns:
        polars.DataFrame: DataFrame containing Record Type 3 and 4 info.

    Example:
        >>> ssim_dataframe = parse_ssim_to_dataframe("path/to/ssim_file.ssim")
        >>> ssim_dataframe = parse_ssim_to_dataframe("path/to/ssim_file.ssim", batch_size=5000)
        >>> ssim_dataframe = parse_ssim_to_dataframe("path/to/ssim_file.ssim", buffer_size=128 * 1024)
        >>> ssim_dataframe = parse_ssim_to_dataframe("path/to/ssim_file.ssim", condense_segments=True)
    """
    ...

def parse_ssim_to_csv(
        file_path: str,
        output_path: str,
        batch_size: int = 10000,
        buffer_size: int = 8192,
        condense_segments: bool = False
) -> None:
    """
    Parse SSIM file and write directly to CSV file.

    Args:
        file_path (str): Path to the SSIM file.
        output_path (str): Output Path for the CSV file.
        batch_size (int, optional): Batch size for streaming. Defaults to 10000.
        buffer_size (int, optional): Buffer size in bytes for file reading. Defaults to 8192 (8KB).
            For larger files, consider using 131072 (128KB) for better performance.
        condense_segments (bool, optional): If True, condense multiple segments for the same flight into a single column/row in list of json format.
            Defaults to False. This can reduce number of rows and improve performance as well file size.

    Returns:
        None: File is written to disk.

    Example:
        >>> parse_ssim_to_csv("path/to/ssim_file.ssim", "output/path/output_file.csv")
        >>> parse_ssim_to_csv("path/to/ssim_file.ssim", "output/path/output_file.csv", batch_size=5000)
        >>> parse_ssim_to_csv("path/to/ssim_file.ssim", "output/path/output_file.csv", buffer_size=128 * 1024)
        >>> parse_ssim_to_csv("path/to/ssim_file.ssim", "output/path/output_file.csv", condense_segments=True)
    """
    ...

def parse_ssim_to_parquets(
        file_path: str,
        output_path: Optional[str] = ".",
        compression: Optional[str] = "uncompressed",
        batch_size: int = 10000,
        buffer_size: int = 8192,
        condense_segments: bool = False
) -> None:
    """
    Parse SSIM file and write contents to parquet files.

    Args:
        file_path (str): Path to the SSIM file.
        output_path (str): Output Path for the parquet files. Defaults to current directory.
        compression (str, optional): Parquet Compression Options are "snappy", "gzip", "lz4", "zstd", or "uncompressed".
                                   Defaults to "uncompressed".
        batch_size (int, optional): Batch size for streaming. Defaults to 10000.
        buffer_size (int, optional): Buffer size in bytes for file reading. Defaults to 8192 (8KB).
            For larger files, consider using 131072 (128KB) for better performance.
        condense_segments (bool, optional): If True, condense multiple segments for the same flight into a single column/row in list of json format.
            Defaults to False. This can reduce number of rows and improve performance as well file size.

    Returns:
        None: Files are written to disk.

    Example:
        >>> parse_ssim_to_parquets("path/to/ssim_file.ssim")
        >>> parse_ssim_to_parquets("path/to/ssim_file.ssim", "./output_path")
        >>> parse_ssim_to_parquets("path/to/ssim_file.ssim", "./output_path", "zstd", batch_size=5000)
        >>> parse_ssim_to_parquets("path/to/ssim_file.ssim", buffer_size=128 * 1024)
        >>> parse_ssim_to_parquets("path/to/ssim_file.ssim", buffer_size=128 * 1024, condense_segments=True)
    """
    ...