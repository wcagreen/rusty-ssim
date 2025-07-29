import polars as pl

def split_ssim_to_dataframes(
    file_path: str,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Parse SSIM file into Polars DataFrames (types 2, 3, 4).

    Args:
        file_path (str):Path to the SSIM file.

    Returns:
        Tuple of DataFrames (record_type2, record_type3, record_type4).

    Example:
        >>> record_type2, record_type3, record_type4 = split_ssim_to_dataframes("path/to/ssim_file.ssim")
    """
    ...

def parse_ssim_to_dataframe(file_path: str) -> pl.DataFrame:
    """
    Parse SSIM file into a single Polars DataFrame containing Record Type 3 and 4.

    Args:
        file_path (str):Path to the SSIM file.

    Returns:
        polars.DataFrame:DataFrame containing Record Type 3 and 4 info.

    Example:
        >>> ssim_dataframe = parse_ssim_to_dataframe("path/to/ssim_file.ssim")
    """
    ...
