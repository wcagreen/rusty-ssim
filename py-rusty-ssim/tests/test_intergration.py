import pytest
import polars as pl
import tempfile
import os
from pathlib import Path


try:
    import rustyssim
except Exception(ImportError):
    pytest.skip(
        "rustyssim not available - run 'maturin develop' first", allow_module_level=True
    )


def multi_ssim_content_generator(flight_count=1000, ivi_count=10):
    """Create larger SSIM content for performance testing"""
    lines = [
        "1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001"
    ]

    carriers = ["XX", "YY", "ZZ"]

    for carrier in carriers:
        lines.append(
            f"2U{carrier}  0008S18 25MAR1827OCT1813OCT17                                    P                                                                                                                      1301000002"
        )

        for flight_idx in range(flight_count):
            flight_number = f"{1000 + flight_idx:04d}"
            for ivi in range(ivi_count):
                padded_ivi = str(ivi).zfill(2)
                leg = "01"
                lines.append(
                    f"3 XX {flight_number}{padded_ivi}{leg}J28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003"
                )
                lines.append(
                    f"4 XX {flight_number}{padded_ivi}{leg}J              AB050AMSGRQKL 2562                                                                                                                                                    000006"
                )
                lines.append(
                    f"4 XX {flight_number}{padded_ivi}{leg}J              AB127AMSGRQKLM DBA FLYFREE                                                                                                                                            000006"
                )

        lines.append(
            f"5 {carrier}                                                                                                                                                                                       000011E000012"
        )

    return "\n".join(lines)


def ssim_content_generator(flight_count=1000, ivi_count=10):
    """Create larger SSIM content for performance testing"""
    lines = [
        "1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001",
        "2UXX  0008S18 25MAR1827OCT1813OCT17                                    P                                                                                                                      1301000002",
    ]

    for flight_idx in range(flight_count):
        flight_number = f"{1000 + flight_idx:04d}"
        for ivi in range(ivi_count):
            padded_ivi = str(ivi).zfill(2)
            leg = "01"
            lines.append(
                f"3 XX {flight_number}{padded_ivi}{leg}J28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003"
            )
            lines.append(
                f"4 XX {flight_number}{padded_ivi}{leg}J              AB050AMSGRQKL 2562                                                                                                                                                    000006"
            )
            lines.append(
                f"4 XX {flight_number}{padded_ivi}{leg}J              AB127AMSGRQKLM DBA FLYFREE                                                                                                                                            000006"
            )

    lines.append(
        "5 XX                                                                                                                                                                                       000011E000012"
    )

    return "\n".join(lines)


@pytest.fixture
def large_ssim_content():
    """Generate a large SSIM content for performance testing"""
    return ssim_content_generator(flight_count=5000, ivi_count=20)


@pytest.fixture
def typical_ssim_content():
    """Generate typical SSIM content for testing"""
    return ssim_content_generator(flight_count=10, ivi_count=10)


@pytest.fixture
def typical_multi_ssim_content():
    """Generate typical multi-carrier SSIM content for testing"""
    return multi_ssim_content_generator(flight_count=100, ivi_count=10)


@pytest.fixture
def temp_multi_ssim_file(typical_multi_ssim_content):
    """Create a temporary SSIM file"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ssim", delete=False) as f:
        f.write(typical_multi_ssim_content)
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def temp_ssim_file(typical_ssim_content):
    """Create a temporary SSIM file"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ssim", delete=False) as f:
        f.write(typical_ssim_content)
        f.flush()
        yield f.name
    os.unlink(f.name)


@pytest.fixture
def temp_large_ssim_file(large_ssim_content):
    """Create a temporary SSIM file"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ssim", delete=False) as f:
        f.write(large_ssim_content)
        f.flush()
        yield f.name
    os.unlink(f.name)


def test_parse_ssim_to_dataframe(temp_ssim_file):
    """Test parsing SSIM to single DataFrame"""
    df = rustyssim.parse_ssim_to_dataframe(temp_ssim_file)

    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0
    assert "airline_designator" in df.columns
    assert "flight_number" in df.columns


def test_parse_ssim_to_dataframe_with_batch_size(temp_ssim_file):
    """Test parsing with custom batch size"""
    df = rustyssim.parse_ssim_to_dataframe(temp_ssim_file, batch_size=1000)

    assert isinstance(df, pl.DataFrame)
    assert len(df) > 0


def test_split_ssim_to_dataframes(temp_ssim_file):
    """Test splitting SSIM into separate DataFrames"""
    carrier_df, flights_df, segments_df = rustyssim.split_ssim_to_dataframes(
        temp_ssim_file
    )

    assert isinstance(carrier_df, pl.DataFrame)
    assert isinstance(flights_df, pl.DataFrame)
    assert isinstance(segments_df, pl.DataFrame)

    assert len(carrier_df) > 0
    assert len(flights_df) > 0
    assert len(segments_df) > 0


def test_error_handling_invalid_file():
    """Test error handling with invalid file paths"""
    with pytest.raises(Exception):
        rustyssim.parse_ssim_to_dataframe("non_existent_file.ssim")


@pytest.mark.parametrize("batch_size", [1, 10, 100, 1000])
def test_batch_size_consistency(temp_ssim_file, batch_size):
    """Test that different batch sizes produce consistent results"""
    df1 = rustyssim.parse_ssim_to_dataframe(temp_ssim_file, batch_size=batch_size)
    df2 = rustyssim.parse_ssim_to_dataframe(temp_ssim_file, batch_size=10000)

    assert df1.shape == df2.shape


def test_multiple_carriers(temp_multi_ssim_file):
    """Test processing files with multiple carriers"""

    carrier_df, flights_df, segments_df = rustyssim.split_ssim_to_dataframes(
        temp_multi_ssim_file
    )

    unique_carriers = carrier_df["airline_designator"].unique().to_list()
    assert len(unique_carriers) == 3

    assert len(flights_df) == 3000
    assert len(segments_df) == 6000


def test_parse_ssim_to_csv(temp_ssim_file):
    """Test parsing SSIM to CSV file"""
    with tempfile.NamedTemporaryFile(
        prefix="output", suffix=".csv", delete=False
    ) as output_file:
        output_path = output_file.name

    try:
        rustyssim.parse_ssim_to_csv(temp_ssim_file, output_path)

        assert os.path.exists(output_path)
        assert os.path.getsize(output_path) > 0

        df = pl.read_csv(output_path)
        assert len(df) > 0

    finally:
        if os.path.exists(output_path):
            os.unlink(output_path)


def test_parse_ssim_to_parquets(temp_ssim_file):
    """Test parsing SSIM to Parquet files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        rustyssim.parse_ssim_to_parquets(temp_ssim_file, temp_dir, compression="snappy")

        parquet_files = list(Path(temp_dir).glob("*.parquet"))
        assert len(parquet_files) > 0

        for pq_file in parquet_files:
            df = pl.read_parquet(str(pq_file))
            assert len(df) > 0


@pytest.mark.parametrize(
    "compression", ["snappy", "gzip", "lz4", "lzo", "zstd", "brotli", "uncompressed"]
)
def test_different_compressions(temp_ssim_file, compression):
    """Test different compression options for parquet files"""
    with tempfile.TemporaryDirectory() as temp_dir:
        rustyssim.parse_ssim_to_parquets(
            temp_ssim_file, temp_dir, compression=compression
        )

        if compression == "gzip":
            parquet_files = list(Path(temp_dir).glob("*.parquet.gz"))
        else:
            parquet_files = list(Path(temp_dir).glob("*.parquet"))

        assert len(parquet_files) > 0


@pytest.mark.benchmark
def test_performance_scaling(temp_large_ssim_file):
    """Test performance with larger files"""

    import time

    start_time = time.time()
    result = rustyssim.parse_ssim_to_dataframe(temp_large_ssim_file)
    end_time = time.time()

    parse_time = end_time - start_time
    assert parse_time < 240, f"Parsing took too long: {parse_time:.2f}s"
    assert len(result) > 0
