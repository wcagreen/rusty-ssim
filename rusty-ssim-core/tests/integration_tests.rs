use rusty_ssim_core::{ssim_to_csv, ssim_to_dataframe, ssim_to_dataframes, ssim_to_parquets};
use std::fs;
use std::io::Write;
use tempfile::{NamedTempFile, TempDir};

pub const SAMPLE_SSIM_DATA: &str = r#"1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
2UXX  0008S18 25MAR1827OCT1813OCT17                                    P                                                                                                                      1301000002
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
3 XX   120102P28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003
3 XX   130101J02APR1805APR18   4    AMS05200520+0200  GRQ06000600+0200  73HY                                                                                                Y189VV738H189         000004
4 XX   130101J              AB050AMSGRQKL 2562                                                                                                                                                    000006
4 XX   130101J              AB127AMSGRQKLM DBA FLYFREE                                                                                                                                            000006
3AXX  1230501J01APR1820APR18    5   AMS04350435+0200  LJU06200620+0200  73HY                                                             XZ  123                            Y189VV738H189         000005
4AXX  1230501J             1AB010AMSLJUKL 2561                                                                                                                                                    000006
3 XX 12340601J01APR1827APR18   4  7 AMS04350435+0200  LJU06200620+0200  73WY                                                             YY  123                            Y149VV73W             000007
4 XX 12340601J              AB010AMSLJUKL 2561                                                                                                                                                    000008
3XXX 12340301J22SEP1825OCT18     6  AMS11451145+0200  SID18301830-0100  73HY                                                             XY 1234                            Y189VV738H189         000009
3XXX 12340401J01OCT1826OCT18 2      AMS05550555+0200  BVC12451245-0100  73HY                                                                                                Y189VV738H            000010
3 XX 00770101P28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                                                                Y189VV738H189         000003
3 XX 77770101J28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                                                                Y189VV738H189         000003
3 XX 77770102J28MAR1803APR18 2      AMS05100510+0000  ORD08000800+0200  73HY                                                                                                Y189VV738H189         000003
3 XX 77770103J28MAR1803APR18 2      ORD05100510+0000  ATL08000800+0200  73HY                                                                                                Y189VV738H189         000003
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
5 XX                                                                                                                                                                                       000011E000012
"#;

pub const MINIMAL_SSIM_DATA: &str = r#"1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
2UXX  0008S18 25MAR1827OCT1813OCT17                                    P                                                                                                                      1301000002
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
3 XX   120102P28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
5 XX                                                                                                                                                                                       000011E000012
"#;

pub const EMPTY_SSIM_DATA: &str = "";

fn create_temp_ssim_file(content: &str) -> NamedTempFile {
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    temp_file
        .write_all(content.as_bytes())
        .expect("Failed to write to temp file");
    temp_file
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_ssim_to_dataframe_success() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);
        let file_path = temp_file.path().to_str().unwrap();

        let result = ssim_to_dataframe(file_path, Some(1000));
        assert!(
            result.is_ok(),
            "Failed to parse SSIM to DataFrame: {:?}",
            result.err()
        );

        let df = result.unwrap();
        assert!(!df.is_empty(), "DataFrame should not be empty");

        // Check that we have the expected columns count.
        let column_names = df.get_column_names();
        assert_eq!(column_names.len(), 63);

        println!("DataFrame shape: {:?}", df.shape());
        println!("Columns: {:?}", column_names);
    }

    #[test]
    fn test_ssim_to_dataframes_success() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);
        let file_path = temp_file.path().to_str().unwrap();

        let result = ssim_to_dataframes(file_path, Some(1000));
        assert!(
            result.is_ok(),
            "Failed to parse SSIM to DataFrames: {:?}",
            result.err()
        );

        let (carriers_df, flights_df, segments_df) = result.unwrap();

        // Check carriers DataFrame
        assert!(
            !carriers_df.is_empty(),
            "Carriers DataFrame should not be empty"
        );
        assert_eq!(carriers_df.get_column_names().len(), 16);
        assert_eq!(carriers_df.height(), 1);

        // Check flights DataFrame
        assert!(
            !flights_df.is_empty(),
            "Flights DataFrame should not be empty"
        );
        assert_eq!(flights_df.get_column_names().len(), 47);
        assert_eq!(flights_df.height(), 10);

        assert!(
            !segments_df.is_empty(),
            "For this test, segments should not be empty"
        );
        assert_eq!(segments_df.get_column_names().len(), 16);
        assert_eq!(segments_df.height(), 4);
    }

    #[test]
    fn test_ssim_to_csv_success() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);
        let file_path = temp_file.path().to_str().unwrap();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path().join("output.csv");
        let output_path_str = output_path.to_str().unwrap();

        let result = ssim_to_csv(file_path, output_path_str, Some(1000));
        assert!(
            result.is_ok(),
            "Failed to write SSIM to CSV: {:?}",
            result.err()
        );

        assert!(output_path.exists(), "Output CSV file should exist");
        let file_content = fs::read_to_string(output_path).expect("Failed to read output file");
        assert!(!file_content.is_empty(), "CSV file should not be empty");
        assert!(
            file_content.contains("airline_designator"),
            "CSV should contain headers"
        );

        println!("CSV file size: {} bytes", file_content.len());
    }

    #[test]
    fn test_ssim_to_parquets_success() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);
        let file_path = temp_file.path().to_str().unwrap();

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path().to_str().unwrap();

        let result = ssim_to_parquets(file_path, Some(output_path), Some("snappy"), Some(1000));
        assert!(
            result.is_ok(),
            "Failed to write SSIM to Parquet: {:?}",
            result.err()
        );

        // Check that parquet files were created
        let entries = fs::read_dir(temp_dir.path()).expect("Failed to read output directory");
        let parquet_files: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map_or(false, |ext| ext == "parquet")
            })
            .collect();

        assert!(
            !parquet_files.is_empty(),
            "Should have created at least one parquet file"
        );

        for file in &parquet_files {
            let metadata = file.metadata().expect("Failed to get file metadata");
            assert!(metadata.len() > 0, "Parquet file should not be empty");
            println!(
                "Created parquet file: {:?} ({} bytes)",
                file.path(),
                metadata.len()
            );
        }
    }

    #[test]
    fn test_minimal_ssim_data() {
        let temp_file = create_temp_ssim_file(MINIMAL_SSIM_DATA);
        let file_path = temp_file.path().to_str().unwrap();

        let result = ssim_to_dataframe(file_path, Some(100));
        assert!(
            result.is_ok(),
            "Failed to parse minimal SSIM data: {:?}",
            result.err()
        );

        let df = result.unwrap();
        println!("Minimal data shape: {:?}", df.shape());
    }

    #[test]
    fn test_empty_ssim_file() {
        let temp_file = create_temp_ssim_file(EMPTY_SSIM_DATA);
        let file_path = temp_file.path().to_str().unwrap();

        let result = ssim_to_dataframe(file_path, Some(100));

        match result {
            Ok(df) => {
                println!(
                    "Empty file resulted in DataFrame with shape: {:?}",
                    df.shape()
                );
            }
            Err(e) => {
                println!(
                    "Empty file resulted in error (which might be expected): {:?}",
                    e
                );
            }
        }
    }

    #[test]
    fn test_nonexistent_file() {
        let result = ssim_to_dataframe("nonexistent_file.ssim", Some(100));
        assert!(result.is_err(), "Should fail when file doesn't exist");

        if let Err(e) = result {
            println!("Expected error for nonexistent file: {:?}", e);
        }
    }

    #[test]
    fn test_different_batch_sizes() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);
        let file_path = temp_file.path().to_str().unwrap();

        // Test with different batch sizes
        for batch_size in [1, 2, 4, 6] {
            let result = ssim_to_dataframe(file_path, Some(batch_size));
            assert!(
                result.is_ok(),
                "Failed with batch size {}: {:?}",
                batch_size,
                result.err()
            );

            let df = result.unwrap();
            println!(
                "Batch size {}: DataFrame shape {:?}",
                batch_size,
                df.shape()
            );
        }
    }

    #[test]
    fn test_different_compressions() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);
        let file_path = temp_file.path().to_str().unwrap();

        let compressions = ["snappy", "gzip", "lz4", "zstd", "uncompressed"];

        for compression in compressions {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let output_path = temp_dir.path().to_str().unwrap();

            let result =
                ssim_to_parquets(file_path, Some(output_path), Some(compression), Some(100));
            assert!(
                result.is_ok(),
                "Failed with compression {}: {:?}",
                compression,
                result.err()
            );

            // Check that files were created
            let entries = fs::read_dir(temp_dir.path()).expect("Failed to read output directory");
            let parquet_count = entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry
                        .path()
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map_or(false, |ext| ext == "parquet")
                })
                .count();

            assert!(
                parquet_count > 0,
                "Should create parquet files with {} compression",
                compression
            );
            println!(
                "Compression {}: created {} parquet files",
                compression, parquet_count
            );
        }
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_large_file_performance() {
        let large_content = SAMPLE_SSIM_DATA.repeat(100);
        let temp_file = create_temp_ssim_file(&large_content);
        let file_path = temp_file.path().to_str().unwrap();

        let start = Instant::now();
        let result = ssim_to_dataframe(file_path, Some(1000));
        let duration = start.elapsed();

        assert!(
            result.is_ok(),
            "Failed to parse large SSIM file: {:?}",
            result.err()
        );

        let df = result.unwrap();
        println!("Large file processing time: {:?}", duration);
        println!("Large file DataFrame shape: {:?}", df.shape());

        assert!(
            duration.as_secs() < 10,
            "Processing should complete within 10 seconds"
        );
    }

    #[test]
    fn test_streaming_vs_batch_sizes() {
        let large_content = SAMPLE_SSIM_DATA.repeat(50);
        let temp_file = create_temp_ssim_file(&large_content);
        let file_path = temp_file.path().to_str().unwrap();

        let batch_sizes = [100, 500, 1000, 5000];

        for &batch_size in &batch_sizes {
            let start = Instant::now();
            let result = ssim_to_dataframe(file_path, Some(batch_size));
            let duration = start.elapsed();

            assert!(
                result.is_ok(),
                "Failed with batch size {}: {:?}",
                batch_size,
                result.err()
            );

            println!("Batch size {}: {:?}", batch_size, duration);
        }
    }
}
