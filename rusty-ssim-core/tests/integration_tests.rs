use polars::prelude::*;
use polars_testing::assert_dataframe_equal;
use polars_testing::asserts::DataFrameEqualOptions;
use rusty_ssim_core::{ssim_to_csv, ssim_to_dataframe, ssim_to_dataframes, ssim_to_parquets};
use std::fs;
use tempfile::TempDir;

fn ssim_file_generator(flights_count: i16, ivi_count: i8) -> String {
    let mut lines = vec![
        "1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001".to_string(),
    ];
    lines.push("2UXX  0008S18 25MAR1827OCT1813OCT17                                    P                                                                                                                      1301000002".to_string());
    for flight_idx in 0..flights_count {
        let flight_number = format!("{:04}", 1000 + flight_idx);
        for ivi in 0..ivi_count {
            let padded_ivi = format!("{:02}", ivi);
            let leg = "01";
            lines.push(format!(
                "3 XX {flight}{ivi_info}{leg_info}J28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003",
                flight = flight_number,
                ivi_info = padded_ivi,
                leg_info = leg
            ));

            lines.push(format!(
                "4 XX {flight}{ivi_info}{leg_info}J              AB050AMSGRQKL 2562                                                                                                                                                    000006",
                flight = flight_number,
                ivi_info = padded_ivi,
                leg_info = leg
            ));

            lines.push(format!(
                "4 XX {flight}{ivi_info}{leg_info}J              AB127AMSGRQKLM DBA FLYFREE                                                                                                                                            000006",
                flight = flight_number,
                ivi_info = padded_ivi,
                leg_info = leg
            ));
        }
    }

    lines.push(String::from("5 XX                                                                                                                                                                                       000011E000012"));

    lines.join("\n")
}

fn multi_carrier_ssim_file_generator(flights_count: i16, ivi_count: i8) -> String {
    let mut lines = vec![
        "1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001".to_string(),
    ];

    let carriers = ["XX ", "YY ", "ZZ "];

    for carrier in &carriers {
        lines.push(format!("2U{carrier_code}  0008S18 25MAR1827OCT1813OCT17                                    P                                                                                                                      1301000002",
                           carrier_code=carrier
        ));

        for flight_idx in 0..flights_count {
            let flight_number = format!("{:04}", 1000 + flight_idx);
            for ivi in 0..ivi_count {
                let padded_ivi = format!("{:02}", ivi);
                let leg = "01";
                lines.push(format!(
                    "3 {carrier_code} {flight}{ivi_info}{leg_info}J28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003",
                    carrier_code=carrier,
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg
                ));

                lines.push(format!(
                    "4 {carrier_code} {flight}{ivi_info}{leg_info}J              AB050AMSGRQKL 2562                                                                                                                                                    000006",
                    carrier_code=carrier,
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg
                ));

                lines.push(format!(
                    "4 {carrier_code} {flight}{ivi_info}{leg_info}J              AB127AMSGRQKLM DBA FLYFREE                                                                                                                                            000006",
                    carrier_code=carrier,
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg
                ));
            }
        }

        lines.push(format!("5 {carrier_code}                                                                                                                                                                                       000011E000012",
                           carrier_code=carrier));
    }
    lines.join("\n")
}

fn create_temp_multi_ssim_file(flights_count: i16, ivi_count: i8) -> (String, TempDir) {
    let content = multi_carrier_ssim_file_generator(flights_count, ivi_count);
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let file_path = temp_dir.path().join("test.ssim");

    fs::write(&file_path, content).expect("Failed to write SSIM file");

    (file_path.to_string_lossy().to_string(), temp_dir)
}

fn create_temp_ssim_file(flights_count: i16, ivi_count: i8) -> (String, TempDir) {
    let content = ssim_file_generator(flights_count, ivi_count);
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let file_path = temp_dir.path().join("test.ssim");

    fs::write(&file_path, content).expect("Failed to write SSIM file");

    (file_path.to_string_lossy().to_string(), temp_dir)
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_ssim_to_dataframe_success() {
        let (file_path, _temp_dir) = create_temp_ssim_file(10, 2);

        let result = ssim_to_dataframe(&file_path, Some(10000));
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
        let (file_path, _temp_dir) = create_temp_ssim_file(10, 2);

        let result = ssim_to_dataframes(&file_path, Some(1000));
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
        assert_eq!(flights_df.height(), 20);

        assert!(
            !segments_df.is_empty(),
            "For this test, segments should not be empty"
        );
        assert_eq!(segments_df.get_column_names().len(), 16);
        assert_eq!(segments_df.height(), 40);
    }

    #[test]
    fn test_multi_carrier_ssim_to_dataframe_success() {
        let (file_path, _temp_dir) = create_temp_multi_ssim_file(10, 2);

        let result = ssim_to_dataframe(&file_path, Some(10000));
        assert!(
            result.is_ok(),
            "Failed to parse SSIM to DataFrame: {:?}",
            result.err()
        );

        let df = result.unwrap();
        assert!(!df.is_empty(), "DataFrame should not be empty");

        let unique_carriers = df
            .clone()
            .lazy()
            .select([
                col("airline_designator"),
                col("control_duplicate_indicator"),
            ])
            .unique(None, UniqueKeepStrategy::First)
            .collect();

        let options = DataFrameEqualOptions::default().with_check_row_order(false);

        let expected_carriers = df! {
            "airline_designator" => ["XX ", "YY ", "ZZ "],
            "control_duplicate_indicator" => [" ", " ", " "]
        };

        assert_dataframe_equal!(
            &unique_carriers.unwrap(),
            &expected_carriers.unwrap(),
            options
        );
    }

    #[test]
    fn test_ssim_to_csv_success() {
        let (file_path, temp_dir) = create_temp_ssim_file(10, 2);

        let output_path = temp_dir.path().join("output.csv");
        let output_path_str = output_path.to_str().unwrap();

        let result = ssim_to_csv(&file_path, output_path_str, Some(1000));
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
        let (file_path, temp_dir) = create_temp_ssim_file(10, 2);

        let output_path = temp_dir.path().to_str().unwrap();

        let result = ssim_to_parquets(&file_path, Some(output_path), Some("snappy"), Some(1000));
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
        let (file_path, _temp_dir) = create_temp_ssim_file(2, 1);

        let result = ssim_to_dataframe(&file_path, Some(100));
        assert!(
            result.is_ok(),
            "Failed to parse minimal SSIM data: {:?}",
            result.err()
        );

        let df = result.unwrap();
        println!("Minimal data shape: {:?}", df.shape());
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
        let (file_path, _temp_dir) = create_temp_ssim_file(1000, 10);

        // Test with different batch sizes
        for batch_size in [1000, 5000, 10000, 15000] {
            let result = ssim_to_dataframe(&file_path, Some(batch_size));
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
        let (file_path, temp_dir) = create_temp_ssim_file(100, 10);

        let compressions = ["snappy", "gzip", "lz4", "zstd", "uncompressed"];

        for compression in compressions {
            let output_path = temp_dir.path().to_str().unwrap();

            let result =
                ssim_to_parquets(&file_path, Some(output_path), Some(compression), Some(100));
            assert!(
                result.is_ok(),
                "Failed with compression {}: {:?}",
                compression,
                result.err()
            );

            let expected_ext = match compression {
                "gzip" => "gz",
                _ => "parquet",
            };

            // Check that files were created
            let entries = fs::read_dir(temp_dir.path()).expect("Failed to read output directory");
            let parquet_count = entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry
                        .path()
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .map_or(false, |ext| ext == expected_ext)
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
        let (file_path, _temp_dir) = create_temp_ssim_file(5000, 20);

        let start = Instant::now();
        let result = ssim_to_dataframe(&file_path, Some(100000));
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
            duration.as_secs() < 240,
            "Processing should complete within < 240 seconds"
        );
    }
}
