use polars::prelude::*;
use polars_testing::assert_dataframe_equal;
use polars_testing::asserts::DataFrameEqualOptions;
use rand::Rng;
use rustyssim::{ssim_to_csv, ssim_to_dataframe, ssim_to_dataframes, ssim_to_parquets};
use std::fs;
use tempfile::TempDir;

fn multi_legged_ssim_file_generator(flights_count: i16, ivi_count: i8) -> String {
    let mut rng = rand::rng();
    let mut lines = vec![
        "1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001".to_string(),
    ];
    lines.push("2UXX  0008S18 25MAR1827OCT1813OCT17                                    P                                                                                                                      1301000002".to_string());

    // Sample airport pairs for multi-leg flights
    let leg_configs = [
        // Single leg flights
        vec![("KEF", "0510", "AMS", "0800")],
        vec![("LHR", "0900", "JFK", "1200")],
        vec![("CDG", "1400", "DXB", "2300")],
        // Two leg flights
        vec![
            ("KEF", "0510", "AMS", "0800"),
            ("AMS", "0930", "LHR", "1000"),
        ],
        vec![
            ("JFK", "0800", "ORD", "1000"),
            ("ORD", "1130", "LAX", "1400"),
        ],
        vec![
            ("DXB", "0200", "BKK", "1100"),
            ("BKK", "1230", "SIN", "1530"),
        ],
    ];

    for flight_idx in 0..flights_count {
        let flight_number = format!("{:04}", 1000 + flight_idx);

        // Randomly select a leg configuration
        let selected_config = &leg_configs[rng.random_range(0..leg_configs.len())];

        for ivi in 1..ivi_count {
            let padded_ivi = format!("{:02}", ivi);

            // Generate each leg
            for (leg_idx, leg_data) in selected_config.iter().enumerate() {
                let leg_number = format!("{:02}", leg_idx + 1);
                let (origin, dep_time, dest, arr_time) = leg_data;

                lines.push(format!(
                    "3 XX {flight}{ivi_info}{leg_info}J28MAR1803APR18 2      {origin}{dep}{dep}+0000  {dest}{arr}{arr}+0200  73HY                                                             XY   13                            Y189VV738H189         000003",
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg_number,
                    origin = origin,
                    dep = dep_time,
                    dest = dest,
                    arr = arr_time
                ));

                lines.push(format!(
                    "4 XX {flight}{ivi_info}{leg_info}J              AB050{origin}{dest}KL 2562                                                                                                                                                    000006",
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg_number,
                    dest = dest,
                    origin = origin
                ));

                lines.push(format!(
                    "4 XX {flight}{ivi_info}{leg_info}J              AB127{origin}{dest}KLM DBA FLYFREE                                                                                                                                            000006",
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg_number,
                    dest = dest,
                    origin = origin
                ));
            }
        }
    }

    lines.push(String::from("5 XX                                                                                                                                                                                       000011E000012"));

    lines.join("\n")
}

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

    let carriers = [["XX", " "], ["YY", "X"], ["YY", " "], ["ZZ", " "]];

    for carrier in &carriers {
        let carrier_code = carrier[0];
        let carrier_flag = carrier[1];

        let prefix = format!(
            "2U{}  0008S18 25MAR1827OCT1813OCT17                                    P",
            carrier_code
        );

        // column 108 (1-based) => index 107 (0-based)
        let flag_index = 107usize;
        let pad_to_flag = flag_index.saturating_sub(prefix.len());

        let tail = "1301000002";

        let used_before_tail = prefix.len() + pad_to_flag + 1;
        let pad_after_flag = 200usize.saturating_sub(used_before_tail + tail.len());

        let line = format!(
            "{}{}{}{}{}",
            prefix,
            " ".repeat(pad_to_flag),
            carrier_flag,
            " ".repeat(pad_after_flag),
            tail
        );

        lines.push(line);
        for flight_idx in 0..flights_count {
            let flight_number = format!("{:04}", 1000 + flight_idx);
            for ivi in 0..ivi_count {
                let padded_ivi = format!("{:02}", ivi);
                let leg = "01";
                lines.push(format!(
                    "3 {carrier_code} {flight}{ivi_info}{leg_info}J28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003",
                    carrier_code=carrier[0],
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg
                ));

                lines.push(format!(
                    "4 {carrier_code} {flight}{ivi_info}{leg_info}J              AB050AMSGRQKL 2562                                                                                                                                                    000006",
                    carrier_code=carrier[0],
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg
                ));

                lines.push(format!(
                    "4 {carrier_code} {flight}{ivi_info}{leg_info}J              AB127AMSGRQKLM DBA FLYFREE                                                                                                                                            000006",
                    carrier_code=carrier[0],
                    flight = flight_number,
                    ivi_info = padded_ivi,
                    leg_info = leg
                ));
            }
        }

        lines.push(format!("5 {carrier_code}                                                                                                                                                                                       000011E000012",
                           carrier_code=carrier[0]));
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

fn create_temp_ssim_file(flights_count: i16, ivi_count: i8, multi_leg: bool) -> (String, TempDir) {
    if multi_leg {
        let content = multi_legged_ssim_file_generator(flights_count, ivi_count);

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let file_path = temp_dir.path().join("test.ssim");
        fs::write(&file_path, content).expect("Failed to write SSIM file");
        return (file_path.to_string_lossy().to_string(), temp_dir);
    } else {
        let content = ssim_file_generator(flights_count, ivi_count);
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let file_path = temp_dir.path().join("test.ssim");

        fs::write(&file_path, content).expect("Failed to write SSIM file");

        return (file_path.to_string_lossy().to_string(), temp_dir);
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_ssim_to_dataframe_success() {
        let (file_path, _temp_dir) = create_temp_ssim_file(10, 2, false);

        let result = ssim_to_dataframe(&file_path, Some(10000), Some(8192), Some(false));
        assert!(
            result.is_ok(),
            "Failed to parse SSIM to DataFrame: {:?}",
            result.err()
        );

        let df = result.unwrap();
        assert!(df.height() > 0, "DataFrame should not be empty");

        // Check that we have the expected columns count.
        let column_names = df.get_column_names();
        assert_eq!(column_names.len(), 63);

        println!("DataFrame shape: {:?}", df.shape());
        println!("Columns: {:?}", column_names);
    }

    #[test]
    fn test_ssim_to_dataframes_success() {
        let (file_path, _temp_dir) = create_temp_ssim_file(10, 2, false);

        let result = ssim_to_dataframes(&file_path, Some(1000), Some(8192));
        assert!(
            result.is_ok(),
            "Failed to parse SSIM to DataFrames: {:?}",
            result.err()
        );

        let (carriers_df, flights_df, segments_df) = result.unwrap();

        // Check carriers DataFrame
        assert!(
            carriers_df.height() > 0,
            "Carriers DataFrame should not be empty"
        );
        assert_eq!(carriers_df.get_column_names().len(), 16);
        assert_eq!(carriers_df.height(), 1);

        // Check flights DataFrame
        assert!(
            flights_df.height() > 0,
            "Flights DataFrame should not be empty"
        );
        assert_eq!(flights_df.get_column_names().len(), 47);
        assert_eq!(flights_df.height(), 20);

        assert!(
            segments_df.height() > 0,
            "For this test, segments should not be empty"
        );
        assert_eq!(segments_df.get_column_names().len(), 16);
        assert_eq!(segments_df.height(), 40);
    }

    #[test]
    fn test_multi_carrier_ssim_to_dataframe_success() {
        let (file_path, _temp_dir) = create_temp_multi_ssim_file(10, 2);

        let result = ssim_to_dataframe(&file_path, Some(10000), Some(8192), Some(false));
        assert!(
            result.is_ok(),
            "Failed to parse SSIM to DataFrame: {:?}",
            result.err()
        );

        let df = result.unwrap();
        assert!(df.height() > 0, "DataFrame should not be empty");

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
            "airline_designator" => ["XX ", "YY ", "YY ", "ZZ "],
            "control_duplicate_indicator" => [" ", "X", " ", " "]
        };

        assert_dataframe_equal!(
            &unique_carriers.unwrap(),
            &expected_carriers.unwrap(),
            options
        );
    }

    #[test]
    fn test_ssim_to_csv_success() {
        let (file_path, temp_dir) = create_temp_ssim_file(10, 2, false);

        let output_path = temp_dir.path().join("output.csv");
        let output_path_str = output_path.to_str().unwrap();

        let result = ssim_to_csv(
            &file_path,
            output_path_str,
            Some(1000),
            Some(8192),
            Some(false),
        );
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
        let (file_path, temp_dir) = create_temp_ssim_file(10, 2, false);

        let output_path = temp_dir.path().to_str().unwrap();

        let result = ssim_to_parquets(
            &file_path,
            Some(output_path),
            Some("snappy"),
            Some(1000),
            Some(8192),
            Some(false),
        );
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
        let (file_path, _temp_dir) = create_temp_ssim_file(2, 1, false);

        let result = ssim_to_dataframe(&file_path, Some(100), Some(8192), Some(false));
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
        let result = ssim_to_dataframe("nonexistent_file.ssim", Some(100), Some(8192), Some(false));
        assert!(result.is_err(), "Should fail when file doesn't exist");

        if let Err(e) = result {
            println!("Expected error for nonexistent file: {:?}", e);
        }
    }

    #[test]
    fn test_different_batch_sizes() {
        let (file_path, _temp_dir) = create_temp_ssim_file(1000, 10, false);

        // Test with different batch sizes
        for batch_size in [1000, 5000, 10000, 15000] {
            let result = ssim_to_dataframe(&file_path, Some(batch_size), Some(8192), Some(false));
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
        let (file_path, temp_dir) = create_temp_ssim_file(100, 10, false);

        let compressions = ["snappy", "gzip", "lz4", "zstd", "uncompressed"];

        for compression in compressions {
            let output_path = temp_dir.path().to_str().unwrap();

            let result = ssim_to_parquets(
                &file_path,
                Some(output_path),
                Some(compression),
                Some(100),
                Some(8192),
                Some(false),
            );
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

    #[test]
    fn test_condense_segments() {
        let (file_path, _temp_dir) = create_temp_ssim_file(10, 5, false);

        let result = ssim_to_dataframe(&file_path, Some(10000), Some(8192), Some(true));
        assert!(
            result.is_ok(),
            "Failed to parse SSIM with condensed segments: {:?}",
            result.err()
        );

        let df = result.unwrap();
        println!("Condensed segments DataFrame shape: {:?}", df.shape());

        // Check that the segment_data column exists
        assert_eq!(df.get_column_names().len(), 58);
    }

    #[test]
    fn test_multi_leg_segments_join() {
        let (file_path, _temp_dir) = create_temp_ssim_file(10, 2, true);

        let result = ssim_to_dataframe(&file_path, Some(10000), Some(8192), Some(false));
        let df = result.unwrap();

        let result_agg = df
            .clone()
            .lazy()
            .group_by([
                col("flight_designator"),
                col("control_duplicate_indicator"),
                col("leg_sequence_number"),
            ])
            .agg([col("leg_sequence_number").count().alias("leg_count")])
            .filter(col("leg_count").gt(lit(2)))
            .collect()
            .unwrap();

        assert_eq!(
            result_agg.height(),
            0,
            "There should be no legs with more than 2 segments."
        );
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_large_file_performance() {
        let (file_path, _temp_dir) = create_temp_ssim_file(5000, 20, false);

        let start = Instant::now();
        let result = ssim_to_dataframe(&file_path, Some(100000), Some(8192), Some(false));
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
