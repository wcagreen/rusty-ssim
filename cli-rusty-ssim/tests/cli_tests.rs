use std::io::Write;
use std::process::Command;
use tempfile::{NamedTempFile, TempDir};

pub const CLI_APP: &str = "cli-rusty-ssim";

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

pub const SAMPLE_MULTI_SSIM_DATA: &str = r#"1AIRLINE STANDARD SCHEDULE DATA SET                                                                                                                                                            001000001
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
2UXX  0008S18 25MAR1827OCT1813OCT17                                    P                                   X                                                                                  1301000002
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
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
2LYY  0008S18 25MAR1827OCT1813OCT18                                    P                                   X                                                                                  1301000002
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
3 YY   120102P28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                             XY   13                            Y189VV738H189         000003
3 YY   130101J02APR1805APR18   4    AMS05200520+0200  GRQ06000600+0200  73HY                                                                                                Y189VV738H189         000004
4 YY   130101J              AB050AMSGRQKL 2562                                                                                                                                                    000006
4 YY   130101J              AB127AMSGRQKLM DBA FLYFREE                                                                                                                                            000006
3AYY  1230501J01APR1820APR18    5   AMS04350435+0200  LJU06200620+0200  73HY                                                             XZ  123                            Y189VV738H189         000005
4AYY  1230501J             1AB010AMSLJUKL 2561                                                                                                                                                    000006
3 YY 12340601J01APR1827APR18   4  7 AMS04350435+0200  LJU06200620+0200  73WY                                                             YY  123                            Y149VV73W             000007
4 YY 12340601J              AB010AMSLJUKL 2561                                                                                                                                                    000008
3XYY 12340301J22SEP1825OCT18     6  AMS11451145+0200  SID18301830-0100  73HY                                                             XY 1234                            Y189VV738H189         000009
3XYY 12340401J01OCT1826OCT18 2      AMS05550555+0200  BVC12451245-0100  73HY                                                                                                Y189VV738H            000010
3 YY 00770101P28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                                                                Y189VV738H189         000003
3 YY 77770101J28MAR1803APR18 2      KEF05100510+0000  AMS08000800+0200  73HY                                                                                                Y189VV738H189         000003
3 YY 77770102J28MAR1803APR18 2      AMS05100510+0000  ORD08000800+0200  73HY                                                                                                Y189VV738H189         000003
3 YY 77770103J28MAR1803APR18 2      ORD05100510+0000  ATL08000800+0200  73HY                                                                                                Y189VV738H189         000003
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
5 YY                                                                                                                                                                                       000011E000012
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
2UXX  0008S18 25MAR1827OCT1813OCT19                                    P                                                                                                                      1301000002
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

fn create_temp_ssim_file(content: &str) -> NamedTempFile {
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    temp_file
        .write_all(content.as_bytes())
        .expect("Failed to write to temp file");
    temp_file
}


#[cfg(test)]
mod cli_tests {
    use std::fs;
    use super::*;


    #[test]
    fn test_cli_csv_command() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path().join("output.csv");

        let output = Command::new(CLI_APP)
            .args(&[
                "csv",
                "-s", temp_file.path().to_str().unwrap(),
                "-o", output_path.to_str().unwrap(),
                "-b", "100"
            ])
            .output()
            .expect("Failed to execute CLI command");

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!("CLI command failed:\nSTDOUT: {}\nSTDERR: {}", stdout, stderr);
        }

        // Check that the output file was created
        assert!(output_path.exists(), "Output CSV file should exist");

        let file_content = fs::read_to_string(&output_path).expect("Failed to read output file");
        assert!(!file_content.is_empty(), "CSV file should not be empty");

        println!("CLI CSV test passed. Output file size: {} bytes", file_content.len());

    }

    #[test]
    fn test_cli_small_ssim_csv_command(){
        let temp_file = create_temp_ssim_file(MINIMAL_SSIM_DATA);
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path().join("output.csv");

        let output = Command::new(CLI_APP)
            .args(&[
                "csv",
                "-s", temp_file.path().to_str().unwrap(),
                "-o", output_path.to_str().unwrap(),
                "-b", "100"
            ])
            .output()
            .expect("Failed to execute CLI command");

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!("CLI command failed:\nSTDOUT: {}\nSTDERR: {}", stdout, stderr);
        }

        // Check that the output file was created
        assert!(output_path.exists(), "Output CSV file should exist");

        let file_content = fs::read_to_string(&output_path).expect("Failed to read output file");
        assert!(!file_content.is_empty(), "CSV file should not be empty");

        println!("CLI CSV test passed. Output file size: {} bytes", file_content.len());
    }


    #[test]
    fn test_cli_single_carrier_parquet_command() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path();

        let output = Command::new(CLI_APP)
            .args(&[
                "parquet",
                "-s", temp_file.path().to_str().unwrap(),
                "-o", output_path.to_str().unwrap(),
                "-c", "snappy",
                "-b", "100"
            ])
            .output()
            .expect("Failed to execute CLI command");

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!("CLI command failed:\nSTDOUT: {}\nSTDERR: {}", stdout, stderr);
        }

        // Check that parquet files were created
        let entries = fs::read_dir(temp_dir.path()).expect("Failed to read output directory");
        let parquet_files: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension()
                    .and_then(|ext| ext.to_str())
                    .map_or(false, |ext| ext == "parquet")
            })
            .collect();

        assert!(!parquet_files.is_empty(), "Should have created at least one parquet file");

        for file in &parquet_files {
            let metadata = file.metadata().expect("Failed to get file metadata");
            assert!(metadata.len() > 0, "Parquet file should not be empty");
            println!("Created parquet file: {:?} ({} bytes)", file.path(), metadata.len());
        }

        println!("CLI Parquet test passed. Created {} files", parquet_files.len());
    }

    #[test]
    fn test_cli_multi_carrier_parquet_command() {
        let temp_file = create_temp_ssim_file(SAMPLE_MULTI_SSIM_DATA);
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_path = temp_dir.path();

        let output = Command::new(CLI_APP)
            .args(&[
                "parquet",
                "-s", temp_file.path().to_str().unwrap(),
                "-o", output_path.to_str().unwrap(),
                "-c", "snappy",
                "-b", "100"
            ])
            .output()
            .expect("Failed to execute CLI command");

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            panic!("CLI command failed:\nSTDOUT: {}\nSTDERR: {}", stdout, stderr);
        }

        // Check that parquet files were created
        let entries = fs::read_dir(temp_dir.path()).expect("Failed to read output directory");
        let parquet_files: Vec<_> = entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry.path().extension()
                    .and_then(|ext| ext.to_str())
                    .map_or(false, |ext| ext == "parquet")
            })
            .collect();

        assert!(!parquet_files.is_empty(), "Should have created at least one parquet file");

        for file in &parquet_files {
            let metadata = file.metadata().expect("Failed to get file metadata");
            assert!(metadata.len() > 0, "Parquet file should not be empty");
            println!("Created parquet file: {:?} ({} bytes)", file.path(), metadata.len());
        }

        println!("CLI Parquet test passed. Created {} files", parquet_files.len());
    }

    #[test]
    fn test_cli_csv_invalid_arguments() {
        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);


        // Test missing required arguments
        let output = Command::new(CLI_APP)
            .args(&["csv",
                "-s", temp_file.path().to_str().unwrap()])
            .output()
            .expect("Failed to execute CLI command");

        assert!(!output.status.success(), "CLI should fail with missing arguments");

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("required") || stderr.contains("missing"),
                "Error message should mention missing/required arguments");

        println!("CLI invalid arguments test passed");
    }

    #[test]
    fn test_cli_parquet_invalid_arguments() {

        // Test missing required arguments
        let output = Command::new(CLI_APP)
            .args(&["parquet"])
            .output()
            .expect("Failed to execute CLI command");

        assert!(!output.status.success(), "CLI should fail with missing arguments");

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("required") || stderr.contains("missing"),
                "Error message should mention missing/required arguments");

        println!("CLI invalid arguments test passed");
    }

    #[test]
    fn test_cli_help_command() {
        let output = Command::new(CLI_APP)
            .args(&["--help"])
            .output()
            .expect("Failed to execute CLI command");

        assert!(output.status.success(), "Help command should succeed");

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("SSIM"), "Help should mention SSIM");
        assert!(stdout.contains("csv") || stdout.contains("parquet"),
                "Help should mention available commands");

        println!("CLI help test passed");
    }


    #[test]
    fn test_cli_different_compressions() {

        let temp_file = create_temp_ssim_file(SAMPLE_SSIM_DATA);

        let compressions = ["snappy", "gzip", "lz4", "zstd", "uncompressed", "brotli", "lzo"];

        for compression in compressions {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");

            let output = Command::new(CLI_APP)
                .args(&[
                    "parquet",
                    "-s", temp_file.path().to_str().unwrap(),
                    "-o", temp_dir.path().to_str().unwrap(),
                    "-c", compression,
                    "-b", "50"
                ])
                .output()
                .expect("Failed to execute CLI command");

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                panic!("CLI command failed with compression {}: {}", compression, stderr);
            }

            // Check that files were created
            let entries = fs::read_dir(temp_dir.path()).expect("Failed to read output directory");
            let parquet_count = entries
                .filter_map(|entry| entry.ok())
                .filter(|entry| {
                    entry.path().extension()
                        .and_then(|ext| ext.to_str())
                        .map_or(false, |ext| ext == "parquet")
                })
                .count();

            assert!(parquet_count > 0, "Should create parquet files with {} compression", compression);
            println!("Compression {} test passed: created {} files", compression, parquet_count);
        }
    }

    #[test]
    fn test_cli_nonexistent_input_file() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let output_file = temp_dir.path().join("output.csv");

        let output = Command::new(&CLI_APP)
            .args(&[
                "csv",
                "-s", "nonexistent_file.ssim",
                "-o", output_file.to_str().unwrap(),
            ])
            .output()
            .expect("Failed to execute CLI command");

        assert!(!output.status.success(), "CLI should fail with nonexistent input file");

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.to_lowercase().contains("error") ||
                    stderr.to_lowercase().contains("failed") ||
                    stderr.to_lowercase().contains("not found"),
                "Error message should indicate file not found");

        println!("CLI nonexistent file test passed");
    }

}