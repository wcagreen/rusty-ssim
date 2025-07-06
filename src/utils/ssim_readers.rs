use std::fs::read_to_string;

pub fn read_all_ssim(file_path: &str) -> String {
    let ssim_file: String = read_to_string(file_path).expect("Failed to read in file.");
    return ssim_file;
}
