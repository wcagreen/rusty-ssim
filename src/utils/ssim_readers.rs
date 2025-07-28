use std::fs::read_to_string;

// TODO Add another read option to handle large ssim files.


/// Reads a whole ssim file into memory.
///
/// # Arguments
/// * `file_path` - The output file path.
///
/// # Errors
/// Returns a String if writing fails, then it errors out.
pub fn read_all_ssim(file_path: &str) -> String {
    let ssim_file: String = read_to_string(file_path).expect("Failed to read in file.");
    return ssim_file;
}
