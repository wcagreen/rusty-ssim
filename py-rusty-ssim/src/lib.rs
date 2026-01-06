use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

pub use rusty_ssim_core::{ssim_to_csv, ssim_to_dataframe, ssim_to_dataframes, ssim_to_parquets};

#[inline(always)]
fn runtime_error(msg: String) -> PyErr {
    PyRuntimeError::new_err(msg)
}

#[inline(always)]
fn value_error(msg: String) -> PyErr {
    PyValueError::new_err(msg)
}

#[pyfunction]
#[pyo3(signature = (file_path, output_path, batch_size=10000, buffer_size=8192, condense_segments=false))]
fn parse_ssim_to_csv(
    py: Python<'_>,
    file_path: &str,
    output_path: &str,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
    condense_segments: Option<bool>,
) -> PyResult<()> {
    py.allow_threads(|| ssim_to_csv(file_path, output_path, batch_size, buffer_size, condense_segments))
        .map_err(|e| runtime_error(format!("Failed to process SSIM file: {}", e)))
}

#[pyfunction]
#[pyo3(signature = (file_path, output_path=".", compression="uncompressed", batch_size=10000, buffer_size=8192, condense_segments=false))]
fn parse_ssim_to_parquets(
    py: Python<'_>,
    file_path: &str,
    output_path: Option<&str>,
    compression: Option<&str>,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
    condense_segments: Option<bool>,
) -> PyResult<()> {
    py.allow_threads(|| ssim_to_parquets(file_path, output_path, compression, batch_size, buffer_size, condense_segments))
        .map_err(|e| runtime_error(format!("Failed to process SSIM file: {}", e)))
}

#[pyfunction]
#[pyo3(signature = (file_path, batch_size=10000, buffer_size=8192))]
fn split_ssim_to_dataframes(
    py: Python<'_>,
    file_path: &str,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
) -> PyResult<(PyDataFrame, PyDataFrame, PyDataFrame)> {
    let (carrier_df, flights_df, segments_df) = py
        .allow_threads(|| ssim_to_dataframes(file_path, batch_size, buffer_size))
        .map_err(|e| value_error(e.to_string()))?;

    Ok((
        PyDataFrame(carrier_df),
        PyDataFrame(flights_df),
        PyDataFrame(segments_df),
    ))
}

#[pyfunction]
#[pyo3(signature = (file_path, batch_size=10000, buffer_size=8192, condense_segments=false))]
fn parse_ssim_to_dataframe(
    py: Python<'_>,
    file_path: &str,
    batch_size: Option<usize>,
    buffer_size: Option<usize>,
    condense_segments: Option<bool>,
) -> PyResult<PyDataFrame> {
    let ssim_dataframe = py
        .allow_threads(|| ssim_to_dataframe(file_path, batch_size, buffer_size, condense_segments))
        .map_err(|e| value_error(e.to_string()))?;

    Ok(PyDataFrame(ssim_dataframe))
}

#[pymodule]
fn rustyssim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_ssim_to_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(split_ssim_to_dataframes, m)?)?;
    m.add_function(wrap_pyfunction!(parse_ssim_to_csv, m)?)?;
    m.add_function(wrap_pyfunction!(parse_ssim_to_parquets, m)?)?;
    Ok(())
}
