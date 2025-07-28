use polars::prelude::*;
use pyo3::prelude::*;

pub use rusty_ssim_core::ssim_to_dataframe;


/// Parses an SSIM file and converts it to a Polars DataFrame.
///
/// # Arguments
/// * `file_path` - File path to the SSIM file.
/// # Errors
/// Returns a Polars Dataframe, if fails it errors out.
#[pyfunction]
fn parse_ssim_to_dataframe(py: Python<'_>, file_path: &str) -> PyResult<PyObject> {
    let mut ssim_dataframe = ssim_to_dataframe(file_path)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let mut buffer = Vec::new();
    IpcWriter::new(&mut buffer)
        .finish(&mut ssim_dataframe)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let polars = py.import("polars")?;
    let io = py.import("io")?;
    let bytes_io = io.getattr("BytesIO")?.call1((buffer,))?;

    let df = polars.getattr("read_ipc")?.call1((bytes_io,))?;

    Ok(df.into())
}

#[pymodule]
fn rusty_ssim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_ssim_to_dataframe, m)?)?;
    Ok(())
}
