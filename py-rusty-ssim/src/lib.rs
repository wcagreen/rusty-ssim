use pyo3::prelude::*;
use polars::prelude::*;
use polars::io::ipc::IpcWriter;
use pyo3::types::PyBytes;

pub use rusty_ssim_core::{ssim_to_dataframe};

#[pyfunction]
fn parse_ssim_to_dataframe(py: Python<'_>, file_path: &str) -> PyResult<Py<PyBytes>>  {
    let mut ssim_dataframe = ssim_to_dataframe(file_path).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
    let mut buffer = Vec::new();
    IpcWriter::new(&mut buffer)
        .finish(&mut ssim_dataframe)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    Ok(PyBytes::new(py, &buffer).into())
}

#[pymodule]
fn rusty_ssim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_ssim_to_dataframe, m)?)?;
    Ok(())
}