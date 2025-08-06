use polars::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

pub use rusty_ssim_core::{ssim_to_csv, ssim_to_dataframe, ssim_to_dataframes, ssim_to_parquets};

#[pyfunction]
#[pyo3(signature = (file_path, output_path, batch_size=10000))]
fn parse_ssim_to_csv(
    _py: Python<'_>,
    file_path: &str,
    output_path: &str,
    batch_size: Option<usize>,
) -> PyResult<()> {
    ssim_to_csv(file_path, output_path, batch_size).map_err(|e| match e {
        _ => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to process SSIM file: {}",
            e
        )),
    })?;

    Ok(())
}

#[pyfunction]
#[pyo3(signature = (file_path, output_path=".", compression="uncompressed", batch_size=10000))]
fn parse_ssim_to_parquets(
    _py: Python<'_>,
    file_path: &str,
    output_path: Option<&str>,
    compression: Option<&str>,
    batch_size: Option<usize>,
) -> PyResult<()> {
    ssim_to_parquets(file_path, output_path, compression, batch_size).map_err(|e| match e {
        _ => PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
            "Failed to process SSIM file: {}",
            e
        )),
    })?;

    Ok(())
}

#[pyfunction]
#[pyo3(signature = (file_path, batch_size=10000))]
fn split_ssim_to_dataframes(
    py: Python<'_>,
    file_path: &str,
    batch_size: Option<usize>,
) -> PyResult<Py<PyAny>> {
    let (carrier_df, flights_df, segments_df) = ssim_to_dataframes(file_path, batch_size)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let polars = py.import("polars")?;
    let io = py.import("io")?;
    let read_ipc = polars.getattr("read_ipc")?;
    let bytes_io_class = io.getattr("BytesIO")?;

    let dataframe_to_python = |df: DataFrame| -> PyResult<Py<PyAny>> {
        let mut buffer = Vec::new();
        IpcWriter::new(&mut buffer)
            .finish(&mut df.clone())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let bytes_io = bytes_io_class.call1((buffer,))?;
        Ok(read_ipc.call1((bytes_io,))?.into())
    };

    let carrier_py = dataframe_to_python(carrier_df)?;
    let flights_py = dataframe_to_python(flights_df)?;
    let segments_py = dataframe_to_python(segments_df)?;

    let py_tuple = PyTuple::new(py, &[carrier_py, flights_py, segments_py]);

    Ok(py_tuple?.into())
}

#[pyfunction]
#[pyo3(signature = (file_path, batch_size=10000))]
fn parse_ssim_to_dataframe(
    py: Python<'_>,
    file_path: &str,
    batch_size: Option<usize>,
) -> PyResult<PyObject> {
    let mut ssim_dataframe = ssim_to_dataframe(file_path, batch_size)
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
fn rustyssim(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_ssim_to_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(split_ssim_to_dataframes, m)?)?;
    m.add_function(wrap_pyfunction!(parse_ssim_to_csv, m)?)?;
    m.add_function(wrap_pyfunction!(parse_ssim_to_parquets, m)?)?;
    Ok(())
}
