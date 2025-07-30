use polars::prelude::*;
use pyo3::prelude::*;
use pyo3::types::PyTuple;

pub use rusty_ssim_core::ssim_to_dataframe;
pub use rusty_ssim_core::ssim_to_dataframes;



#[pyfunction]
#[pyo3(text_signature = "(file_path, /)")]
/// Parse SSIM file into Polars DataFrames (types 2, 3, 4)
///
/// # Arguments
/// * `file_path` - File path to the SSIM file.
/// # Errors
/// Returns three Polar Dataframes if it fails it errors out.
fn split_ssim_to_dataframes(py: Python<'_>, file_path: &str, streaming: Option<bool>, batch_size: Option<usize>) -> PyResult<Py<PyAny>>  {
    let (carrier_df, flights_df, segments_df) = ssim_to_dataframes(file_path, streaming, batch_size).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;

    let polars = py.import("polars")?;
    let io = py.import("io")?;
    let read_ipc = polars.getattr("read_ipc")?;
    let bytes_io_class = io.getattr("BytesIO")?;

    let dataframe_to_python = | df: DataFrame | -> PyResult<Py<PyAny>>  {
        let mut buffer = Vec::new();
        IpcWriter::new(&mut buffer).finish(&mut df.clone()).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string()))?;
        let bytes_io = bytes_io_class.call1((buffer,))?;
        Ok(read_ipc.call1((bytes_io,))?.into())
    };

    let carrier_py = dataframe_to_python(carrier_df)?;
    let flights_py  = dataframe_to_python(flights_df)?;
    let segments_py  = dataframe_to_python(segments_df)?;

    let py_tuple = PyTuple::new(py, &[carrier_py, flights_py, segments_py]);
    
    Ok(py_tuple?.into())
}



#[pyfunction]
#[pyo3(text_signature = "(file_path, /)")]
/// Parses an SSIM file and converts it to a Polars DataFrame.
///
/// # Arguments
/// * `file_path` - File path to the SSIM file.
/// # Errors
/// Returns a Polar Dataframe if it fails it errors out.
fn parse_ssim_to_dataframe(py: Python<'_>, file_path: &str, streaming: Option<bool>, batch_size: Option<usize>) -> PyResult<PyObject> {
    let mut ssim_dataframe = ssim_to_dataframe(file_path, streaming, batch_size)
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
    m.add_function(wrap_pyfunction!(split_ssim_to_dataframes, m)?)?;
    Ok(())
}
