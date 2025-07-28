# rusty-ssim
A Rust built IATA SSIM parser tool that can be run either by CLI or Python. This tool can parse ssim file into dataframe. There is a plan to add JSON option in the future.

## Installation

### CLI Installation
```
git clone https://github.com/wcagreen/rusty-ssim.git
cd rusty-ssim
cargo build -p cli-rusty-simm --release
```
You will then need to set it to your path to run the ssim cli applications.

### Python Installation
Currently, this package is not in pypi, but I am planning on trying to upload it to pypi in the future.
```
git clone https://github.com/wcagreen/rusty-ssim.git
cd rusty-ssim
maturin develop -m py-rusty-ssim/Cargo.toml
```

## Examples

### CLI Example

```
ssim --ssim-path .\test_files\ssim_file.dat --output-path test.csv --file-type csv --compression uncompressed
```

### Python Example

``` 
import rusty_ssim as rs

df = rs.parse_ssim_to_dataframe(".\test_files\ssim_file.dat")
```


## Contributing
If you like to contribute, then feel free to reach out.

## License
This project is licensed under the MIT License.