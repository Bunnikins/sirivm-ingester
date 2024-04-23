# BODS Siri VM processor

This project provides a Python-based solution for processing Siri VM data from the BODS API and also the Siri VM bulk download endpoint. It's designed to fetch, extract, transform, and load (ETL) data for further analysis or storage in a SQLite database.

## Features

- **API Data Extraction**: Fetches XML data from specified API endpoints.
- **ZIP File Processing**: Extracts and processes XML data from ZIP archives.
- **Data Transformation**: Applies necessary transformations to the extracted data.
- **Database Integration**: Loads transformed data into a SQLite database, ensuring data integrity and avoiding duplicates.

## Getting Started

### Prerequisites

- Python 3.x
- `requests` library for API requests
- `lxml` library for XML parsing
- `sqlite3` module (included with Python)

### Installation

1. Clone the repository to your local machine:

   ```bash
   git clone https://github.com/yourusername/yourprojectname.git
   ```

2. Navigate to the project directory:

   ```bash
   cd yourprojectname
   ```

3. Install the required Python packages:

   ```bash
   pip install -r requirements.txt
   ```

### Usage

To start processing data, run the `main.py` script with the desired mode (`api` or `zip`):

```bash
python main.py api
```

or

```bash
python main.py zip
```

Ensure you set the `BODS_API_KEY` environment variable for API access:

```bash
export BODS_API_KEY='your_api_key_here'
```

## Modules

- **`main.py`**: The entry point of the application, handling user input to select the data source.
- **`api_extract.py`**: Manages fetching and processing data from APIs.
- **`zip_extract.py`**: Handles extraction and processing of XML data from ZIP files.
- **`transform_load.py`**: Contains functions for data transformation and loading into the database.
- **`CSVview.py`**: Contains a script you can run on the DB to summarise the contents in CSV form.
- **`HTMLview.py`**: Contains a script you can run on the DB to summarise the contents in HTML form.
- **`measure_latency.py`**: A script to compare the response time to the recorded at time to find the record with the best latency.

## Alternative method using direct ingestion from s3

Configure valid AWS credentials as environment variables by either exporting AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION or AWS_PROFILE
Configure the name of the s3 bucket to ingest from by exporting S3_BUCKET

Execute get_from_s3.py


## Configuration

- API endpoints and parameters can be configured in `main.py`.
- ZIP file URLs are specified in `main.py` for the ZIP processing mode.
- The SQLite database file is dynamically generated with a timestamped filename.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues to discuss proposed changes or report bugs.

## License

This project is licensed under the MIT License.


