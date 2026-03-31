# Ookla Pipeline Welcome

## Quick Start Guide

1. ### Prereqs
  * Docker and Docker Compose
  * Internet Connection (required to fetch data from Ookla's S3 open-data bucket)
2. ### Installation
```
git clone https://github.com/mrsanford/ookla-raster-dataset.git
cd ookla-raster-dataset
docker-compose up
```
3. ### Execution (via Docker)
The best way to run the pipeline end-to-end is with Docker Compose. It avoids configuration issues.
```
docker compose up --build
```
4. ### Project Structure
The files are managed as ```Path``` objects from pathlib.

## Background
The Ookla Speedtest data (*Global Fixed and Mobile Network Peformance Maps*) is collected by [Speedtest](https://www.speedtest.net), a platform that measures internet connection speeds. Ookla collects global data on internet performance including download/upload (mbps), latency (ms), number of tests and devices, and download/upload loaded latency speeds (ms).

The datasets is valuable in analyzing internet infrastructure and performance trends, especially in its potential in visualizing socioeconomic disparities in underdeveloped regions.  

The projec processes Ookla's open-source geospatial datasets, processes and transforms, and generates raster products. The output raster is a five-band GeoTIFF, where each band represents the aforementioned data points mentioned above (e.g., download/upload speeds, latency (ms), number of tests, etc.).

## Pipeline Architecture


## Configuration
Parameters are availabile for customization in ```utils/helpers.py```
* **ZOOM_LEVEL**: Adjusts the resolution of the ouptput grid (2^16 by default corresponds to ZOOM_LEVEL=16)
* **BAND_COLS**: Define which metrics (e.g, `avg_d_kbps`,`tests`) are mapped to specific raster bands
* **QUARTERS**: Select time range from Ookla's S3 archive

## Custom Logging Architecture
You can adjust custom loggers or monitor the process of any pipeline stage with custom logging. The following example will return the logs from the download stage.
```tail -f logs/DOWNLOAD.log```

## Example Visualization
