# ookla-raster-dataset

## Background
The Ookla Speedtest dataset (aka *Global Fixed and Mobile Network Performance Maps*) comes from speedtest.net, which is a tool that allows used to measure internet connection speeds. Ookla has collected global data regarding internet performance including download/upload speeds (mbps), latency (ms), the number of tests and devices, and the download/upload loaded latency speeds (ms). The dataset is valuable in analyzing internet infrastructure and performance trends around the world; moreover, it is beneficial in potentially visualizing disparities by region.

The goal of this project is to process Ookla's provided geospatial datasets and generate raster images from the spatial data. The project reads, manipulates, and visualizes this data by transforming it into a GeoTIFF. The visualization is a five-band raster.

## Objectives
### Efficient Downloading from AWS S3 Buckets
  - A consideration for this project is that AWS S3 Buckets are only accessible without an AWS account if the bucket is public (an important consideration if attempting to gather proprietary data)
### Data Cleaning, Iterative File Management
### Raster Generation

## Quick Start Guide
