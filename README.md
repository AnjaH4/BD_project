# New York Parking Tickets - Big Data Project 2023/24

## Overview
This repository contains the code, data, and resources for the New York Parking Tickets Big Data Project for the 2023/24 academic year. The project involves comprehensive data analysis on parking violations in New York City from 2014 to 2024. Key tasks include data ingestion, augmentation, exploratory data analysis, streaming data processing, batch processing with machine learning, and performance evaluation across various data formats.

## Project Structure

### Data
The `data/` directory contains all data aggregates generated by the project. These aggregates are primarily the processed outputs of bigger datasets from the `augment_data.ipynb` notebook.

### Plots
The `plots/` directory stores all visualizations and graphical outputs from the exploratory data analysis (EDA) and other parts of the project.

### Source Code (`src/`)
The `src/` directory is the main source code folder, organized into subdirectories based on different stages of the project:
- **1-filetypes/**: Code for transforming different file formats (csv, parquet, hdf5) used throughout the project.
- **2-data-augment/**: Scripts and notebooks for data augmentation from different sources.
- **3-data-eda/**: Notebooks and scripts for exploratory data analysis (EDA), providing insights and cleaning the data into the dataset before further processing.
- **4-kafka/**: Code related to streaming data processing using Kafka.
- **5-machine-learning/**: Machine learning models for batch processing and prediction.
- **eda_DuckDB/**: Code using DuckDB, for exploratory data analysis, optimized for performance and handling large datasets.
- **time_memory_comparisson/**: Scripts and resources for comparing time and memory usage across different data processing methods.

### Documentation
- **`Instructions.pdf`**: Project Instructions
- **`Report_Kolar_Hrvatic.pdf`**: The final report for the project, which includes a summary of the project goals, methodologies, results, and conclusions.

## Getting Started
To get started with this project, clone the repository and set up the environment using the `environment.yml` file:

```bash
git clone https://github.com/AnjaH4/BD_project
cd your-repo-folder
conda env create -f environment.yml
conda activate your-environment-name
