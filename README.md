# GitHub Data Processing Project

## Overview:
This project aims to fetch data from the GitHub API, process it, and store it in a parquet file. It consists of two main components: retrieving information from Github API and then transforming it using PySpark.

## Components:
1. get_github_info.py: contains functions to fetch data from the GitHub API.
2. transform_data.py: contains functions to tranform raw GitHub data into a structured schema using PySpark.
3. main.py: run main.py to run the project.

## Usage:
1. Ensure that the required environment variables, such as GITHUB_TOKEN, are set up in the .env file.
2. Run main.py to start the data retrieval and transformation process.
3. The processed data will be stored in Parquet format in the github_info.parquet file.

## Requirements:
- python 3.10
