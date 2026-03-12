# Thumbtack ML POC: Synthetic Traffic Producer

## Overview
This project serves as the **Data Generation Layer** for the Thumbtack GPU-Accelerated ML POC. It acts as a high-fidelity simulator for the Thumbtack website, generating messy, natural-language service requests and staging them in S3 for downstream GPU processing.

The primary goal is to validate that Astronomer's GPU-enabled clusters can handle the scale and "messiness" of Thumbtack's category classification models under realistic production conditions.

## Project Contents
Your Astro project contains the following files and folders:

- **dags**: This folder contains the simulation logic for the POC:
    - `thumbtack_producer`: This DAG uses the Airflow 3.1 Task SDK to generate thousands of unique project descriptions across 27 service categories. It mimics human typing quirks (typos, mixed casing), strictly follows business hours (8 AM - 7 PM EST), and simulates mid-day traffic surges. It utilizes a **Dataset Outlet** to signal data availability to downstream consumers.
- **Dockerfile**: Contains the versioned Astro Runtime image. This project leverages Airflow 3.1 features for high-performance task execution.
- **requirements.txt**: Includes the necessary libraries for high-speed Parquet generation and data manipulation:
    - `pandas`
    - `pyarrow`
    - `numpy`
    - `pendulum`
- **include**: Empty by default; used for additional SQL or metadata files.
- **packages.txt**: Used for OS-level dependencies if required by the Parquet engine.

## Architecture & Handoff

1. **Bronze Generation**: The Producer generates raw Parquet files and drops them into a partitioned structure: `s3://thumbtack-poc-staging/inbound/YYYY-MM-DD/`.
2. **Geographic Diversity**: Data points are generated across 25+ major US metropolitan hubs to test geographical transformation logic in Databricks.
3. **Data-Aware Handoff**: Instead of polling S3 with brittle sensors, the Producer now broadcasts a successful update to the `Dataset("s3://thumbtack-poc-staging/inbound/")`.
4. **Consumer Trigger**: A downstream **Inference DAG** (hosted in a separate cluster) is scheduled directly on this Dataset. It triggers immediately upon the Producer's completion, ensuring 100% compute efficiency by only activating GPU node pools when fresh data is ready for processing.

## Connection Setup

To run the Producer DAG, ensure the following connection is configured in your Astro Deployment:

| Connection ID | Type | Permissions |
| :--- | :--- | :--- |
| `thumbtack` | Amazon Web Services | `s3:PutObject` and `s3:ListBucket` on `thumbtack-poc-staging` |

## Deploy Your Project Locally

Start Airflow on your local machine by running:
`astro dev start`

When the containers are ready, access the Airflow UI at http://localhost:8080/. Ensure your local `airflow_settings.yaml` or Environment Variables include the `aws_fe_srvc_account` credentials to allow the simulator to write to S3.

## Deploy Your Project to Astronomer

To push this Producer project to your dedicated Thumbtack-Sandbox or Demo deployment, run:
`astro deployment deploy <deployment-id>`

## Contact
Ravi Jha (ravi.jha@astronomer.io) or Slack for questions
