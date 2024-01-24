
# ETL Pipeline for Spotify Data

## Introduction
This ETL (Extract, Transform, Load) pipeline is designed for processing and analyzing Spotify data. The pipeline extracts data from the Spotify API, transforms it for analytical purposes, and loads it into a PostgreSQL database. This enables deep insights into music trends, artist popularity, playlist preferences, and other data-driven decisions in the music industry.

## Table of Contents
1. Introduction
2. Technical Overview
3. Usage
4. Features
5. Dependencies
6. Configuration
7. Docker Container Setup with AWS ECS
8. Testing
9. Project Implications
10. Contributors

## Technical Overview
- **Extraction**: The pipeline begins with the extraction phase, where it interfaces with the Spotify API using a customized module (spotify_api.py). It handles API authentication, rate limiting, and data retrieval, ensuring comprehensive and reliable data extraction. This includes gathering detailed information about tracks, playlists, artists, and user data, depending on the configured parameters.

- **Transformation:** During the transformation stage, the data undergoes a series of processing steps to convert it into a format suitable for analysis. This involves data cleansing, normalization, and transformation. The transformations.py module is utilized for executing SQL-based transformations and data structuring. It ensures that the data conforms to the target schema, integrates data from multiple sources, and prepares it for analytical querying.

- **Load:** The final stage involves loading the transformed data into a PostgreSQL database. The postgresql.py module manages database connections and operations, including the creation of tables, insertion of data, and execution of SQL queries. This module ensures efficient and secure data transfer into the database, supporting both full loads and incremental updates.

- **Logging and Monitoring:** Throughout the ETL process, the pipeline employs comprehensive logging and monitoring to track its progress and performance. The metadata_logging.py and pipeline_logging.py modules capture essential metadata, log events, and monitor the health of the pipeline, providing insights for troubleshooting and optimization.

- **Testing and Validation:** The pipeline includes a testing framework (test_spotify_api.py) to ensure the reliability and accuracy of the data extraction and transformation processes. This involves unit tests and integration tests that validate the functionality of the API interactions and data processing logic.

- **Containerization and Deployment:** The pipeline is designed to be containerized using Docker, facilitating easy deployment and scaling. The provided Dockerfile outlines the containerization process. Additionally, instructions for deploying the pipeline in a cloud environment using AWS ECS are included, ensuring high availability and scalability.

This ETL pipeline represents a sophisticated data engineering solution, leveraging best practices in software development, data processing, and cloud-based deployment. It is tailored to meet the demanding requirements of data analysis in the music industry, providing a scalable and efficient approach to unlocking insights from Spotify's rich dataset.

## Usage

To run this ETL pipeline locally, follow these steps:
1. Set up your environment variables in the `.env` file. 
2. Install the necessary dependencies using `pip install -r requirements.txt`.
3. Set up your PostgreSQL server.
4. Run the `wrapper_script.sh` to start the ETL process.

Alternatively, you can containerize the pipeline using Docker. Change all SERVER_NAME environmental variables in .env file to `host.docker.internal`:
```
# build spotify_etl docker image
docker build -t spotify_etl:1.0 .

# run spotify_etl docker container
docker run --env-file .env spotify_etl:1.0
```

## Features
- `pipelines.py`: Manages the overall ETL pipeline, including database creation and pipeline execution.
- `postgresql.py`: Defines a PostgreSqlClient class for database interactions, including executing SQL commands and managing table operations.
- `spotify_api.py`: Contains a `SpotifyAPI` class for interfacing with the Spotify API, including methods to retrieve playlists, artists, tracks, and albums.
- `connectors.py`: Provides functions to initialize Spotify client and database connections, and write data to the database.
- `metadata_logging.py`: Includes classes for logging metadata and tracking the ETL process.
- `extract_load_transform.py`: Defines a `SqlTransform` class for extracting and transforming data, and loading it into the database.
- `database_extractor.py`: Features multiple classes (`SqlExtractConfig`, `SqlExtractParser`, `DatabaseTableExtractor`) for extracting data from databases with different configurations.
- `transformations.py`: Contains a `SqlTransform` class for performing SQL-based data transformations.
- `spotify_assets.py`: Includes various functions for extracting and transforming Spotify data like playlists, tracks, albums, and artists.
- `pipeline_logging.py`: Contains a `PipelineLogging` class for logging pipeline execution details.

## Dependencies
The project dependencies are listed in `requirements.txt`. They will be installed in the docker container.

## Configuration
Generate your Spotify API keys: https://developer.spotify.com/dashboard

Configure the project using the `.env` file with the following structure:

```
# Spotify API Credentials
SPOTIFY_CLIENT_ID={your_spotify_client_id}
SPOTIFY_CLIENT_SECRET={your_spotify_client_secret}

# Source Database Configuration
SOURCE_DB_USERNAME={username}
SOURCE_DB_PASSWORD={your_rds_db_password}
SOURCE_SERVER_NAME={your_rds_server_endpoint}
SOURCE_DATABASE_NAME={db_name}
SOURCE_PORT={port}

# Logging Database Configuration
LOGGING_USERNAME={username}
LOGGING_PASSWORD={your_rds_db_password}
LOGGING_SERVER_NAME={your_rds_server_endpoint}
LOGGING_DATABASE_NAME={db_name}
LOGGING_PORT={port}

# Target Database for Processed Data
TARGET_DB_USERNAME={username}
TARGET_DB_PASSWORD={your_rds_db_password}
TARGET_SERVER_NAME={your_rds_server_endpoint}
TARGET_DATABASE_NAME={db_name}
TARGET_PORT={port}
```

## Docker Container Setup with AWS ECS
1. Build the Docker Image
First, build a Docker image using the provided Dockerfile. This image will contain your ETL application and all its dependencies.

```
docker build -t spoty_etl .
```

2. Push the Image to Amazon ECR
Next, push the built image to an Amazon Elastic Container Registry (ECR) repository. If you haven't already created a repository in ECR, you'll need to do so.

```
$(aws ecr get-login --no-include-email --region your-region)
docker tag spoty_etl:latest your-aws-account-id.dkr.ecr.your-region.amazonaws.com/spoty_etl:latest
docker push your-aws-account-id.dkr.ecr.your-region.amazonaws.com/spoty_etl:latest

```

3. Create an ECS Task Definition
In the AWS ECS console, create a new task definition. This task definition will reference the Docker image you pushed to ECR.
Choose the appropriate launch type (EC2).
Define the task execution IAM role which gives your task permissions to make AWS API calls.
Configure the task size, including CPU and memory allocations.
Add a container definition:
Specify the image URL from ECR.
Set essential details like container name, memory limits, and port mappings.
Define environment variables and command if necessary.

4. Configure an ECS Cluster
Set up an ECS cluster if you don't have one. The cluster is a logical grouping of tasks or services.
Go to the ECS console and create a new cluster.
Select the cluster type (EC2).
Follow the prompts to configure cluster settings.

5. Create an ECS Service
Within your cluster, create a new service that will run and maintain the desired number of task instances.
Choose the cluster you created.
Start the "Create Service" wizard.
Select the task definition and revision you created.
Configure the service:
Choose the service type.
Define the number of tasks to run concurrently.
Configure the network and security settings.

6. Start the ECS Service
Once the service is configured, start it. AWS ECS will automatically deploy the specified number of tasks and ensure they are running.
Review your service configuration.
Click "Create Service."
Once the service is created, ECS will start running instances of your task according to your configuration.

7. IAM Role Configuration for S3 Access
To allow your ECS tasks to read the .env file from an Amazon S3 bucket, you need to attach the appropriate permissions to the IAM role used by ECS tasks.
Create or Edit an IAM Role:
Go to the IAM console in AWS.
If you already have an IAM role for your ECS tasks, select it. Otherwise, create a new role.
Ensure the role is assignable to ECS tasks.
Attach Policies for S3 Access:
Attach the AmazonS3ReadOnlyAccess policy to the role. This provides read-only access to all your S3 buckets and objects.
If you need more restricted access, create a custom policy with specific permissions to only read the .env file from the specified bucket, and attach this policy to the role.
Custom Policy Example:

Here's an example policy allowing read access to a specific .env file in a specific bucket:
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::your-bucket-name/path/to/your/.env"
        }
    ]
}
```
Replace your-bucket-name and path/to/your/.env with your actual bucket name and .env file path.
Attach the Role to the ECS Task Definition:
Edit your ECS task definition and attach the IAM role with S3 access.
Save the changes to the task definition.
Environment Variable Retrieval in Application:
Modify your application's startup script or code to retrieve the .env file from S3 upon initialization.
Use AWS SDKs or CLI commands to download the .env file from the S3 bucket.
By configuring this IAM role, your ECS tasks will have the necessary permissions to access and read the .env file from an S3 bucket, ensuring secure and efficient management of configuration settings.

8. Monitor the Service
Use the ECS console to monitor the status of your service and tasks. You can view logs, metrics, and other details to ensure your ETL pipeline is running as expected.

If you encounter issues, refer to the AWS ECS documentation for troubleshooting or consider enabling more detailed logging and monitoring for deeper insights.

## Testing
To run tests, execute the `test_spotify_api.py` script using this command in your terminal:
```
# run tests
 python -m etl_project_test.connectors.test_spotify_api
```

## Project Implications
The ETL Pipeline for Spotify Data is a powerful tool with a range of potential applications and implications in the realms of data analysis, music industry insights, and consumer behavior understanding. Here are some of the key implications:

### Music Industry Insights
- **Trend Identification**: Detect emerging music trends including popular genres, artists, and songs.
- **Artist Popularity Tracking**: Monitor changes in artist popularity, aiding talent scouts and record labels.
### User Behavior and Preferences
- **Enhanced Recommendations**: Leverage listening data to improve personalized playlist recommendations.
- **Listening Trends Analysis**: Uncover patterns in music consumption, like peak listening times and genre preferences.
### Marketing and Promotions
- **Targeted Advertising**: Guide advertising campaigns using data-driven insights for effective promotions.
- **Event Planning Support**: Inform music festival and concert planning with data on current musical trends.
### Academic Research and Cultural Analysis
- **Cultural Studies**: Analyze regional and demographic variations in music preferences for cultural research.
- **Music Therapy Insights**: Contribute to music therapy and psychology by understanding music's impact on mood.
### Data-Driven Predictive Analytics
- **Predictive Modeling**: Create models to forecast music trends.
- **Algorithm Training**: Utilize the data set for machine learning applications, including genre classification and mood-based playlists.
### Business Intelligence for the Music Industry
- **Insights for Artists and Producers**: Provide artists with actionable insights on listener preferences.
- **Performance Metrics Analysis**: Offer detailed metrics for songs and albums to inform marketing strategies.
### Streaming Platform Enhancement
- **User Interface Improvement**: Use data to design more engaging and user-friendly streaming interfaces.
### Global Music Consumption Insights
- **Worldwide Music Trends**: Gain global insights into music preferences, revealing cross-cultural music consumption patterns.

This ETL pipeline not only represents a technical milestone in data processing but also serves as a key to understanding and leveraging music consumption trends and their broader cultural impact.
## Contributors
### André Silva
### Thomas Cortebeeck
### Gabrielle Klimovitsky

---
