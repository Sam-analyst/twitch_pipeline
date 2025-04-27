# twitch_pipeline
This repository contains logic for a Twitch data pipeline built using **Astronomer** (Airflow).

There are two DAGs in this repo:
- **`get_raw_twitch_data`** - Runs every 4 hours. Extracts Twitch data via their public API and stores the raw data into an AWS S3 bucket.
- **`transform_raw_twitch_data`** - Runs once per day. Transforms the raw data and loads it into Apache Iceberg tables.

### Setup 
---

This section outlines how to run the pipeline locally and what’s needed from AWS.

#### Local Environment

1. Clone this repo onto your local machine.
``` bash
git clone https://github.com/Sam-analyst/twitch_pipeline.git
```
2. Install the Astro CLI: Follow these [intructions](https://www.astronomer.io/docs/astro/cli/install-cli?tab=linux#install-the-astro-cli) for installation.

3. Create a `.env` file.
``` bash
touch .env
```
Add the following environment variables (replace with your own values):
```
TWITCH_CLIENT_ID="your-twitch-client-id"
TWITCH_ACCESS_TOKEN="your-twitch-access-token"
AWS_ACCESS_KEY_ID="your-aws-key-id"
AWS_SECRET_ACCESS_KEY="your-aws-secret-access-key"
AWS_DEFAULT_REGION="your-aws-default-region"
```
- For Twitch credentials, follow the instructions [here](https://github.com/Sam-analyst/twitch_game_analytics/blob/main/README.md).
- For AWS credentials, create a new IAM user and access keys in the AWS Console.

4. Email on DAG failure (Optional): The DAGs are configured to send email alerts on failure (currently set to my email). If you'd like to configure this for yourself:
    - Watch this [YouTube video](https://www.youtube.com/watch?v=D18G7hW8418).
    - Refer to Astronmer's [email notification guide](https://www.astronomer.io/docs/astro/airflow-email-notifications/).

5. Start the Airflow environment
```
astro dev start
```
Then go to [localhost:8080](http://localhost:8080) to access the UI.

6. Set up Airflow AWS connection:
    -  In the Airflow UI: go to **Admin > Connections**
    -  Create a new connection called `aws_conn` with the following configurations:
        - Conn ID: `aws_conn`
        - Conn Type: `Amazon Web Services`
        - AWS Access Key ID: your `AWS_ACCESS_KEY_ID`
        - AWS Secret Access Key: your `AWS_SECRET_ACCESS_KEY`
        - Extra: `{"AWS_DEFAULT_REGION": "your-aws-default-region"}`

#### Infrastructure
- The pipeline writes to a personal S3 bucket. To use your own:
    - Create a new bucket in AWS S3.
    - Update the DAG code to reference your new bucket name.

- Iceberg Tables:
    - These must be created manually.
    - Use AWS Athena to define the tables.
    - Register the tables in AWS Glue so that pyiceberg can interact with the tables.
 
#### Additonal notes
- The aws credentials are being defined in two places. This is not ideal, but I couldn't get pyiceberg to recognize the `aws_conn` object. Ideally, we should use the `aws_conn` connection for authentication with pyiceberg.
