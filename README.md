# Real Time Data Streaming and Analysis
 ### Building a pipeline that continuously ingests simulated patient telemetry data, processes it in real-time, and lands the aggregated data into BigQuery for immediate analysis.
### Using Google Cloud Pub/Sub, the script takes the messages and pushes it into Dataflow to be transformed into a format that is usable for BigQuery
<p>
 <ul>
  <p>GCP Services Used:</p>

<li>Cloud Pub/Sub </li>
<li>Cloud Dataflow (Streaming Mode)</li>
<li>BigQuery: As the real-time analytics data warehouse where your processed data will be stored.</li>
 </ul>
</p>
<h2> Install Python 3 and the necessary libraries into the environment</h2>
pip install apache-beam[gcp] google-cloud-pubsub google-cloud-bigquery pandas 
<h2>Create Pub/Sub Topic, Subscription</h2>
<p>gcloud pubsub topics create telemetry-data-stream --project YOUR_GCP_PROJECT_ID</p>
<p>gcloud pubsub subscriptions create telemetry-data-stream-sub \
  --topic telemetry-data-stream \
  --ack-deadline=600 \
  --message-retention-duration=7d \
  --project YOUR_GCP_PROJECT_ID
</p>
<h2>Create Big Query Dataset to house tables</h2>
gcloud bq mk --location=us-central1 YOUR_GCP_PROJECT_ID:telemetry_analytics
<h2>Create a JSON file with the telemetry schema</h2>
<h2>Create the table in BigQuery</h2>
