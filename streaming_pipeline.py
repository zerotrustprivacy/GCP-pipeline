import argparse
import json
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.combiners import MeanCombineFn

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ParseMessage(beam.DoFn):
    """Parses the Pub/Sub message (JSON string) into a dictionary."""
    def process(self, element):
        try:
            yield json.loads(element.decode('utf-8'))
        except json.JSONDecodeError as e:
            logging.error(f"Could not parse JSON: {element}. Error: {e}")

class ExtractPatientHeartRate(beam.DoFn):
    """Extracts patient_id and heart_rate, ensuring data types."""
    def process(self, element):
        try:
            patient_id = element.get("patient_id")
            heart_rate = element.get("heart_rate")
            
            # Basic validation
            if patient_id and isinstance(heart_rate, (int, float)):
                yield {"patient_id": patient_id, "heart_rate": int(heart_rate)}
            else:
                logging.warning(f"Skipping malformed message: {element}")
        except Exception as e:
            logging.error(f"Error extracting data from message: {element}. Error: {e}")

def run():
    parser = argparse.ArgumentParser(description='Real-time Telemetry Processing Pipeline')
    parser.add_argument('--project', dest='project', required=True,
                        help='Your Google Cloud Project ID.')
    parser.add_argument('--region', dest='region', default='us-central1',
                        help='The GCP region to run the Dataflow job in.')
    parser.add_argument('--input_subscription', dest='input_subscription', required=True,
                        help='Pub/Sub Subscription ID to read from (e.g., telemetry-data-stream-sub).')
    parser.add_argument('--output_table', dest='output_table', required=True,
                        help='BigQuery table to write results to (e.g., telemetry_analytics.heart_rate_summary_no_partitioning).')
    parser.add_argument('--temp_location', dest='temp_location', required=True,
                        help='Cloud Storage location for temporary files (e.g., gs://your-bucket/tmp).')
    parser.add_argument('--staging_location', dest='staging_location', required=True,
                        help='Cloud Storage location for staging files (e.g., gs://your-bucket/staging).')
    parser.add_argument('--job_name', dest='job_name', default='telemetry-streaming-job',
                        help='Unique job name for Dataflow.')

    args, beam_args = parser.parse_known_args()

    # Configure PipelineOptions for streaming
    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(StandardOptions).streaming = True # CRITICAL for streaming mode

    # Configure Google Cloud specific options
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = args.project
    google_cloud_options.region = args.region
    google_cloud_options.temp_location = args.temp_location
    google_cloud_options.staging_location = args.staging_location
    google_cloud_options.job_name = args.job_name
    google_cloud_options.runner = 'DataflowRunner' # Ensure DataflowRunner is used

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(subscription=f'projects/{args.project}/subscriptions/{args.input_subscription}')
            | 'ParseJson' >> beam.ParDo(ParseMessage())
            | 'ExtractData' >> beam.ParDo(ExtractPatientHeartRate())
            # Group data into 1-minute fixed windows for aggregation
            # Key by patient_id before windowing if you want per-patient aggregation within window
            | 'WindowBy1Minute' >> beam.WindowInto(beam.window.FixedWindows(60)) # 60 seconds = 1 minute
            | 'GroupByPatient' >> beam.GroupByKey() # Group by patient_id if not keyed
            | 'AggregateHeartRates' >> beam.CombineValues(MeanCombineFn()) # Simple mean example, expand for min/max/count
            
            # The CombineValues(MeanCombineFn()) requires input in (key, value) pairs.
            # So, the input to AggregateHeartRates needs to be (patient_id, heart_rate)
            # Let's adjust for correct keying before CombineValues:
            # | 'KeyByPatient' >> beam.Map(lambda x: (x['patient_id'], x['heart_rate']))
            # | 'WindowBy1Minute' >> beam.WindowInto(beam.window.FixedWindows(60))
            # | 'GroupAndAggregate' >> beam.CombinePerKey(MeanCombineFn()) # Combines values per key within window

            # Let's rewrite the aggregation part more clearly for this schema:
            | 'KeyByPatientId' >> beam.Map(lambda x: (x['patient_id'], x['heart_rate']))
            | 'Window' >> beam.WindowInto(beam.window.FixedWindows(60)) # 1-minute window
            | 'GroupAndAggregate' >> beam.CombinePerKey(
                lambda rates: {
                    'avg_heart_rate': sum(rates) / len(rates) if rates else None,
                    'min_heart_rate': min(rates) if rates else None,
                    'max_heart_rate': max(rates) if rates else None,
                    'num_readings': len(rates)
                }
            )
            # Format results for BigQuery
            | 'FormatForBigQuery' >> beam.Map(lambda element, window: {
                'start_time': window.start.to_rfc3339(),
                'end_time': window.end.to_rfc3339(),
                'patient_id': element[0],
                'avg_heart_rate': element[1]['avg_heart_rate'],
                'min_heart_rate': element[1]['min_heart_rate'],
                'max_heart_rate': element[1]['max_heart_rate'],
                'num_readings': element[1]['num_readings']
            }, window=beam.DoFn.WindowParam)
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                table=args.output_table,
                schema='AUTO_DETECT', # Or provide a schema if not auto-detecting
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND # Append new data
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
