# healthcare_pipeline.py
import apache_beam as beam
import argparse
import pandas as pd
# Import other libraries for preprocessing

class ProcessPatientData(beam.DoFn):
    def process(self, element):
        # Assuming element is a single line from a CSV
        # Parse the CSV line
        values = element.split(',') # Basic split, use csv module for robustness
        # Example basic cleaning/transformation (replace with your actual logic)
        processed_values = []
        for i, value in enumerate(values):
            if i == 0: # Example: Convert first column to integer
                processed_values.append(value)
            elif i == 2: # Example: Handle missing in third column
                 processed_values.append(value if value else 'Unknown')
            else:
                processed_values.append(value)
        # Return the processed data, perhaps as a comma-separated string or a dictionary
        yield ','.join(map(str, processed_values))

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        required=True,
                        help='Input file(s) in Cloud Storage (e.g. gs://your-bucket/raw/*.csv)')
    parser.add_argument('--output',
                        dest='output',
                        required=True,
                        help='Output file path in Cloud Storage (e.g. gs://your-bucket/processed/)')
    known_args, pipeline_args = parser.parse_known_args()

    # Define pipeline options
    pipeline_options = beam.options.pipeline_options.PipelineOptions(pipeline_args)
    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True # Needed for some libraries

    # Explicitly set DataflowRunner
    pipeline_args.append('--runner=DataflowRunner')
    pipeline_args.append('--project=gen-lang-client-0404218523') # Replace with your project ID
    pipeline_args.append('--region=us-central1') # Replace with your GCP region
    pipeline_args.append('--staging_location=gs://sh-pipeline/staging/') # Replace with a staging location
    pipeline_args.append('--temp_location=gs://sh-pipeline/temp/') # Replace with a temporary location


    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'ReadFromGCS' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1) # Add skip_header_lines=1 if your CSV has a header
            | 'ProcessData' >> beam.ParDo(ProcessPatientData())
            | 'WriteToGCS' >> beam.io.WriteToText(known_args.output, num_shards=1) # Adjust num_shards as needed
        )

if __name__ == '__main__':
    run()
