#!/usr/bin/env python3
import apache_beam as beam
import os
import datetime

def processline(line):
    strline = line.decode('utf-8').strip()  # decode bytes from Pub/Sub
    parameters = strline.split(",")
   
    try:
        if len(parameters) != 7:
            yield beam.pvalue.TaggedOutput('malformed', {'message': strline})
            return

        outputrow = {
        'timestamp': parameters[0],
        'ipaddr': parameters[1],
        'action': parameters[2],
        'srcacct': parameters[3],
        'destacct': parameters[4],
        'amount': float(parameters[5]),
        'customername': parameters[6],
        }

    # Valid record goes to main output
        yield outputrow

    except Exception:
        # Catch parse errors (e.g. amount not numeric)
        yield beam.pvalue.TaggedOutput('malformed', {'message': strline})


def run():
    projectname = os.getenv('GOOGLE_CLOUD_PROJECT')
    bucketname = os.getenv('GOOGLE_CLOUD_PROJECT') + '-bucket'
    jobname = 'mars-job-' + datetime.datetime.now().strftime("%Y%m%d%H%M")
    region = 'us-central1'

    # https://cloud.google.com/dataflow/docs/reference/pipeline-options
    argv = [
      '--streaming',
      '--runner=DataflowRunner',
      '--project=' + projectname,
      '--job_name=' + jobname,
      '--region=' + region,
      '--staging_location=gs://' + bucketname + '/staging/',
      '--temp_location=gs://' + bucketname + '/temploc/',
      '--max_num_workers=2',
      '--machine_type=e2-standard-2',
   #   '--service_account_email=marssa@' + projectname + ".iam.gserviceaccount.com"
      '--save_main_session'
    ]

    p = beam.Pipeline(argv=argv)
    subscription = "projects/" + projectname + "/subscriptions/mars-activities"
    outputtable = projectname + ":mars.activities"
    errortable = projectname + ":mars.raw"
    
    print("Starting Beam Job - next step start the pipeline")
    processed = (
        p
        | 'Read Messages' >> beam.io.ReadFromPubSub(subscription=subscription)
        | 'Process Lines' >> beam.FlatMap(processline).with_outputs('malformed', main='valid')
    )
    processed.valid | 'Write Valid' >> beam.io.WriteToBigQuery(outputtable)
    processed.malformed | 'Write Malformed' >> beam.io.WriteToBigQuery(errortable)
    p.run().wait_until_finish()


if __name__ == '__main__':
    run()
