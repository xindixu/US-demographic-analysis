import datetime
import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions


class Format(beam.DoFn):
    def process(self, element):
        new_dic = element
        ZIPCODE = element.get('ZIPCODE')
        if len(ZIPCODE) > 5:
            ZIPCODE = ZIPCODE[:5]
        new_dic['ZIPCODE'] = ZIPCODE
        return [new_dic]


def run():
    PROJECT_ID = 'sashimi-266523'  # change to your project id
    BUCKET = 'gs://sashimi-sushi'  # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + \
        datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'private-school-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)

    sql = 'SELECT * FROM edge_1718_modeled.private_school'

    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write original PCollection to input file
    query_results | 'Record original data' >> WriteToText(DIR_PATH +
                                                          'private_school_Beam_DF_input.txt')

    # apply ParDo to format and rename column names and pass to the next Pardo
    formated_pcoll = query_results | 'Format' >> beam.ParDo(
        Format())

    # write formatted PCollection to output file
    formated_pcoll | 'Record processed data' >> WriteToText(DIR_PATH +
                                                            'private_school_Beam_DF_output.txt')

    dataset_id = 'edge_1718_workflow_modeled'
    table_id = 'private_school_Beam_DF'
    schema_id = 'SCHOOLID:STRING,NAME:STRING,STREET:STRING,CITY:STRING,STATE:STRING,ZIPCODE:STRING,LONGITUDE:FLOAT,LATITUDE:FLOAT'

    # write PCollection to new BQ table
    formated_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id,
                                                                 table=table_id,
                                                                 schema=schema_id,
                                                                 project=PROJECT_ID,
                                                                 create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                 write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()
