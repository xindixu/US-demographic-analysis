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
        ZIPCODE = element.get('ZIPCODE')
        ZCTA5 = element.get('ZCTA5')
        STATE = element.get('STATE')
        new_dic = dict()
        new_dic['ZIPCODE'] = str(ZIPCODE).zfill(5)
        new_dic['ZCTA5'] = str(ZCTA5).zfill(5)
        new_dic['STATE'] = STATE
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
    google_cloud_options.job_name = 'zip_to_zcta5-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)

    sql = 'SELECT * FROM uds_mapper_modeled.zip_to_zcta5'

    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write original PCollection to input file
    query_results | 'Record original data' >> WriteToText(DIR_PATH +
                                                          'ZIP_To_ZCTA5_Beam_DF_input.txt')

    # apply ParDo to format and rename column names and pass to the next Pardo
    formated_pcoll = query_results | 'Format' >> beam.ParDo(
        Format())

    # write formatted PCollection to output file
    formated_pcoll | 'Record processed data' >> WriteToText(DIR_PATH +
                                                            'ZIP_To_ZCTA5_Beam_DF_output.txt')

    dataset_id = 'uds_mapper_modeled'
    table_id = 'ZIP_To_ZCTA5_Beam_DF'
    schema_id = 'ZIPCODE:STRING,STATE:STRING,ZCTA5:STRING'


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
