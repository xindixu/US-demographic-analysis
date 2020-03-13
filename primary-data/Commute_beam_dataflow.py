import datetime
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions


class FormatFn(beam.DoFn):
    def process(self, element):
        column_name = [
                    'DP03_0019PE',         # Drive alone
                    'DP03_0020PE',         # Carpooled
                    'DP03_0021PE',         # Public_Transportation
                    'DP03_0022PE',         # Walked
                    'DP03_0023PE',         # Others
                    'DP03_0024PE',         # Work from home
                    'DP03_0025E']         # Mean_Travel_Time_To_Work


        new_label = [
                    'Drive_Alone',
                    'Carpooled',
                    'Public_Transportation',
                    'Walking',
                    'Others',
                    'Work_From_Home',
                    'Mean_Travel_Time_To_Work']
        
        
        for i in column_name:
            value = element.get(i)
            if value is None:
                element[i] = 0
        new_dic = dict()
        name = element.get('NAME')
        num = name[6:]
        new_dic['ZCTA5'] = num
        for i in range(len(column_name)):
            new_dic[new_label[i]] = element.get(column_name[i])
        
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
    google_cloud_options.job_name = 'commute-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)

    sql = 'SELECT NAME, DP03_0019PE, DP03_0020PE, DP03_0021PE, DP03_0022PE, DP03_0023PE, DP03_0024PE, DP03_0025E FROM acs_2018_modeled.Commute'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write original PCollection to input file
    query_results | 'Record original data' >> WriteToText(DIR_PATH + 'input.txt')

    # apply ParDo to format null values to 0 and pass to the next Pardo
    formatted_pcoll = query_results | 'Format income' >> beam.ParDo(FormatFn())

    # write formatted PCollection to output file
    formatted_pcoll | 'Record processed data' >> WriteToText(DIR_PATH + 'output.txt')

    dataset_id = 'acs_2018_modeled'
    table_id = 'Commute_Beam_DF'
    schema_id = 'ZCTA5:STRING,Drive_Alone:FLOAT,Carpooled:FLOAT,Public_Transportation:FLOAT,Walking:FLOAT,Others:FLOAT,Work_From_Home:FLOAT,Mean_Travel_Time_To_Work:FLOAT'

    # write PCollection to new BQ table
    
    formatted_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id,
                                                                   table=table_id,
                                                                   schema=schema_id,
                                                                   project=PROJECT_ID,
                                                                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                   write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                                   batch_size=int(100))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
