import datetime
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions



class FormatColumnFn(beam.DoFn):
    def process(self, element):
        column_label = ['DP04_0017PE',     # 2014 or later
                        'DP04_0018PE',     # 2010-2013
                        'DP04_0019PE',     # 2000-2009
                        'DP04_0020PE',     # 1990-1999
                        'DP04_0021PE',     # 1980-1989
                        'DP04_0022PE',     # 1970-1979
                        'DP04_0023PE',     # 1960-1969
                        'DP04_0024PE',     # 1950-1959
                        'DP04_0025PE',     # 1940-1949
                        'DP04_0026PE']     # 1939 or before
        
        new_label = ['Built_2014_or_later',
                     'Built_2010_to_2013',
                     'Built_2000_to_2009',
                     'Built_1990_to_1999',
                     'Built_1980_to_1989',
                     'Built_1970_to_1979',
                     'Built_1960_to_1969',
                     'Built_1950_to_1959',
                     'Built_1940_to_1949',
                     'Built_1939_or_before']
        
        for i in column_label:
            if element.get(i) is None:
                element[i] = 0
                
        new_dic = dict()
        new_dic['NAME'] = element.get('NAME')
        for i in range(len(new_label)):
            new_dic[new_label[i]] = element.get(column_label[i])
        #print(new_dic)
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
     google_cloud_options.job_name = 'built-year-df'
     google_cloud_options.staging_location = BUCKET + '/staging'
     google_cloud_options.temp_location = BUCKET + '/temp'
     options.view_as(StandardOptions).runner = 'DataflowRunner'

     # Create the Pipeline with the specified options.
     p = Pipeline(options=options)
    
     sql = 'SELECT NAME, DP04_0017PE, DP04_0018PE, DP04_0019PE, DP04_0020PE, DP04_0021PE, DP04_0022PE, \
     DP04_0023PE, DP04_0024PE, DP04_0025PE, DP04_0026PE FROM acs_2018_modeled.Built_Year limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     # write raw PCollection to log file
     query_results | 'Record original data' >> WriteToText(DIR_PATH + 'input.txt')

     # apply ParDo to format the column of the data
     formatted_pcoll = query_results | 'Format column' >> beam.ParDo(FormatColumnFn())

     # write PCollection to log file
     formatted_pcoll | 'Record the classified data' >> WriteToText(DIR_PATH + 'output.txt')

        
     dataset_id = 'acs_2018_modeled'
     table_id = 'Built_Year_DF'
     schema_id = 'NAME:STRING,Built_2014_or_later:FLOAT,Built_2010_to_2013:FLOAT,Built_2000_to_2009:FLOAT,Built_1990_to_1999:FLOAT,Built_1980_to_1989:FLOAT,Built_1970_to_1979:FLOAT,Built_1960_to_1969:FLOAT,Built_1950_to_1959:FLOAT,Built_1940_to_1949:FLOAT,Built_1939_or_before:FLOAT'

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