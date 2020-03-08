import datetime
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions



class FormatHousingU_ColumnFn(beam.DoFn):
    def process(self, element):
        
        column_label = ['DP04_0001E',     # Total Housing 
                        'DP04_0002PE',     # Occupied Housing
                        'DP04_0003PE']     # Vacant Housing

        new_label = ['Total_Housing_Units',
                     'Occupied_Housing_Units',
                     'Vacant_Housing_Units']
        
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
     google_cloud_options.job_name = 'housing-unit-status-df'
     google_cloud_options.staging_location = BUCKET + '/staging'
     google_cloud_options.temp_location = BUCKET + '/temp'
     options.view_as(StandardOptions).runner = 'DataflowRunner'

     # Create the Pipeline with the specified options.
     p = Pipeline(options=options)
    
     sql = 'SELECT NAME, DP04_0001E, DP04_0002PE, DP04_0003PE FROM acs_2018_modeled.Housing_Unit_Status order by NAME limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     # write raw PCollection to log file
     query_results | 'Record original data' >> WriteToText(DIR_PATH+'input.txt')

     # apply ParDo to format the column of the data
     formatted_pcoll = query_results | 'Format column' >> beam.ParDo(FormatHousingU_ColumnFn())

     # write PCollection to log file
     formatted_pcoll | 'Record the classified data' >> WriteToText(DIR_PATH+'output.txt')

        
     dataset_id = 'acs_2018_modeled'
     table_id = 'Housing_Unit_Status_DF'
     schema_id = 'NAME:STRING,Total_Housing_Units:INTEGER,Occupied_Housing_Units:FLOAT,Vacant_Housing_Units:FLOAT'

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