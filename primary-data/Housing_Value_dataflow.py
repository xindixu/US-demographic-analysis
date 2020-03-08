import datetime
import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions


class FormatHousingval_ColumnFn(beam.DoFn):
    def process(self, element):
        
        column_label = ['DP04_0081PE',     # < 50k
                        'DP04_0082PE',     # 50k to 99k
                        'DP04_0083PE',     # 100k to 149k
                        'DP04_0084PE',     # 150k to 199k
                        'DP04_0085PE',     # 200k to 399k
                        'DP04_0086PE',     # 400k to 499k
                        'DP04_0087PE',     # 500k to 999k
                        'DP04_0088PE']     # > 1M

        new_label = ['Less_than_V50k',
                     'V50k_to_99k',
                     'V100k_to_149k',
                     'V150k_to_199k',
                     'V200k_to_399k',
                     'V400k_to_499k',
                     'V500k_to_999k',
                     'V1M_and_more']
        
        for i in column_label:
            if element.get(i) is None:
                element[i] = 0
                
        new_dic = dict()
        new_dic['NAME'] = element.get('NAME')
        for i in range(len(new_label)):
            new_dic[new_label[i]] = element.get(column_label[i])
        #print(new_dic)
        return [new_dic]
    
    
class ClassifyHousingvalFn(beam.DoFn):
    def process(self, element):
        
        low = element.get(new_label[0]) + element.get(new_label[1]) + element.get(new_label[2]) + element.get(new_label[3])
        middle = element.get(new_label[4]) + element.get(new_label[5])
        high = element.get(new_label[6]) + element.get(new_label[7])
        
        new_keys = ['Low_Housing_Value','Middle_Housing_Value', 'High_Housing_Value']
        
        new_dic = dict()
        
        new_dic['NAME'] = element.get('NAME')
        for i in range(len(new_label)):
            new_dic[new_label[i]] = element.get(new_label[i])
        
        new_dic[new_keys[0]] = low
        new_dic[new_keys[1]] = middle
        new_dic[new_keys[2]] = high
        
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
     google_cloud_options.job_name = 'housing-value-df'
     google_cloud_options.staging_location = BUCKET + '/staging'
     google_cloud_options.temp_location = BUCKET + '/temp'
     options.view_as(StandardOptions).runner = 'DataflowRunner'

     # Create the Pipeline with the specified options.
     p = Pipeline(options=options)
    
     sql = 'SELECT NAME, DP04_0081PE, DP04_0082PE, DP04_0083PE, DP04_0084PE, DP04_0085PE, DP04_0086PE, \
     DP04_0087PE, DP04_0088PE FROM acs_2018_modeled.Housing_Value limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     # write raw PCollection to log file
     query_results | 'Record original data' >> WriteToText(DIR_PATH+'input.txt')

     # apply ParDo to format the column of the data
     formatted_pcoll = query_results | 'Format column' >> beam.ParDo(FormatHousingval_ColumnFn())
    
     # apply Pardo to classify the data
     classified_pcoll = formatted_pcoll | 'Classify the data' >> beam.ParDo(ClassifyHousingvalFn())

     # write PCollection to log file
     classified_pcoll | 'Record the classified data' >> WriteToText(DIR_PATH+'output.txt')

        
     dataset_id = 'acs_2018_modeled'
     table_id = 'Housing_Value_DF'
     schema_id = 'NAME:STRING,Less_than_V50k:FLOAT,V50k_to_99k:FLOAT,V100k_to_149k:FLOAT,V150k_to_199k:FLOAT,V200k_to_399k:FLOAT,V400k_to_499k:FLOAT,V500k_to_999k:FLOAT,V1M_and_more:FLOAT,Low_Housing_Value:FLOAT,Middle_Housing_Value:FLOAT,High_Housing_Value:FLOAT'

     # write PCollection to new BQ table
     classified_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id, 
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