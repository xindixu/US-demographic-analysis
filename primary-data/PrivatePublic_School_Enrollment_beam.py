import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

column_label = ['S1401_C03_008E',     # Undergraduate Public 
                'S1401_C05_008E',     # Undergraduate Private
                'S1401_C03_009E',     # Graduate Public
                'S1401_C05_009E']     # Graduate Private

new_label = ['College_Undergrad_Public',
             'College_Undergrad_Private',
             'Grad_HigherEdu_Public',
             'Grad_HigherEdu_Private']


class FormatColumnFn(beam.DoFn):
    def process(self, element):
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
     PROJECT_ID = 'sashimi-266523' 

     # Project ID is required when using the BQ source
     options = {
     'project': PROJECT_ID
     }
     opts = beam.pipeline.PipelineOptions(flags=[], **options)

     # Create beam pipeline using local runner
     p = beam.Pipeline('DirectRunner', options=opts)
    
     sql = 'SELECT NAME, S1401_C03_008E, S1401_C05_008E, S1401_C03_009E, S1401_C05_009E FROM acs_2018_modeled.PrivatePublic_School_Enrollment limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     # write raw PCollection to log file
     query_results | 'Record original data' >> WriteToText('input.txt')

     # apply ParDo to format the column of the data
     formatted_pcoll = query_results | 'Format column' >> beam.ParDo(FormatColumnFn())

     # write PCollection to log file
     formatted_pcoll | 'Record the classified data' >> WriteToText('output.txt')

        
     dataset_id = 'acs_2018_modeled'
     table_id = 'PrivatePublic_School_Enrollment_Beam'
     schema_id = 'NAME:STRING,College_Undergrad_Public:INTEGER,College_Undergrad_Private:INTEGER,Grad_HigherEdu_Public:INTEGER,Grad_HigherEdu_Private:INTEGER'

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