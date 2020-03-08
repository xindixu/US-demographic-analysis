import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

column_label = ['S1401_C01_004E',     # Kindergarden
                'S1401_C01_005E',     # Grade 1 to 4
                'S1401_C01_006E',     # Grade 5 to 8
                'S1401_C01_007E',     # Grade 9 to 12
                'S1401_C01_008E',     # College and Undergraduate
                'S1401_C01_009E']     # Graduate and Higher Education

new_label = ['Kindergarden',
             'Grade1_to_4',
             'Grade5_to_8',
             'Grade9_to_12',
             'College_Undergrad',
             'Grad_HigherEdu']


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
    
     sql = 'SELECT NAME, S1401_C01_004E, S1401_C01_005E, S1401_C01_006E, S1401_C01_007E, S1401_C01_008E, S1401_C01_009E FROM acs_2018_modeled.General_School_Enrollment limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     # write raw PCollection to log file
     query_results | 'Record original data' >> WriteToText('input.txt')

     # apply ParDo to format the column of the data
     formatted_pcoll = query_results | 'Format column' >> beam.ParDo(FormatColumnFn())

     # write PCollection to log file
     formatted_pcoll | 'Record the classified data' >> WriteToText('output.txt')

        
     dataset_id = 'acs_2018_modeled'
     table_id = 'General_School_Enrollment_Beam'
     schema_id = 'NAME:STRING,Kindergarden:INTEGER,Grade1_to_4:INTEGER,Grade5_to_8:INTEGER,Grade9_to_12:INTEGER,College_Undergrad:INTEGER,Grad_HigherEdu:INTEGER'

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