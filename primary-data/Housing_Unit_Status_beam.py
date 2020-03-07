import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

column_label = ['DP04_0001E',     # Total Housing 
                'DP04_0002PE',     # Occupied Housing
                'DP04_0003PE']     # Vacant Housing

new_label = ['Total_Housing_Units',
             'Occupied_Housing_Units',
             'Vacant_Housing_Units']


class FormatHousingU_ColumnFn(beam.DoFn):
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
    
     sql = 'SELECT NAME, DP04_0001E, DP04_0002PE, DP04_0003PE FROM acs_2018_modeled.Housing_Unit_Status order by NAME limit 50'
     bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

     query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
     
     # write raw PCollection to log file
     query_results | 'Record original data' >> WriteToText('input.txt')

     # apply ParDo to format the column of the data
     formatted_pcoll = query_results | 'Format column' >> beam.ParDo(FormatHousingU_ColumnFn())

     # write PCollection to log file
     formatted_pcoll | 'Record the classified data' >> WriteToText('output.txt')

        
     dataset_id = 'acs_2018_modeled'
     table_id = 'Housing_Unit_Status_Beam'
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