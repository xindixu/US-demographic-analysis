import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


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
        
        name = element.get('NAME')
        for i in column_name:
            value = element.get(i)
            if value is None:
                element[i] = 0
        new_dic = dict()
        new_dic['NAME'] = name
        for i in range(len(column_name)):
            new_dic[new_label[i]] = element.get(column_name[i])
        
        return [new_dic]



def run():
    PROJECT_ID = 'sashimi-266523'

    options = {
        'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT NAME, DP03_0019PE, DP03_0020PE, DP03_0021PE, DP03_0022PE, DP03_0023PE, DP03_0024PE, DP03_0025E FROM acs_2018_modeled.Commute limit 50'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write original PCollection to input file
    query_results | 'Record original data' >> WriteToText('input.txt')

    # apply ParDo to format null values to 0 and pass to the next Pardo
    formatted_pcoll = query_results | 'Format income' >> beam.ParDo(FormatFn())

    # write formatted PCollection to output file
    formatted_pcoll | 'Record processed data' >> WriteToText('output.txt')

    dataset_id = 'acs_2018_modeled'
    table_id = 'Commute_Beam'
    schema_id = 'NAME:STRING,Drive_Alone:FLOAT,Carpooled:FLOAT,Public_Transportation:FLOAT,Walking:FLOAT,Others:FLOAT,Work_From_Home:FLOAT,Mean_Travel_Time_To_Work:FLOAT'

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
