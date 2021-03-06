import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

class Format(beam.DoFn):
    def process(self, element):
        original_label = [
            'DP03_0002PE',
            'DP03_0004PE',
            'DP03_0005PE',
            'DP03_0014E',
        ]

        new_label = [
            'Labor_Force',
            'Civilian_Labor_Force_Employed',
            'Civilian_Labor_Force_Unemployed',
            'Children_In_HouseHold',
        ]

        name = element.get('NAME')
        for i in original_label:
            value = element.get(i)
            if value is None:
                element[i] = 0
        new_dic = dict()
        new_dic['NAME'] = name
        for i in range(len(original_label)):
            new_dic[new_label[i]] = element.get(original_label[i])

        return [new_dic]

def run():
    PROJECT_ID = 'sashimi-266523'

    options = {
        'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT * FROM acs_2018_modeled.Labor_Force_Stats limit 50'

    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write original PCollection to input file
    query_results | 'Record original data' >> WriteToText(
        'input.txt')

    # apply ParDo to format and rename column names and pass to the next Pardo
    formated_pcoll = query_results | 'Format' >> beam.ParDo(
        Format())

    # write formatted PCollection to output file
    formated_pcoll | 'Record processed data' >> WriteToText(
        'output.txt')

    dataset_id = 'acs_2018_modeled'
    table_id = 'Labor_Force_Stats_Beam'
    schema_id = 'NAME:STRING,\
Labor_Force:FLOAT,\
Civilian_Labor_Force_Employed:FLOAT,\
Civilian_Labor_Force_Unemployed:FLOAT,\
Children_In_HouseHold:INTEGER'

    # write PCollection to new BQ table

    formated_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id,
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
