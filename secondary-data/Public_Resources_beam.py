import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


class Format(beam.DoFn):
    def process(self, element):
        new_dic = element
        ZIPCODE = element.get('ZIPCODE')
        new_dic['ZIPCODE'] = ZIPCODE.zfill(5)
        return [new_dic]


def run():
    PROJECT_ID = 'sashimi-266523'

    options = {
        'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT * FROM iml_2018_modeled.public_resources limit 50'

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

    dataset_id = 'iml_2018_modeled'
    table_id = 'Public_Resources_Beam'
    schema_id = 'MID:INTEGER,\
DISCIPL:STRING,\
COMMONNAME:STRING,\
LEGALNAME:STRING,\
ADSTREET:STRING,\
ADCITY:STRING,\
ADSTATE:STRING,\
ZIPCODE:STRING,\
PHONE:STRING,\
WEBURL:STRING,\
INCOME:FLOAT,\
REVENUE:FLOAT,\
LONGITUDE:FLOAT,\
LATITUDE:FLOAT'

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
