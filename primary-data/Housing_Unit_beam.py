import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText

column_label = ['DP04_0007PE',     # Detached 1-Unit
                'DP04_0008PE',     # Attached 1-Unit
                'DP04_0009PE',     # 2 Units
                'DP04_0010PE',     # 3-4 Units
                'DP04_0011PE',     # 5-9 Units
                'DP04_0012PE',     # 10-19 Units
                'DP04_0013PE',     # 20 and more Units
                'DP04_0014PE',     # Mobile home
                'DP04_0015PE']     # RV & boat

new_label = ['Detached_Unit_1',
             'Attached_Unit_1',
             'Units_2',
             'Units_3_to_4',
             'Units_5_to_9',
             'Units_10_to_19',
             'Units_20_or_more',
             'Mobile_Home',
             'RV_Boat']


class FormatHousingU_ColumnFn(beam.DoFn):
    def process(self, element):
        for i in column_label:
            if element.get(i) is None:
                element[i] = 0

        new_dic = dict()
        new_dic['NAME'] = element.get('NAME')
        for i in range(len(new_label)):
            new_dic[new_label[i]] = element.get(column_label[i])
        # print(new_dic)
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

    sql = 'SELECT NAME, DP04_0007PE, DP04_0008PE, DP04_0009PE, DP04_0010PE, DP04_0011PE, DP04_0012PE, \
     DP04_0013PE, DP04_0014PE, DP04_0015PE FROM acs_2018_modeled.Housing_Unit limit 50'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write raw PCollection to log file
    query_results | 'Record original data' >> WriteToText('input.txt')

    # apply ParDo to format the column of the data
    formatted_pcoll = query_results | 'Format column' >> beam.ParDo(
        FormatHousingU_ColumnFn())

    # write PCollection to log file
    formatted_pcoll | 'Record the classified data' >> WriteToText('output.txt')

    dataset_id = 'acs_2018_modeled'
    table_id = 'Housing_Unit_Beam'
    schema_id = 'NAME:STRING,Detached_Unit_1:FLOAT,Attached_Unit_1:FLOAT,Units_2:FLOAT,Units_3_to_4:FLOAT,Units_5_to_9:FLOAT,Units_10_to_19:FLOAT,Units_20_or_more:FLOAT,Mobile_Home:FLOAT,RV_Boat:FLOAT'

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
