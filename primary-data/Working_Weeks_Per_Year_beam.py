import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


class Format(beam.DoFn):
    def process(self, element):
        original_label = [
            'S2303_C01_002E',
            'S2303_C01_003E',
            'S2303_C01_004E',
            'S2303_C01_005E',
            'S2303_C01_006E',
            'S2303_C01_007E',
            'S2303_C01_008E'
        ]

        new_label = [
            'Work_50_to_52_weeks',
            'Work_48_to_49_weeks',
            'Work_40_to_47_weeks',
            'Work_27_to_39_weeks',
            'Work_14_to_26_weeks',
            'Work_1_to_13_weeks',
            'No_working'
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

    sql = 'SELECT * FROM acs_2018_modeled.Working_Weeks_Per_Year limit 50'

    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write original PCollection to input file
    query_results | 'Record original data' >> WriteToText(
        'input.txt')

    # apply ParDo to format and rename column names and pass to the next Pardo
    formatted_pcoll = query_results | 'Format' >> beam.ParDo(
        Format())

    # write formatted PCollection to output file
    formatted_pcoll | 'Record processed data' >> WriteToText(
        'output.txt')

    dataset_id = 'acs_2018_modeled'
    table_id = 'Working_Weeks_Per_Year_Beam'
    schema_id = 'NAME:STRING,\
Work_50_to_52_weeks:INTEGER,\
Work_48_to_49_weeks:INTEGER,\
Work_40_to_47_weeks:INTEGER,\
Work_27_to_39_weeks:INTEGER,\
Work_14_to_26_weeks:INTEGER,\
Work_1_to_13_weeks:INTEGER,\
No_working:INTEGER'
    

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
