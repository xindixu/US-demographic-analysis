import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


original_label = [
    'DP03_0033E',
    'DP03_0034E',
    'DP03_0035E',
    'DP03_0036E',
    'DP03_0037E',
    'DP03_0038E',
    'DP03_0039E',
    'DP03_0040E',
    'DP03_0041E',
    'DP03_0042E',
    'DP03_0043E',
    'DP03_0045E'
]

new_label = [
    'Agriculture',
    'Construction',
    'Manufacturing',
    'Wholesale',
    'Retail',
    'Transportation',
    'Information',
    'Finance_Real_Estate',
    'Professional_Scientific_Management',
    'Educational_Health_Care',
    'Art_Entertainment',
    'Public_Administration'
]


class FormatIndustry(beam.DoFn):
    def process(self, element):
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


class FindPredominantIndustry(beam.DoFn):
    def process(self, element):

        new_dic = dict()
        for i in range(len(new_label)):
            new_dic[new_label[i]] = element.get(new_label[i])

        sorted_industry = sorted(new_dic.items(), key=lambda x: x[1])
        new_dic['NAME'] = element.get('NAME')
        new_dic['Predominant_Industry_First'] = sorted_industry[-1][0] if sorted_industry[-1][1] > 0 else None
        new_dic['Predominant_Industry_Second'] = sorted_industry[-2][0] if sorted_industry[-2][1] > 0 else None
        new_dic['Predominant_Industry_Third'] = sorted_industry[-3][0] if sorted_industry[-3][1] > 0 else None

        return [new_dic]


def run():
    PROJECT_ID = 'sashimi-266523'

    options = {
        'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT NAME, DP03_0033E, DP03_0034E, DP03_0035E, DP03_0036E, DP03_0037E, DP03_0038E, DP03_0039E, DP03_0040E, DP03_0041E, DP03_0042E, DP03_0043E, DP03_0045E FROM acs_2018_modeled.Labor_In_Industry where DP03_0033E > 10 and DP03_0034E > 10 limit 50'

    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write original PCollection to input file
    query_results | 'Record original data' >> WriteToText(
        'labor_in_industry_input.txt')

    # apply ParDo to format and rename column names and pass to the next Pardo
    formated_pcoll = query_results | 'Format industry' >> beam.ParDo(
        FormatIndustry())

    # apply ParDo to find the predominant industry in one area
    processed_pcoll = formated_pcoll | 'Find predominant industry' >> beam.ParDo(
        FindPredominantIndustry())

    # write formatted PCollection to output file
    processed_pcoll | 'Record processed data' >> WriteToText(
        'labor_in_industry_output.txt')

    dataset_id = 'acs_2018_modeled'
    table_id = 'Labor_In_Industry_Beam'
    schema_id = 'NAME:STRING,\
Agriculture:FLOAT,\
Construction:FLOAT,\
Manufacturing:FLOAT,\
Wholesale:FLOAT,\
Retail:FLOAT,\
Transportation:FLOAT,\
Information:FLOAT,\
Finance_Real_Estate:FLOAT,\
Professional_Scientific_Management:FLOAT,\
Educational_Health_Care:FLOAT,\
Art_Entertainment:FLOAT,\
Public_Administration:FLOAT,\
Predominant_Industry_First:STRING,\
Predominant_Industry_Second:STRING,\
Predominant_Industry_Third:STRING'

    # write PCollection to new BQ table

    processed_pcoll | 'Write BQ table' >> beam.io.WriteToBigQuery(dataset=dataset_id,
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
