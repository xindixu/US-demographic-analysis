import logging
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText


column_name = [
    'DP03_0052PE',        # Income_less_10K: STRING
    'DP03_0053PE',        # Income_10K_14K: STRING
    'DP03_0054PE',        # Income_15K_24K: STRING
    'DP03_0055PE',        # Income_25K_34K: STRING
    'DP03_0056PE',        # Income_35K_49K: STRING
    'DP03_0057PE',        # Income_50K_74K: STRING
    'DP03_0058PE',        # Income_75K_99K: STRING
    'DP03_0059PE',        # Income_100K_149K: STRING
    'DP03_0060PE',        # Income_150K_199K: STRING
    'DP03_0061PE', 		  # Income_200K_more: STRING
]

new_label = ['Income_less_10K',
             'Income_10K_14K',
             'Income_15K_24K',
             'Income_25K_34K',
             'Income_35K_49K',
             'Income_50K_74K',
             'Income_75K_99K',
             'Income_150K_199K',
             'Income_200K_more',
            ]

class FormatIncomeFn(beam.DoFn):
    def process(self, element):
        name = element.get('NAME')
        for i in column_name:
            value = element.get(i)
            if value is None:
                element[i] = 0

        # create key, value pairs
        income_tuple = (name, element)
        return [income_tuple]


class ClassifyIncomeFn(beam.DoFn):
    def process(self, element):
        name, income_obj = element
        income = []

        for i in sorted(income_obj.keys()):
            if i != 'NAME':
                income.append(income_obj[i])

        # compute percentage of population in a certain social-eco class
        # cutoff comes from U.S.News

        lowest = income[0] + income[1] + income[2]
        lower_middle = income[3] + income[4]
        middle = income[5] + income[6]
        upper_middle = income[7] + income[8]
        rich = income[9]

        economic_classes_keys = [
            'Lowest', 'Lower_Middle', 'Middle', 'Upper_Middle', 'Rich']

        income_obj[economic_classes_keys[0]] = lowest
        income_obj[economic_classes_keys[1]] = lower_middle
        income_obj[economic_classes_keys[2]] = middle
        income_obj[economic_classes_keys[3]] = upper_middle
        income_obj[economic_classes_keys[4]] = rich

        economic_classes = [lowest, lower_middle, middle, upper_middle, rich]
        income_obj['Predominant_Class'] = economic_classes_keys[economic_classes.index(
            max(economic_classes))]

        # use median to compute the average income
        score = [5, (14+10) / 2,
                 (24+15) / 2, (34+25) / 2,
                 (49+35) / 2, (74+50) / 2,
                 (99+75) / 2, (149+100) / 2,
                 (199+150) / 2, 250
                 ]
        income_obj['Average'] = 0
        for i in range(len(score)):
            income_obj['Average'] += income[i] * score[i]
        income_obj['Average'] /= 250


        # create key, value pairs
        income_tuple = (name, income_obj)
        return [income_obj]


def run():
    PROJECT_ID = 'sashimi-266523'

    options = {
        'project': PROJECT_ID
    }
    opts = beam.pipeline.PipelineOptions(flags=[], **options)

    # Create beam pipeline using local runner
    p = beam.Pipeline('DirectRunner', options=opts)

    sql = 'SELECT * FROM acs_2018_modeled.Income where DP03_0052PE > 10 and DP03_0056PE > 10 and DP03_0059PE is not null limit 50'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write raw PCollection to log file
    query_results | 'Record oringal data' >> WriteToText(
        'input.txt')

    # apply ParDo to formate null values to 0
    formated_pcoll = query_results | 'Format income' >> beam.ParDo(
        FormatIncomeFn())

    formated_pcoll | 'Record formated data' >> WriteToText(
        'formated_pcoll_output.txt')

    # apply ParDo to classify income and social-econ status
    classified_pcoll = formated_pcoll | 'Classify income and social-econ status' >> beam.ParDo(
        ClassifyIncomeFn())

    # write PCollection to log file
    classified_pcoll | 'Record processed data' >> WriteToText(
        'classified_pcoll_output.txt')

    dataset_id = 'acs_2018_modeled'
    table_id = 'Income_Beam'
    schema_id = "NAME:STRING, \
     DP03_0052PE:FLOAT,    \
     DP03_0053PE:FLOAT, \
     DP03_0054PE:FLOAT, \
     DP03_0055PE:FLOAT, \
     DP03_0056PE:FLOAT, \
     DP03_0057PE:FLOAT, \
     DP03_0058PE:FLOAT, \
     DP03_0059PE:FLOAT,   \
     DP03_0060PE:FLOAT,   \
     DP03_0061PE:FLOAT,   \
     Lowest:FLOAT,  \
     Lower_Middle:FLOAT,    \
     Middle:FLOAT,  \
     Upper_Middle:FLOAT,    \
     Rich:FLOAT,    \
     Predominant_Class:STRING, \
     Average:FLOAT"    \

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
