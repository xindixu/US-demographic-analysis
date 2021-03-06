import datetime
import logging
import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.pipeline import PipelineOptions
from apache_beam.pipeline import Pipeline
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions



class FormatIncomeFn(beam.DoFn):
    def process(self, element):
        
        column_name = [
            'DP03_0052PE',         # Income_less_10K: STRING
            'DP03_0053PE',         # Income_10K_14K: STRING
            'DP03_0054PE',         # Income_15K_24K: STRING
            'DP03_0055PE',         # Income_25K_34K: STRING
            'DP03_0056PE',         # Income_35K_49K: STRING
            'DP03_0057PE',         # Income_50K_74K: STRING
            'DP03_0058PE',         # Income_75K_99K: STRING
            'DP03_0059PE',         # Income_100K_149K: STRING
            'DP03_0060PE',         # Income_150K_199K: STRING
            'DP03_0061PE']        # Income_200K_more: STRING


        new_label = [
            'Income_Less_10k',
            'Income_10k_14k',
            'Income_15k_24k',
            'Income_25k_34k',
            'Income_35k_49k',
            'Income_50k_74k',
            'Income_75k_99k',
            'Income_100k_149k',
            'Income_150k_199k',
            'Income_200k_More']
    
        for i in column_name:
            value = element.get(i)
            if value is None:
                element[i] = 0
        new_dic = dict()
        name = element.get('NAME')
        num = name[6:]
        new_dic['ZCTA5'] = num
        for i in range(len(column_name)):
            new_dic[new_label[i]] = element.get(column_name[i])
        
        return [new_dic]


class ClassifyIncomeFn(beam.DoFn):
    def process(self, element):
        new_label = [
            'Income_Less_10k',
            'Income_10k_14k',
            'Income_15k_24k',
            'Income_25k_34k',
            'Income_35k_49k',
            'Income_50k_74k',
            'Income_75k_99k',
            'Income_100k_149k',
            'Income_150k_199k',
            'Income_200k_More']
        
        # compute percentage of population in a certain social-eco class
        # cutoff comes from U.S.News

        lowest = element.get(new_label[0]) + element.get(new_label[1]) + element.get(new_label[2])
        lower_middle = element.get(new_label[3]) + element.get(new_label[4])
        middle = element.get(new_label[5]) + element.get(new_label[6])
        upper_middle = element.get(new_label[7]) + element.get(new_label[8])
        rich = element.get(new_label[9])

        economic_classes_keys = ['Lowest', 'Lower_Middle', 'Middle', 'Upper_Middle', 'Rich']
        
        new_dic = dict()
        new_dic['ZCTA5'] = element.get('ZCTA5')
        for i in range(len(new_label)):
            new_dic[new_label[i]] = element.get(new_label[i])
            
        new_dic[economic_classes_keys[0]] = lowest
        new_dic[economic_classes_keys[1]] = lower_middle
        new_dic[economic_classes_keys[2]] = middle
        new_dic[economic_classes_keys[3]] = upper_middle
        new_dic[economic_classes_keys[4]] = rich

        economic_classes = [lowest, lower_middle, middle, upper_middle, rich]
        new_dic['Predominant_Class'] = economic_classes_keys[economic_classes.index(
            max(economic_classes))]

        # use median to compute the average income
        score = [5, (14+10) / 2,
                 (24+15) / 2, (34+25) / 2,
                 (49+35) / 2, (74+50) / 2,
                 (99+75) / 2, (149+100) / 2,
                 (199+150) / 2, 250
                 ]
        new_dic['Average'] = 0
        for i in range(len(score)):
            new_dic['Average'] += element.get(new_label[i]) * score[i]
        new_dic['Average'] /= 250

        # return element
        return [new_dic]


def run():
    PROJECT_ID = 'sashimi-266523'  # change to your project id
    BUCKET = 'gs://sashimi-sushi'  # change to your bucket name
    DIR_PATH = BUCKET + '/output/' + \
        datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S') + '/'

    # Create and set your PipelineOptions.
    options = PipelineOptions(flags=None)

    # For Dataflow execution, set the project, job_name,
    # staging location, temp_location and specify DataflowRunner.
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = 'income-df'
    google_cloud_options.staging_location = BUCKET + '/staging'
    google_cloud_options.temp_location = BUCKET + '/temp'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # Create the Pipeline with the specified options.
    p = Pipeline(options=options)

    sql = 'SELECT NAME, DP03_0052PE, DP03_0053PE, DP03_0054PE, DP03_0055PE, DP03_0056PE, DP03_0057PE, DP03_0058PE, DP03_0059PE, DP03_0060PE, DP03_0061PE FROM acs_2018_modeled.Income where DP03_0052PE > 10 and DP03_0056PE > 10 and DP03_0059PE is not null'
    bq_source = beam.io.BigQuerySource(query=sql, use_standard_sql=True)

    query_results = p | 'Read from BigQuery' >> beam.io.Read(bq_source)

    # write original PCollection to input file
    query_results | 'Record original data' >> WriteToText(DIR_PATH + 'income_input.txt')

    # apply ParDo to format null values to 0 and pass to the next Pardo
    formated_pcoll = query_results | 'Format income' >> beam.ParDo(FormatIncomeFn())

    # apply ParDo to classify income and social-econ status
    classified_pcoll = formated_pcoll | 'Classify income and social-econ status' >> beam.ParDo(ClassifyIncomeFn())

    # write formatted PCollection to output file
    classified_pcoll | 'Record processed data' >> WriteToText(DIR_PATH + 'income_output.txt')

    dataset_id = 'acs_2018_modeled'
    table_id = 'Income_Beam_DF'
    schema_id = 'ZCTA5:STRING,Income_Less_10k:FLOAT,Income_10k_14k:FLOAT,Income_15k_24k:FLOAT,Income_25k_34k:FLOAT,Income_35k_49k:FLOAT,Income_50k_74k:FLOAT,Income_75k_99k:FLOAT,Income_100k_149k:FLOAT,Income_150k_199k:FLOAT,Income_200k_More:FLOAT,Lowest:FLOAT,Lower_Middle:FLOAT,Middle:FLOAT,Upper_Middle:FLOAT,Rich:FLOAT,Predominant_Class:STRING,Average:FLOAT'

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
