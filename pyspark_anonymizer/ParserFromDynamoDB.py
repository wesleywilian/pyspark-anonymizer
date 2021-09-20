from .Parser import Parser as Parser


class ParserFromDynamoDB:
    functions_map = None
    user_defined_anon = None
    spark_instance = None
    spark_functions = None
    client_error = None
    dataframe_name = None
    dynamo_table = None
    anonymizer = None

    def __init__(self, spark_instance, dataframe_name, dynamo_table, spark_functions, client_error):
        self.spark_instance = spark_instance
        self.dataframe_name = dataframe_name
        self.dynamo_table = dynamo_table
        self.spark_functions = spark_functions
        self.client_error = client_error
        self.__get_anonymizer()

    def __get_anonymizer(self):
        try:
            response = self.dynamo_table.get_item(Key={"dataframe_name": self.dataframe_name})
        except self.client_error as e:
            print(e.response['Error']['Message'])
        else:
            self.anonymizer = response['Item']['anonymizer']

    def parse(self):
        return Parser(self.spark_instance, self.anonymizer, self.spark_functions).parse()
