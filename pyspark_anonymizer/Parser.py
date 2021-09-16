import json


class PysparkAnon:
    functions_map = None
    user_defined_anon = None
    spark_instance = None
    spark_functions = None

    def __init__(self, spark_instance, user_defined_anon, spark_functions):
        self.spark_instance = spark_instance
        self.spark_functions = spark_functions
        self.user_defined_anon = json.loads(user_defined_anon) if type(
            user_defined_anon) is str else user_defined_anon
        self.functions_map = {"drop_column": self.anon_drop_column,
                              "replace": self.anon_replace,
                              "replace_with_regex": self.anon_replace_with_regex,
                              "sha256": self.anon_sha256,
                              "filter_row": self.anon_filter_row}

    def anon_drop_column(self, column_name):
        self.spark_instance = self.spark_instance.drop(column_name)

    def anon_replace(self, column_name, replace_to):
        self.spark_instance = self.spark_instance.withColumn(
            column_name, self.spark_functions.lit(replace_to))

    def anon_replace_with_regex(self, column_name, replace_from_regex, replace_to):
        self.spark_instance = self.spark_instance.withColumn(
            column_name, self.spark_functions.regexp_replace(column_name, replace_from_regex, replace_to))

    def anon_sha256(self, column_name):
        self.spark_instance = self.spark_instance.withColumn(
            column_name, self.spark_functions.sha2(self.spark_instance[column_name], 256))

    def anon_filter_row(self, where):
        self.spark_instance = self.spark_instance.where(where)

    def parse(self):
        # add all anons into spark instance
        for current_check in self.user_defined_anon:
            current_name = current_check['method']
            current_parameters = current_check['parameters']
            self.functions_map[current_name](**current_parameters)
        return self.spark_instance
