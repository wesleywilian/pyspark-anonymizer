from pprint import pprint
import boto3

dynamo_table_name = "pyspark_anonymizer"
dataframe_name = "table_x"


def put_anonymizer(data_anonimizers, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(dynamo_table_name)
    response = table.put_item(
        Item={"dataframe_name": dataframe_name, "anonymizer": data_anonimizers}
    )
    return response


if __name__ == '__main__':
    anonymizers = [
        {
            "method": "drop_column",
            "parameters": {
                "column_name": "marketplace"
            }
        },
        {
            "method": "replace",
            "parameters": {
                "column_name": "customer_id",
                "replace_to": "*"
            }
        },
        {
            "method": "replace_with_regex",
            "parameters": {
                "column_name": "review_id",
                "replace_from_regex": "R\d",
                "replace_to": "*"
            }
        },
        {
            "method": "sha256",
            "parameters": {
                "column_name": "product_id"
            }
        },
        {
            "method": "filter_row",
            "parameters": {
                "where": "product_parent != 738692522"
            }
        }
    ]
    result = put_anonymizer(anonymizers)
    print("Put succeeded:")
    pprint(result)
