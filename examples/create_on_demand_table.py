import boto3

dynamo_table_name = 'pyspark_anonymizer'


def create_table(dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

    table_resource = dynamodb.create_table(
        TableName=dynamo_table_name,
        KeySchema=[
            {
                'AttributeName': 'dataframe_name',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'dataframe_name',
                'AttributeType': 'S'
            }
        ],
        BillingMode='PAY_PER_REQUEST'
    )
    return table_resource


if __name__ == '__main__':
    table = create_table()
    print("Table status:", table.table_status)
