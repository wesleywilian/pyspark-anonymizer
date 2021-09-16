# pyspark-anonymizer
Python library which makes it possible to dynamically mask data using JSON string or map rules in a PySpark environment.

## Installing

```shell
pip install pyspark-anonymizer
```

## Usage


### Before Masking


```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as spark_functions

spark = SparkSession.builder.appName("your_app_name").getOrCreate()
df = spark.read.parquet("s3://amazon-reviews-pds/parquet/product_category=Electronics/")
df.limit(5).toPandas()
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>marketplace</th>
      <th>customer_id</th>
      <th>review_id</th>
      <th>product_id</th>
      <th>product_parent</th>
      <th>product_title</th>
      <th>star_rating</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>vine</th>
      <th>verified_purchase</th>
      <th>review_headline</th>
      <th>review_body</th>
      <th>review_date</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>US</td>
      <td>51163966</td>
      <td>R2RX7KLOQQ5VBG</td>
      <td>B00000JBAT</td>
      <td>738692522</td>
      <td>Diamond Rio Digital Player</td>
      <td>3</td>
      <td>0</td>
      <td>0</td>
      <td>N</td>
      <td>N</td>
      <td>Why just 30 minutes?</td>
      <td>RIO is really great, but Diamond should increa...</td>
      <td>1999-06-22</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>1</th>
      <td>US</td>
      <td>30050581</td>
      <td>RPHMRNCGZF2HN</td>
      <td>B001BRPLZU</td>
      <td>197287809</td>
      <td>NG 283220 AC Adapter Power Supply for HP Pavil...</td>
      <td>5</td>
      <td>0</td>
      <td>0</td>
      <td>N</td>
      <td>Y</td>
      <td>Five Stars</td>
      <td>Great quality for the price!!!!</td>
      <td>2014-11-17</td>
      <td>2014</td>
    </tr>
    <tr>
      <th>2</th>
      <td>US</td>
      <td>52246039</td>
      <td>R3PD79H9CTER8U</td>
      <td>B00000JBAT</td>
      <td>738692522</td>
      <td>Diamond Rio Digital Player</td>
      <td>5</td>
      <td>1</td>
      <td>2</td>
      <td>N</td>
      <td>N</td>
      <td>The digital audio &amp;quot;killer app&amp;quot;</td>
      <td>One of several first-generation portable MP3 p...</td>
      <td>1999-06-30</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>3</th>
      <td>US</td>
      <td>16186332</td>
      <td>R3U6UVNH7HGDMS</td>
      <td>B009CY43DK</td>
      <td>856142222</td>
      <td>HDE Mini Portable Capsule Travel Mobile Pocket...</td>
      <td>5</td>
      <td>0</td>
      <td>0</td>
      <td>N</td>
      <td>Y</td>
      <td>Five Stars</td>
      <td>I like it, got some for the Grandchilren</td>
      <td>2014-11-17</td>
      <td>2014</td>
    </tr>
    <tr>
      <th>4</th>
      <td>US</td>
      <td>53068431</td>
      <td>R3SP31LN235GV3</td>
      <td>B00000JBSN</td>
      <td>670078724</td>
      <td>JVC FS-7000 Executive MicroSystem (Discontinue...</td>
      <td>3</td>
      <td>5</td>
      <td>5</td>
      <td>N</td>
      <td>N</td>
      <td>Design flaws ruined the better functions</td>
      <td>I returned mine for a couple of reasons:  The ...</td>
      <td>1999-07-13</td>
      <td>1999</td>
    </tr>
  </tbody>
</table>
</div>



### After Masking

In this example we will add the following data anonymizers:

- drop_column on column "marketplace"
- replace all values to "*" of the "customer_id" column
- replace_with_regex "R\d" (R and any digit) to "*" on "review_id" column
- sha256 on "product_id" column
- filter_row with condition "product_parent != 738692522"

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as spark_functions
import pyspark_anonymizer

spark = SparkSession.builder.appName("your_app_name").getOrCreate()
df = spark.read.parquet("s3://amazon-reviews-pds/parquet/product_category=Electronics/")

all_anons = [
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

df_parsed = pyspark_anonymizer.Parser(df, all_anons, spark_functions).parse()
df_parsed.limit(5).toPandas()
```

<div>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>customer_id</th>
      <th>review_id</th>
      <th>product_id</th>
      <th>product_parent</th>
      <th>product_title</th>
      <th>star_rating</th>
      <th>helpful_votes</th>
      <th>total_votes</th>
      <th>vine</th>
      <th>verified_purchase</th>
      <th>review_headline</th>
      <th>review_body</th>
      <th>review_date</th>
      <th>year</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>*</td>
      <td>RPHMRNCGZF2HN</td>
      <td>69031b13080f90ae3bbbb505f5f80716cd11c4eadd8d86...</td>
      <td>197287809</td>
      <td>NG 283220 AC Adapter Power Supply for HP Pavil...</td>
      <td>5</td>
      <td>0</td>
      <td>0</td>
      <td>N</td>
      <td>Y</td>
      <td>Five Stars</td>
      <td>Great quality for the price!!!!</td>
      <td>2014-11-17</td>
      <td>2014</td>
    </tr>
    <tr>
      <th>1</th>
      <td>*</td>
      <td>*U6UVNH7HGDMS</td>
      <td>c99947c06f65c1398b39d092b50903986854c21fd1aeab...</td>
      <td>856142222</td>
      <td>HDE Mini Portable Capsule Travel Mobile Pocket...</td>
      <td>5</td>
      <td>0</td>
      <td>0</td>
      <td>N</td>
      <td>Y</td>
      <td>Five Stars</td>
      <td>I like it, got some for the Grandchilren</td>
      <td>2014-11-17</td>
      <td>2014</td>
    </tr>
    <tr>
      <th>2</th>
      <td>*</td>
      <td>*SP31LN235GV3</td>
      <td>eb6b489524a2fb1d2de5d2e869d600ee2663e952a4b252...</td>
      <td>670078724</td>
      <td>JVC FS-7000 Executive MicroSystem (Discontinue...</td>
      <td>3</td>
      <td>5</td>
      <td>5</td>
      <td>N</td>
      <td>N</td>
      <td>Design flaws ruined the better functions</td>
      <td>I returned mine for a couple of reasons:  The ...</td>
      <td>1999-07-13</td>
      <td>1999</td>
    </tr>
    <tr>
      <th>3</th>
      <td>*</td>
      <td>*IYAZPPTRJF7E</td>
      <td>2a243d31915e78f260db520d9dcb9b16725191f55c54df...</td>
      <td>503838146</td>
      <td>BlueRigger High Speed HDMI Cable with Ethernet...</td>
      <td>3</td>
      <td>0</td>
      <td>0</td>
      <td>N</td>
      <td>Y</td>
      <td>Never got around to returning the 1 out of 2 ...</td>
      <td>Never got around to returning the 1 out of 2 t...</td>
      <td>2014-11-17</td>
      <td>2014</td>
    </tr>
    <tr>
      <th>4</th>
      <td>*</td>
      <td>*RDD9FILG1LSN</td>
      <td>c1f5e54677bf48936fb1e9838869630e934d16ac653b15...</td>
      <td>587294791</td>
      <td>Brookstone 2.4GHz Wireless TV Headphones</td>
      <td>5</td>
      <td>3</td>
      <td>3</td>
      <td>N</td>
      <td>Y</td>
      <td>Saved my. marriage, I swear to god.</td>
      <td>Saved my.marriage, I swear to god.</td>
      <td>2014-11-17</td>
      <td>2014</td>
    </tr>
  </tbody>
</table>
</div>

## Currently supported data masking/anonymization methods
- Methods
  - drop_column - Drop a column.
  - replace - Replace all column to a string.
  - replace_with_regex - Replace column contents with regex.
  - sha256 - Apply sha256 hashing function.
  - filter_row - Apply a filter to the dataframe.
