import pandas as pd

from config import train_values_file, test_values_file, train_labels_file


CSV_HEADER = "id,amount_tsh,date_recorded,funder,gps_height,installer,longitude,latitude,wpt_name,num_private,basin,subvillage,region,region_code,district_code,lga,ward,population,public_meeting,recorded_by,scheme_management,scheme_name,permit,construction_year,extraction_type,extraction_type_group,extraction_type_class,management,management_group,payment,payment_type,water_quality,quality_group,quantity,quantity_group,source,source_type,source_class,waterpoint_type,waterpoint_type_group"

def get_train_values_df():
    """ Return a Pandas DataFrame of the train values
    """
    return pd.read_csv(train_values_file)


def get_train_labels_df():
    """ Return a Pandas DataFrame of the train labels
    """
    return pd.read_csv(train_labels_file)


def get_train_data_df():
    """Merge the train values and labels and return a train data DataFrame
    """
    df_train = get_train_values_df()
    df_train_labels = get_train_labels_df()
    return pd.merge(df_train, df_train_labels, on='id')

def get_test_data_df():
    """ Return a Pandas DataFrame of the test values
    """
    return pd.read_csv(test_values_file)


def get_train_data_spark_df(sqlContext):
    """Merge the train values and labels and return a train Spark DataFrame
    :sqlContext: Spark sql Context
    """

    from pyspark.sql import Row
    df_train_values = sqlContext.read\
                                .format("com.databricks.spark.csv")\
                                .option("header", "true")\
                                .option("inferSchema", "false")\
                                .load(train_values_file)

    df_train_labels = sqlContext.read\
                                .format("com.databricks.spark.csv")\
                                .option("header", "true")\
                                .option("inferSchema", "false")\
                                .load(train_labels_file)
    return df_train_values.join(df_train_labels, "id")


def get_test_data_spark_df(sqlContext):
    """Return the test data in Spark DataFrame
    :sqlContext: Spark sql Context
    """
    from pyspark.sql import Row
    return sqlContext.read\
                     .format("com.databricks.spark.csv")\
                     .option("header", "true")\
                     .option("inferSchema", "false")\
                     .load(test_values_file)

#    # keep the header
#    test_values_f_header = sc\
#        .textFile(test_values_file)\
#        .filter(lambda l: CSV_HEADER in l)
#
#    # get the DF
#    return sc.textFile(test_values_file)\
#             .subtract(test_values_f_header)\
#             .map(lambda line: line.split(","))\
#             .filter(lambda line: len(line)>1)\
#             .map(lambda x: Row(
#                 id=int(x[0]),
#                 gps_height=x[4],
#                 longitude=x[6],
#                 latitude=x[7],
#             )
#             )\
#             .toDF()
