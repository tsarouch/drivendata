def change_dataframe_column_type(df, field, new_type):
        """Chage the type of a DataFrame column to another one
        :df: the original DataFrame
        :field:
        :type: the wanted type (double / int / string)
        """
        from pyspark.sql.types import DoubleType, IntegerType, StringType
        return df.withColumn(field, df[field].cast(new_type))
