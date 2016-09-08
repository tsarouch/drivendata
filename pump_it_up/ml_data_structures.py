def get_supervised_learning_df(df, label_variable, feature_variables,
                               label_name=None,
                               features_name=None):
        """ Return the Supervised Learning dataset as spark DataFrame,
        The Dataset - DataFrame is consisting of both features and labels.
        :df: The input DataFrame holding the data
        :label_variable: The name string of the column used for labeling (independent variable)
        :feature_variables: List of the names of our features
        :label_name: the name of the label column
        :features_name: the name of the features
        """
        from pyspark.ml.linalg import Vectors
        from pyspark.sql import Row

        df = df\
            .rdd\
            .map(lambda x: Row(
                label=x[label_variable],
                features=Vectors.dense([x[feature_name] for \
                                        feature_name in feature_variables])) )\
            .toDF()

        if label_name:
            df = df.withColumnRenamed('label', label_name)

        if features_name:
            df = df.withColumnRenamed('features', features_name)

        return df