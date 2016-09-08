
def get_string_indexed_df(df, field_names, index_appendix='index'):
    """Encode a string column of labels to a column of label indices.
    :df: The input DataFrame
    :field_names: The name of the columns that should be indexed
    :index_appendix: The appendix of the name of the indexed field
    """
    from pyspark.ml.feature import StringIndexer
    for field_name in field_names:
        stringIndexer = StringIndexer(
            inputCol=field_name,
            outputCol=field_name + '_' + index_appendix)
        df = stringIndexer.fit(df).transform(df)
    return df


def get_index_string_d(df, field_name, index_appendix='index'):
    """Return a dictionary of the index (key) and string name (value) of the
    corresponding field that has been indexed
    """
    return  \
        dict(df\
             .rdd\
             .map(lambda x : (x[field_name + \
                                '_' + \
                                index_appendix], x[field_name]))\
             .distinct()\
             .collect())
