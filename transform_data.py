import os
import pyspark
from pyspark.sql.functions import *
from functools import reduce
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


def main():
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)

    # read json files into spark dataframes
    json_dfs = read_json("./github_data", spark)

    # write transformed data to parquet file
    write_to_parquet(json_dfs)


def read_json(path, spark: SparkSession):
    json_dfs = []
    for file in os.listdir(path):
        json_dfs.append(spark.read.json(path + '//' + file, multiLine="true"))
    return json_dfs


# transform github data into required schema
def transform_dataframe(df):
    new_df = df.select(split(col("base.repo.full_name"), '/').getItem(0).alias("Organization Name"),
                       col("base.repo.id").alias("repository_id"), col("base.repo.name").alias("repository_name"),
                       col("base.repo.owner.login").alias("repository_owner"),
                       col("closed_at").alias("merged_at")).filter(df.closed_at.isNotNull())
    num_prs = df.select(df.base).count()
    new_df = new_df.withColumn("num_prs", lit(num_prs))
    num_prs_merged = df.filter(df.state.isin(['closed'])).count()
    new_df = new_df.withColumn("num_prs_merged", lit(num_prs_merged))
    new_df = new_df.withColumn("is_compliant", (new_df.num_prs == new_df.num_prs_merged) &
                               (new_df.repository_owner.contains('Scytale')))
    max_date_df = df.filter(col("closed_at").isNotNull()) \
        .agg(max(col("closed_at")).alias("max_date"))
    new_df = new_df.join(max_date_df, new_df.merged_at == max_date_df.max_date, how='inner')
    return new_df


def write_to_parquet(json_dfs):
    dataframes = []
    for df in json_dfs:
        transformed_df = transform_dataframe(df)
        dataframes.append(transformed_df)
    final_sdf = reduce(pyspark.sql.dataframe.DataFrame.unionByName, dataframes)
    final_sdf.show()
    final_sdf.write.parquet("./github_info.parquet")


if __name__ == "__main__":
    main()
