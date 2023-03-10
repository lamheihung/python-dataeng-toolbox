from typing import Union, Optional
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from pyspark.sql.types import *

from .spark_etl_common import SparkEtlCommon


class DataLakeReader(SparkEtlCommon):
    """DataLakeReader is the reader to get the data path and read the table.

    Args:
        SparkEtlCommon (SparkEtlCommon): The foundamental of Spark application.
    """
    def __init__(self,
                 config: Union[dict, str],
                 spark: Optional[SparkSession]=None,
                 app_name: str='Testing-Spark-App'):
        """Constructs all the necessary attributes for the DataLakeReader.

        Args:
            config (Union[dict, str]): The dictionary or the json path of the configuration.
            spark (Optional[SparkSession], optional): The configured SparkSession. Defaults to None.
            app_name (str, optional): The Spark application name. Defaults to 'Testing-Spark-App'.
        """
        super().__init__(spark=spark, config=config, app_name=app_name)
        self.uri = self.spark.sparkContext._gateway.jvm.java.net.URI
        self.path = self.spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.Path
        self.file_system = self.spark.sparkContext._gateway.jvm.org.apache.hadoop.fs.FileSystem
        self.configuration = self.spark.sparkContext._gateway.jvm.org.apache.hadoop.conf.Configuration
        self.fs = self.file_system.get(self.uri(self.get_datalake_basepath()), self.configuration())
    # ------------------------------
    # path related method
    def get_datalake_basepath(self) -> str:
        """Get the basepath of the data lake

        Returns:
            str: The basepath of the data lake
        """
        path = self.config['bucket_path']
        return path

    def get_table_path(self,
                       table_type: str,
                       table_category: str,
                       table_name: str,
                       data_category: str='data') -> str:
        """Get the path of the table.

        Args:
            table_type (str): The table type. In this repo delta lake will be used, which is {'source', 'bronze', 'silver', 'gold'}.
            table_category (str): The schema of the tables.
            table_name (str): The corresponding table.
            data_category (str, optional): The data category, which could be either 'data' or 'cp'(checkpoint). Defaults to 'data'.

        Returns:
            str: The path of the table.
        """
        path = f"{self.get_datalake_basepath()}/{table_type}/{data_category}/{table_category}/{table_name}"
        return path
    # ------------------------------
    # parquet
    def get_single_parquet_path(self, 
                                basepath: str, 
                                input_date: str) -> str:
        """Get the path of the parquet file.

        Args:
            basepath (str): The path of the table.
            input_date (str): The reading date of the parquet file.

        Returns:
            str: The path of the parquet file.
        """
        path = f"{basepath}/{input_date.strftime('%Y-%m-%d')}"
        return path

    def get_all_parquet_path(self, 
                             basepath: str) -> list[str]:
        """Get all the path of the parquet files.

        Args:
            basepath (str): The path of the table.

        Returns:
            list[str]: The list of path of the parquet files.
        """
        all_path = [str(file_status.getPath()) for file_status in self.fs.listStatus(self.path(basepath))]
        return all_path
    
    def get_all_parquet_date(self, 
                             basepath: str) -> list[datetime]:
        """Get all the corresponding date of the parquet files.

        Args:
            basepath (str): The path of the table.

        Returns:
            list[datetime]: The list of datetime of the parquet files.
        """
        date_lst = sorted(
            [
                datetime(
                    year=int(path.split('/')[-1].split('.')[0].split('-')[0]),
                    month=int(path.split('/')[-1].split('.')[0].split('-')[1]),
                    day=int(path.split('/')[-1].split('.')[0].split('-')[2]),
                    tzinfo=timezone(offset=timedelta(hours=8))
                )
                for path in self.get_all_parquet_path(basepath)
            ]
        )
        return date_lst
    # ------------------------------
    # delta
    def get_all_delta_date(self, 
                           path: str) -> list[datetime]:
        """Transform all the 'ts' to corresponding date if the table is in delta format.

        Args:
            path (str): The path of the table.

        Returns:
            list[datetime]: The list of datetime
        """
        dataframe = self.spark.read.format("delta")\
            .option("readChangeFeed", "true")\
            .option("startingVersion", 0)\
            .load(path)
        dataframe = dataframe\
            .select(
                 func.from_unixtime(func.col('ts')/1000).cast(TimestampType()).alias('timestamp')
            ).withColumn(
                'hour', func.hour(func.col('timestamp'))
            ).withColumn(
                'minute', func.minute(func.col('timestamp'))
            ).withColumn(
                'second', func.second(func.col('timestamp'))
            ).distinct()
        dataframe = dataframe.withColumn(
            'new_timestamp',
            func.when(
                (func.col('hour')==0)&(func.col('minute')==0)&(func.col('second')==0), func.col('timestamp')
            ).otherwise(
                func.col('timestamp')+func.expr('interval 1 days')
            )
        )
        dataframe = dataframe.withColumn(
            'new_date',
            func.to_date(func.col('new_timestamp'))
        )
        date_lst = sorted(
            [
                datetime.combine(row['new_date'], datetime.min.time())
                for row in dataframe.select('new_date').orderBy('new_date').collect()
            ]
        )
        return date_lst
    # ------------------------------
    # read related method
    def read_parquet(self, 
                     path: str) -> DataFrame:
        """Read the parquet file.

        Args:
            path (str): The path of the parquet file.

        Returns:
            DataFrame: The Spark dataframe.
        """
        read_in_parquet = self.spark.read.format('parquet').load(path)
        return read_in_parquet

    def read_delta(self, 
                   path: str, 
                   cdf: bool=True) -> DataFrame:
        """Read the table in delta format.

        Args:
            path (str): The path of the delta table.
            cdf (bool, optional): Read the table in cdc(True) or latest(False) format. Defaults to True.

        Returns:
            DataFrame: The Spark dataframe.
        """
        read_in_delta = self.spark.read.format('delta')\
            .option("readChangeFeed", str(cdf).lower())\
            .option("startingVersion", 0)\
            .load(path)
        return read_in_delta