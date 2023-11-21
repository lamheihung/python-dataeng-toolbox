from typing import Union, Optional, Any
from abc import abstractmethod
from delta.tables import DeltaTable
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql import functions as func
from pyspark.sql.window import Window
from pyspark.sql.types import *

from .data_lake_reader import DataLakeReader


class DeltaLakeTable(DataLakeReader):
    """DeltaLakeTable is the transformer including all the delta table operation.

    Args:
        DataLakeReader (DataLakeReader): The data lake reader to read the data in data lake.
    """
    table_base_columns = [
        ("ts", LongType(), False, None, None),
        ("log_status", StringType(), False, None, None),
        ("data_source", StringType(), False, None, None)
    ]

    def __init__(self,
                 config: Union[str, dict[str, Union[str, dict[str, str]]]],
                 spark: Optional[SparkSession]=None,
                 app_name: str='Testing-App',
                 source_table_category: str='',
                 source_table_name: str='',
                 source_table_type: str='',
                 sink_table_category: str='',
                 sink_table_name: str='',
                 sink_table_type: str='',
                 log_retention_duration_days: int=3650,
                 deleted_file_retention_duration_days: int=3650,
                 check_point_interval: int=7,
                 *args,
                 **kwargs):
        """Constructs all the necessary attributes for the DeltaLakeTable.

        Args:
            config (Union[str, dict[str, Union[str, dict[str, str]]]]): The dictionary or the json path of the configuration.
            spark (Optional[SparkSession], optional): The configured SparkSession. Defaults to None.
            app_name (str, optional): The Spark application name. Defaults to 'Testing-Spark-App'.
            source_table_category (str, optional): The source table category. Defaults to ''.
            source_table_name (str, optional): The source table name. Defaults to ''.
            source_table_type (str, optional): The source table type. Defaults to ''.
            sink_table_category (str, optional): The sink table category. Defaults to ''.
            sink_table_name (str, optional): The sink table name. Defaults to ''.
            sink_table_type (str, optional): The sink table type. Defaults to ''.
            log_retention_duration_days (int, optional): The delta log retention duration in days. Defaults to 3650.
            deleted_file_retention_duration_days (int, optional): The source file retention duration in days. Defaults to 3650.
            check_point_interval (int, optional): Number of delta log file to create a new checkpoint. Defaults to 7.
        """
        super().__init__(spark=spark, config=config, app_name=app_name)
        # source table config
        self.source_table_category = source_table_category
        self.source_table_name = source_table_name
        self.source_table_type = source_table_type
        # sink table config
        self.sink_table_category = sink_table_category
        self.sink_table_name = sink_table_name
        self.sink_table_type = sink_table_type
        # delta table config
        self.log_retention_duration_days = log_retention_duration_days
        self.deleted_file_retention_duration_days = deleted_file_retention_duration_days
        self.check_point_interval = check_point_interval
    # ------------------------------
    # property
    @property
    def source_table_category(self) -> str:
        return self._source_table_category

    @source_table_category.setter
    def source_table_category(self, 
                              value: str):
        # Source table collection name validation can be checked in this step.
        if value not in {''}:
            raise ValueError("Unknown source table category. Please check the source_table_category param value.")
        self._source_table_category = value

    @property
    def source_table_type(self) -> str:
        return self._source_table_type

    @source_table_type.setter
    def source_table_type(self, 
                          value: str):
        # From the delta architecture design, there are totally three
        # layer: bronze, silver and gold. A source layer is added as
        # a landing zone.
        if value not in {'', 'source', 'bronze', 'silver', 'gold'}:
            raise ValueError("Unknown source table type. Please check the source_table_type param value.")
        self._source_table_type = value

    @property
    def sink_table_category(self) -> str:
        return self._sink_table_category

    @sink_table_category.setter
    def sink_table_category(self, 
                            value: str):
        # Sink table collection name validation can be checked in this step.
        if value not in {''}:
            raise ValueError("Unknown sink table category. Please check the sink_table_category param value.")
        self._sink_table_category = value

    @property
    def sink_table_type(self) -> str:
        return self._sink_table_type

    @sink_table_type.setter
    def sink_table_type(self, 
                        value: str):
        # From the delta architecture design, there are totally three
        # layer: bronze, silver and gold. A source layer is added as
        # a landing zone.
        if value not in {'', 'bronze', 'silver', 'gold'}:
            raise ValueError("Unknown sink table type. Please check the sink_table_type param value.")
        self._sink_table_type = value

    @property
    def log_retention_duration_days(self) -> int:
        return self._log_retention_duration_days

    @log_retention_duration_days.setter
    def log_retention_duration_days(self, 
                                    value: int):
        try:
            self._log_retention_duration_days = int(value)
        except:
            raise ValueError("Unknow log retention duration days. This param must be a integer. Please check the log_retention_duration_days param value.")

    @property
    def deleted_file_retention_duration_days(self) -> int:
        return self._deleted_file_retention_duration_days

    @deleted_file_retention_duration_days.setter
    def deleted_file_retention_duration_days(self, 
                                             value: int):
        try:
            self._deleted_file_retention_duration_days = int(value)
        except:
            raise ValueError("Unknow deleted file retention duration days. This param must be a integer. Please check the deleted_file_retention_duration_days param value.")

    @property
    def check_point_interval(self) -> int:
        return self._check_point_interval

    @check_point_interval.setter
    def check_point_interval(self, 
                             value: int):
        try:
            self._check_point_interval = int(value)
        except:
            raise ValueError("Unknow check point interval. This param must be a integer. Please check the check_point_interval param value.")
    # ------------------------------
    # optimization related method
    def optimize_file_compation(self) -> None:
        """Perform the file compaction of the sink table data.

        Returns:
            None: Nothing will be retured.
        """
        DeltaTable.forPath(self.spark, self.get_sink_data_path())\
            .optimize()\
            .executeCompaction()
        return None
    # ------------------------------
    # source related method
    def get_source_data_path(self) -> str:
        """Get the file path of the source table.

        Returns:
            str: The table path of the source table.
        """
        return self.get_table_path(
            self.source_table_type,
            self.source_table_category,
            self.source_table_name
        )

    @abstractmethod
    def read_source_table(self, 
                          input_date: datetime, 
                          read_format: str='delta-log') -> DataFrame:
        """Abstract method to read the source table in different Spark Application.

        Args:
            input_date (datetime): The date image of the data that want to read.
            read_format (str, optional): Three format supported, parquet, delta-log or delta-latest-snap. Defaults to 'delta-log'.

        Returns:
            DataFrame: The Spark dataframe.
        """
        return NotImplemented
    # ------------------------------
    # sink related method
    def get_sink_data_path(self) -> str:
        """Get the file path of the sink table.

        Returns:
            str: The table path of the sink table.
        """
        return self.get_table_path(
            self.sink_table_type,
            self.sink_table_category,
            self.sink_table_name
        )
    
    @abstractmethod
    def read_sink_table(self, 
                        input_date: datetime, 
                        read_format: str='delta-log') -> DataFrame:
        """Abstract method to read the sink table in different Spark Application.

        Args:
            input_date (datetime): The date image of the data that want to read.
            read_format (str, optional): Three format supported, parquet, delta-log or delta-latest-snap. Defaults to 'delta-log'.

        Returns:
            DataFrame: The Spark dataframe.
        """
        return NotImplemented

    @abstractmethod
    def generate_sink_table(self, 
                            input_date: datetime, 
                            source_data: DataFrame, 
                            is_first: bool) -> DataFrame:
        """Abstract method to generate the sink table.

        Args:
            input_date (datetime): The date image of the data that want to write.
            source_data (DataFrame): The Spark source dataframe.
            is_first (bool): If it's true, it will trigger the first building process. Otherwise it will trigger the normal building process.

        Returns:
            DataFrame: The Spark dataframe.
        """
        return NotImplemented
    # ------------------------------
    # create and merge table related method
    def create_delta_table(self, 
                           table_columns: list[tuple[Any]], 
                           partitioned_columns: list[str]) -> None:
        """Create the delta table when the table is initialized.

        Args:
            table_columns (list[tuple[Any]]): Columns of the table.
            partitioned_columns (list[str]): Partitioned columns of the table.

        Returns:
            None 
        """
        print("Creating table...")
        delta_table_q = DeltaTable.create(self.spark)
        for (col_name, data_type, nullable, generated_always_as, comment) in table_columns + self.table_base_columns:
            print(f"Add new column: {col_name}, data type: {data_type}")
            delta_table_q = delta_table_q\
                .addColumn(colName=col_name, dataType=data_type, nullable=nullable, generatedAlwaysAs=generated_always_as, comment=comment)
        if len(partitioned_columns) > 0:
            delta_table_q = delta_table_q\
                .partitionedBy(*partitioned_columns)
        delta_table_q = delta_table_q\
            .property('delta.logRetentionDuration', f"{self.log_retention_duration_days} days")\
            .property('delta.deletedFileRetentionDuration', f"{self.deleted_file_retention_duration_days} days")\
            .property('delta.checkpointInterval', f"{self.check_point_interval}")\
            .property('delta.enableChangeDataFeed', "true")\
            .location(self.get_sink_data_path())
        delta_table_q.execute()
        print("Finished table creation.")
        return None
    
    def update_delta_table(self, 
                           dataframes: list[DataFrame], 
                           table_columns: list[tuple[Any]], 
                           merge_columns: list[str],
                           partitioned_columns: list[str] = []) -> None:
        """Update the delta table.

        Args:
            dataframes (list[DataFrame]): List of input dataframe.
            table_columns (list[tuple[Any]]): Columns of the table.
            merge_columns (list[str]): Columns that use to merge in the table.
            partitioned_columns (list[str], optional): Partitioned colunms of the table if partition pruning is required. Defaults to [].

        Returns:
            None
        """
        print("Updating table...")
        for dataframe in dataframes:
            if len(dataframe.head(1)) > 0:
                # --------------------
                # partition pruning
                if len(partitioned_columns) > 0:
                    partition_pruning_condition = "((" + ") OR (".join([" AND ".join([f"dest.{k} = {v}" for k,v in row.asDict().items()]) for row in dataframe.select(*partitioned_columns).distinct().orderBy(*partitioned_columns).collect()]) + "))"
                else:
                    partition_pruning_condition = "1 = 1"
                # --------------------
                update_table_q = DeltaTable\
                    .forPath(self.spark, self.get_sink_data_path()).alias('dest')\
                    .merge(
                        dataframe.alias('update'),
                        " AND ".join([f"dest.{col} = update.{col}" for col in merge_columns]) + " AND " + partition_pruning_condition
                    ).whenNotMatchedInsert(
                        condition='update.is_deleted_flag = false',
                        values={
                            **{f"{col}": f"update.{col}" for col in [tp[0] for tp in table_columns]},
                            **{"log_status": func.lit("create"), "ts": "update.ts", "data_source": "update.data_source"} 
                        }
                    )
                # collect the table column name
                table_columns_name = [tp[0] for tp in table_columns]
                # --------------------
                # logic when the data in the row is updated
                update_condition = ""
                update_set = {
                    **{f"{col}": f"update.{col}" for col in list(set(table_columns_name)-set(merge_columns))},
                    "ts": "update.ts",
                    "log_status": func.lit("update")
                }
                update_table_q = update_table_q\
                    .whenMatchedUpdate(
                        condition=update_condition,
                        set=update_set
                    )
                # --------------------
                # logic when the data in the row is deleted
                delete_condition = ""
                delete_set = {
                    "ts": "update.ts",
                    "log_status": func.lit("delete")
                }
                update_table_q = update_table_q\
                    .whenMatchedUpdate(
                        condition=delete_condition,
                        set=delete_set
                    )
                # --------------------
                # logic when the data is re-created
                recreate_condition = ""
                recreate_set = {
                    **{f"{col}": f"update.{col}" for col in list(set(table_columns_name)-set(merge_columns))},
                    "ts": "update.ts",
                    "log_status": func.lit("create")
                }
                update_table_q = update_table_q\
                    .whenMatchedUpdate(
                        condition=recreate_condition,
                        set=recreate_set
                    )
                update_table_q.execute()
        print("Finished table updte.")
        return None
    # ------------------------------
    # transformation related method
    @staticmethod
    def get_filter_by_date_only(spark_col: str, 
                                filter_date: datetime) -> Column:
        """Select the data within the day only.

        Args:
            spark_col (str): column name of the timestamp.
            filter_date (datetime): Filter date.

        Returns:
            Column: The filtering codition.
        """
        year_col = func.year(func.from_unixtime(func.col(spark_col)/1000))
        month_col = func.month(func.from_unixtime(func.col(spark_col)/1000))
        day_col = func.dayofmonth(func.from_unixtime(func.col(spark_col)/1000))
        condition = (year_col == filter_date.year)\
            & (month_col == filter_date.month)\
            & (day_col == filter_date.day)
        return condition
    
    @staticmethod
    def get_filter_by_date_before(spark_col: str, 
                                  filter_date: datetime) -> Column:
        """Select the data within the day before.

        Args:
            spark_col (str): column name of the timestamp.
            filter_date (datetime): Filter date.

        Returns:
            Column: The filtering codition.
        """
        year_col = func.year(func.from_unixtime(func.col(spark_col)/1000))
        month_col = func.month(func.from_unixtime(func.col(spark_col)/1000))
        day_col = func.dayofmonth(func.from_unixtime(func.col(spark_col)/1000))
        condition = ((year_col < filter_date.year)\
                    | ((year_col == filter_date.year) & (month_col < filter_date.month))\
                    | ((year_col == filter_date.year) & (month_col == filter_date.month) & (day_col <= filter_date.day)))
        return condition
    
    def transform_delta_log_to_snapshot(self, 
                                        data: DataFrame, 
                                        partition_cols: list[str], 
                                        sorted_columns: list[tuple[Any]]=[('ts', 'desc'), 
                                                                          ('log_status_order', 'desc'), 
                                                                          ('_commit_version', 'desc'), 
                                                                          ('_change_type_order', 'desc')]) -> DataFrame:
        """Transform the delta log to latest snapshot without using time travelling.

        Args:
            data (DataFrame): Table delta log.
            partition_cols (list[str]): Partitioned columns of the data.
            sorted_columns (list[tuple[Any]], optional): Decide which order will be the first. Defaults to [('ts', 'desc'), ('log_status_order', 'desc'), ('_commit_version', 'desc'), ('_change_type_order', 'desc')].

        Raises:
            ValueError: When the ordering is not in {'asc', 'desc'}

        Returns:
            DataFrame: Latest snapshot of the table
        """
        # create the sorting columns
        sorting_columns = []
        for col_name, ordering in sorted_columns:
            if ordering == 'asc':
                sorting_columns.append(func.asc(func.col(col_name)))
            elif ordering == 'desc':
                sorting_columns.append(func.desc(func.col(col_name)))
            else:
                raise ValueError(f"Unknown sorting order: {ordering}")
        # create the order column
        trans_data = data\
            .repartition(*partition_cols)\
            .withColumn(
                'log_status_order',
                func.when(func.col("log_status")=='create', func.lit(1))
                .when(func.col("log_status")=="update", func.lit(2))
                .when(func.col("log_status")=='delete', func.lit(3))
            )
        if '_change_type' in trans_data.columns:
            trans_data = trans_data\
                .withColumn(
                    '_change_type_order',
                    func.when(func.col('_change_type')=='insert', func.lit(1))
                    .when(func.col('_change_type')=='update_preimage', func.lit(2))
                    .when(func.col('_change_type')=='update_postimage', func.lit(3))
                )
        trans_data = trans_data\
            .withColumn(
                'rank',
                func.rank().over(Window.partitionBy(*partition_cols).orderBy(*sorting_columns))
            ).filter(
                (func.col('rank')==1)
            )
        return trans_data.select(*list(data.columns))