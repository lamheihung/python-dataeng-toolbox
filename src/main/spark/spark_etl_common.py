from typing import Union, Optional
from pyspark import SparkConf
from pyspark.sql import SparkSession

from .task_base import TaskBase


class SparkEtlCommon(TaskBase):
    """SparkEtlCommon is the foundamental of all Spark application.

    Args:
        TaskBase (TaskBase): The foundamental of application.
    """
    def __init__(self,
                 config: Union[str, dict[str, Union[str, dict[str, str]]]],
                 spark: Optional[SparkSession]=None,
                 app_name: str='Testing-App'):
        """Constructs all the necessary attributes for the SparkEtlCommon.

        Args:
            config (Union[str, dict[str, Union[str, dict[str, str]]]]): The dictionary or the json path of the configuration. Defaults to None.
            spark (Optional[SparkSession], optional): The configured SparkSession. Defaults to None.
            app_name (str, optional): The Spark application name. Defaults to 'Testing-Spark-App'.
        """
        super().__init__(config=config)
        self.app_name = app_name
        self.spark = spark
    # ------------------------------
    # property
    @property
    def spark(self) -> SparkSession:
        return self._spark

    @spark.setter
    def spark(self, value):
        if isinstance(value, SparkSession):
            self._spark = value
        else:
            self._spark = self.create_spark_session()
    # ------------------------------
    def create_spark_session(self) -> SparkSession:
        """Create Spark session.

        Returns:
            SparkSession: The configured Spark session.
        """
        builder = SparkSession.builder
        spark_config = list()
        # ------------------------------
        # This part is based on different application than you need to connect with your Spark application.
        if 'spark_config' in self.config:
            spark_config += [(key, value) for key, value in self.config['spark_config'].items()]
        # ------------------------------
        # builder
        conf = SparkConf().setAll(spark_config)
        builder = builder.config(conf=conf)
        builder = builder.appName(self.app_name)
        spark = builder.getOrCreate()
        return spark