import json
from redash.query_runner import BaseQueryRunner, register
from redash.utils import json_dumps
from pyspark.sql import SparkSession


class SparkSQL(BaseQueryRunner):
    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "spark_master": {"type": "string"},
                "app_name": {"type": "string"},
            },
            "required": ["spark_master", "app_name"],
        }

    @classmethod
    def type(cls):
        return "sparksql"

    @classmethod
    def name(cls):
        return "SparkSQL"

    def __init__(self, configuration):
        super(SparkSQL, self).__init__(configuration)

        self.spark_master = self.configuration.get("spark_master")
        self.app_name = self.configuration.get("app_name")
        self.spark = None

    def test_connection(self):
        """
        Tests the connection by initializing SparkSession and running a basic query.
        """
        self._init_spark()
        self.spark.sql("SELECT 1").collect()
    def _init_spark(self):
        """
        Initializes the Spark session.
        """
        if not self.spark:
            self.spark = SparkSession.builder \
                .remote(self.spark_master) \
                .appName(self.app_name) \
                .getOrCreate()

    def run_query(self, query, user):
        """
        Executes the given SparkSQL query.
        """
        try:
            self._init_spark()
            df = self.spark.sql(query)
            # Convert the DataFrame to JSON for Redash consumption
            rows = [row.asDict() for row in df.collect()]
            columns = [{"name": col[0], "type": "string"} for col in df.dtypes]
            data = {"columns": columns, "rows": rows}
            return data, None
        except Exception as ex:
            return None, str(ex)


register(SparkSQL)