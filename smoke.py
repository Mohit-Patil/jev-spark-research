"""No external API calls. Minimal real Spark/AQE startup test."""
import json
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master('local[2]').appName('MacResearchLinkSmoke')
         .config('spark.ui.enabled', 'false')
         .config('spark.driver.bindAddress', '127.0.0.1')
         .config('spark.driver.host', '127.0.0.1')
         .config('spark.sql.adaptive.enabled', 'true')
         .config('spark.sql.shuffle.partitions', '4')
         .config('spark.driver.memory', '1g')
         .getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
    answer = spark.range(10000).selectExpr('id % 10 AS k').groupBy('k').count()
    rows = answer.collect()
    assert len(rows) == 10 and all(r['count'] == 1000 for r in rows)
    print(json.dumps({'spark_version': spark.version,
                      'aqe_enabled': spark.conf.get('spark.sql.adaptive.enabled'),
                      'rows_checked': 10000, 'correct': True}))
    print(answer._jdf.queryExecution().executedPlan().toString())
finally:
    spark.stop()
