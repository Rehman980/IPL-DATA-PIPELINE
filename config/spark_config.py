def get_spark_config():
    return {
        "spark.app.name": "IPL Analytics",
        "spark.master": "local[*]",
        "spark.executor.memory": "4g",
        "spark.driver.memory": "4g",
        "spark.sql.shuffle.partitions": "8"
    }