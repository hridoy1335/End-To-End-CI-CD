from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table(
    name="Student_Grade",
    comment="student grade analytics data engineer project",
    table_properties={"pipelines.autoOptimize.managed": "true", "quality": "bronze"}
)
def Student_Grade():
    # Read from the "sample_trips" table, then sum all the fares
    return (
        spark.readStream.table("workspace.oltp.student_grade")
        .withColumn("id", monotonically_increasing_id())
        .withColumn("created_at", current_timestamp())
    )
