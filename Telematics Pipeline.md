# Setting Up the Spark Session and Loading Data


```python
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Telematics Pipeline") \
    .getOrCreate()

# Load data from a CSV file
data_path = r"C:\allVehicles.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

```

# Data Cleansing



```python
# Drop rows with any null values
df = df.na.drop()

# Drop duplicate rows
df = df.dropDuplicates(['deviceID', 'timestamp'])

```

# Transforming accData
Applying the formulae provided in the document to using the @udf decorator from PySpark to create a UDF that follows the provided formula

        


```python
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
import numpy as np

@udf(returnType=FloatType())
def convert_acc(acc_sample):
    if len(acc_sample) == 162:  # Verify the sample length is as expected
        mx = int(acc_sample[0:4], 16)
        my = int(acc_sample[4:8], 16)
        mz = int(acc_sample[8:12], 16)

        # Adjust the values based on two's complement
        mx = mx - 256 if mx > 127 else mx
        my = my - 256 if my > 127 else my
        mz = mz - 256 if mz > 127 else mz

        # Convert to G-force
        mx = mx * 0.01536
        my = my * 0.01536
        mz = mz * 0.01536

        return (mx, my, mz)
    else:
        return (None, None, None)

```


```python
# Applying the UDF to the DataFrame
df = df.withColumn("convertedAccData", convert_acc(col("accData")))

```

*Note: I didn't run this part as I get a Py4JJavaError Error due to wrong version of winutils for Hadoop on my environment can't fix due to **time constraints***



```python
# Display the Results
df.select("deviceID", "timestamp", "accData", "convertedAccData").show(truncate=False)

```

# Save or Continue Processing



```python
# Save the DataFrame to a new CSV file
output_path = r"C:\cleaned_telematics_data.csv"
df.write.csv(output_path, mode="overwrite", header=True)

```

# close spark session


```python
spark.stop()

```
