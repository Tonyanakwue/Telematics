#!/usr/bin/env python
# coding: utf-8

# # Start Spark Session and Load Data 

# In[64]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min

# Start Spark session
spark = SparkSession.builder \
    .appName("Telematics Dashboard") \
    .getOrCreate()

# Load data
df = spark.read.csv(r"C:\allVehicles.csv", header=True, inferSchema=True)

# Data transformation
speed_stats = df.groupBy("tripID").agg(
    avg("gps_speed").alias("avg_speed"),
    max("gps_speed").alias("max_speed"),
    min("gps_speed").alias("min_speed")
)

# Convert to Pandas DataFrame for visualization
pandas_df = speed_stats.toPandas()



# # 1. Filtering Data
# We can  filter out data for example to focus on trips where the speed exceeded 100 km/h.

# In[28]:


from pyspark.sql import SparkSession

# Start a Spark session
spark = SparkSession.builder \
    .appName("High Speed Analysis") \
    .getOrCreate()

# Assuming df is your loaded DataFrame
high_speed_trips = df.filter(df.gps_speed > 100)
high_speed_trips.show()


# # 2. Joining Data
# Join our telematics data with a simulated maintenance DataFrame, linking by deviceID, and adding imaginary maintenance dates and types.

# In[29]:


from pyspark.sql.functions import expr
import random

# Start a Spark session
spark = SparkSession.builder \
    .appName("Maintenance Data Simulation") \
    .getOrCreate()

# Load your main dataset
data_path = r"C:\allVehicles.csv"
df = spark.read.csv(data_path, header=True, inferSchema=True)

# Simulate a maintenance DataFrame by adding artificial maintenance data
maintenance_df = df.select("deviceID").distinct() \
    .withColumn("maintenance_date", expr("current_date()")) \
    .withColumn("maintenance_type", expr("CASE WHEN rand() > 0.5 THEN 'Type A' ELSE 'Type B' END"))

maintenance_df.show()


# In[30]:


# Join the main DataFrame with the simulated maintenance DataFrame
combined_data = df.join(maintenance_df, "deviceID")
combined_data.show()


# # 3. Grouping and Aggregating
# Find how much the speed varies on trips and how many different trouble codes occur during each trip.

# In[65]:


from pyspark.sql.functions import stddev, countDistinct

# Calculate standard deviation of speed and count distinct trouble codes
advanced_stats = df.groupBy("tripID").agg(
    stddev("gps_speed").alias("speed_stddev"),
    countDistinct("dtc").alias("unique_trouble_codes")
)
advanced_stats.show()


# # 4. Window Functions
# Create a DataFrame that ranks each data point by time within each trip, which can be use for analysing sequences or changes over time.

# In[66]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.partitionBy("tripID").orderBy("timestamp")
df_with_row_number = df.withColumn("row_number", row_number().over(windowSpec))
df_with_row_number.show()


# # 5. Pivot Tables
# We can generate a pivot table to explore average speed across different throttle positions for each type of dtc.

# In[67]:


from pyspark.sql.functions import avg

# Assuming df is already your loaded DataFrame from Spark
pivot_table = df.groupBy("dtc").pivot("tPos").agg(
    avg("gps_speed").alias("avg_speed")
)

# Check the output (optional)
pivot_table.show()


# In[ ]:





# # Some Visuliation Using Seaborn

# In[68]:


# Create a distribution plot for average speeds
plt.figure(figsize=(12, 6))
dist_plot = sns.histplot(pandas_df['avg_speed'], bins=20, kde=True)
dist_plot.set_title('Distribution of Average Speeds')
dist_plot.set_xlabel('Average Speed (gps)')
dist_plot.set_ylabel('Frequency')
plt.show()


# In[69]:


# Create a scatter plot
plt.figure(figsize=(12, 6))
scatter_plot = sns.scatterplot(x='min_speed', y='max_speed', hue='tripID', data=pandas_df, palette='viridis', legend=None)
scatter_plot.set_title('Min vs. Max Speeds per Trip')
scatter_plot.set_xlabel('Minimum Speed (gps)')
scatter_plot.set_ylabel('Maximum Speed (gps)')
plt.show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




