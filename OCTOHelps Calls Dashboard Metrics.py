# Databricks notebook source
# MAGIC %md
# MAGIC # Replicate values in OCTOHelps Calls dashboard.
# MAGIC
# MAGIC ## Connect to the data

# COMMAND ----------

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

df = spark.read.table("octo_test.octo_test.octo_ctr_silver")

df_pandas = df.toPandas()

df_pandas.head(1)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Convert DisconnectTimestamp field to DataTime datatype and then convert from UTC to EST

# COMMAND ----------

from datetime import datetime, timedelta
import pandas as pd

df_pandas['DisconnectTimestamp'] = pd.to_datetime(df_pandas['DisconnectTimestamp'])

df_pandas['DisconnectTimestamp_est'] = df_pandas['DisconnectTimestamp'] - timedelta(hours=4, minutes=0)

df_pandas.head(1)

# COMMAND ----------

df_pandas.dtypes

# COMMAND ----------

# Min value
min_date = df_pandas['DisconnectTimestamp_est'].min()
print(min_date)

# Max value
max_date = df_pandas['DisconnectTimestamp_est'].max()
print(max_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter for specific Time Period

# COMMAND ----------

df_pandas = df_pandas[(df_pandas['DisconnectTimestamp_est'] >= '2024-07-01 00:00:00+00:00') & (df_pandas['DisconnectTimestamp_est'] <= '2024-08-31 23:59:59+00:00')]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time Period filtered for

# COMMAND ----------

# Min value
min_date = df_pandas['DisconnectTimestamp_est'].min()
print(min_date)

# Max value
max_date = df_pandas['DisconnectTimestamp_est'].max()
print(max_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Total Wait and Handle Time

# COMMAND ----------

df = df_pandas

import numpy as np

#df['total_wait_time_col'] = df.loc[df['AgentInteractionDuration'].notnull(), 'QueueDuration'].sum()
total_wait_time = df['QueueDuration'].sum()

total_handle_time = df['AgentInteractionDuration'].sum()

print(total_wait_time)
print(total_handle_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incoming Calls

# COMMAND ----------

# Filter initiation methods for incoming calls
incoming_types = ['INBOUND', 'TRANSFER']

df2 = df.copy()
df2 = df2[df2['InitiationMethod'].isin(incoming_types)]
incoming_calls = df2['ContactId'].nunique()

print(incoming_calls)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Handled Calls

# COMMAND ----------

# Filter initiation methods for incoming calls
incoming_types = ['INBOUND', 'TRANSFER']

df3 = df.copy()
df3 = df3[df3['InitiationMethod'].isin(incoming_types)]
df3 = df3[df3['AgentInteractionDuration'].notnull()]
handled_calls = df3['ContactId'].nunique()

print(handled_calls)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated Transfers

# COMMAND ----------

# Filter initiation methods for incoming calls
incoming_types = ['INBOUND']

df4 = df.copy()
df4 = df4[df4['InitiationMethod'].isin(incoming_types)]
df4 = df4[df4['QueueName'].isnull()]
df4 = df4[df4['TransferCompletedTimestamp'].notnull()]
automated_transfers = df4['ContactId'].nunique()

print(automated_transfers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Transfers

# COMMAND ----------

# Filter initiation methods for incoming calls
incoming_types = ['INBOUND']

df5 = df.copy()
df5 = df5[df5['InitiationMethod'].isin(incoming_types)]
df5 = df5[df5['AgentInteractionDuration'].notnull()]
df5 = df5[df5['TransferCompletedTimestamp'].notnull()]
manual_transfers = df5['ContactId'].nunique()

print(manual_transfers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Callbacks Requested

# COMMAND ----------

# Filter initiation methods for incoming calls
incoming_types = ['CALLBACK']

df6 = df.copy()
df6 = df6[df6['InitiationMethod'].isin(incoming_types)]
callbacks_requested = df6['ContactId'].nunique()

print(callbacks_requested)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Callbacks Handled

# COMMAND ----------

# Filter initiation methods for incoming calls
incoming_types = ['CALLBACK']

df6 = df.copy()
df6 = df6[df6['InitiationMethod'].isin(incoming_types)]
df6 = df6[df6['AgentInteractionDuration'].notnull()]
callbacks_handled = df6['ContactId'].nunique()

print(callbacks_handled)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Abandoned Calls

# COMMAND ----------

df7 = df.copy()
df7 = df7[df7['QueueEnqueueTimestamp'].notnull()]
df7 = df7[df7['ConnectedToAgentTimestamp'].isnull()]
df7 = df7[df7['NextContactId'].isnull()]
abandoned_calls = df7['ContactId'].nunique()

print(abandoned_calls)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Not Queued Abandoned

# COMMAND ----------

incoming_types = ['INBOUND']

df8 = df.copy()
df8 = df8[df8['InitiationMethod'].isin(incoming_types)]
df8 = df8[df8['NextContactId'].isnull()]
df8 = df8[df8['QueueEnqueueTimestamp'].isnull()]
df8 = df8[df8['TransferredToEndpointAddress'].isnull()]
notqueuedabandoned = df8['ContactId'].nunique()

print(notqueuedabandoned)

# COMMAND ----------

# MAGIC %md
# MAGIC ## OCTOHelps Dashboard KPI Metrics

# COMMAND ----------

pd.concat([pd.DataFrame({'incoming_calls': [incoming_calls], 'handled_calls': [handled_calls], 'abandoned_calls': [abandoned_calls], 'notqueuedabandoned': [notqueuedabandoned], 'total_wait_time': [total_wait_time], 'total_handle_time': [total_handle_time], 'automated_transfers': [automated_transfers], 'manual_transfers': [manual_transfers], 'callbacks_requested': [callbacks_requested], 'callbacks_handled': [callbacks_handled]})], axis=1)
