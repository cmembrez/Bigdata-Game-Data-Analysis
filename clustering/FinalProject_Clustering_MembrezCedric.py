# coding: utf-8

# # Pink Flamingo - Eglence Inc.
# # Cluster Analysis

# by Cédric Membrez, June 2020

# In[1]:

# imports
from pyspark.sql import SQLContext
from pyspark.sql.functions import months_between, round  # to_date, datediff,
from pyspark.sql.functions import format_number
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import count as _count
from pyspark.sql.functions import col as _col

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

import pycountry_convert as pc
import pycountry
import pandas as pd
from datetime import datetime

import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# In[2]:

# create Spark Context
# sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# ### READ DATA FILES
# 1) read separate CSV files
# 
# 2) merge into one dataframe
# 
# 3) clean dataframe

# In[3]:

# read ad-clicks.csv
adclicks_raw = sqlContext.read.load('file:///home/cloudera/flamingo-data/ad-clicks.csv',
                                    format='com.databricks.spark.csv',
                                    header='true',
                                    inferSchema='true')

# In[4]:

# buy-clicks.csv
buyclicks_raw = sqlContext.read.load('file:///home/cloudera/flamingo-data/buy-clicks.csv',
                                     format='com.databricks.spark.csv',
                                     header='true',
                                     inferSchema='true')

# In[5]:

# game-clicks.csv
gameclicks_raw = sqlContext.read.load('file:///home/cloudera/flamingo-data/game-clicks.csv',
                                      format='com.databricks.spark.csv',
                                      header='true',
                                      inferSchema='true')

# In[6]:

# level-events.csv
levelevents_raw = sqlContext.read.load('file:///home/cloudera/flamingo-data/level-events.csv',
                                       format='com.databricks.spark.csv',
                                       header='true',
                                       inferSchema='true')

# In[7]:

# team-assignments.csv
teamassignments_raw = sqlContext.read.load('file:///home/cloudera/flamingo-data/team-assignments.csv',
                                           format='com.databricks.spark.csv',
                                           header='true',
                                           inferSchema='true')

# In[8]:

# team.csv
team_raw = sqlContext.read.load('file:///home/cloudera/flamingo-data/team.csv',
                                format='com.databricks.spark.csv',
                                header='true',
                                inferSchema='true')

# In[9]:

# user-session.csv
usersession_raw = sqlContext.read.load('file:///home/cloudera/flamingo-data/user-session.csv',
                                       format='com.databricks.spark.csv',
                                       header='true',
                                       inferSchema='true')

# In[10]:

# users.csv
users_raw = sqlContext.read.load('file:///home/cloudera/flamingo-data/users.csv',
                                 format='com.databricks.spark.csv',
                                 header='true',
                                 inferSchema='true')

# In[11]:

# NUMBER OF ROWS PER FILE
print("Number of rows per file\n")
print("ad-clicks: {}".format(adclicks_raw.count()))
print("buy-clicks: {}".format(buyclicks_raw.count()))
print("game-clicks: {}".format(gameclicks_raw.count()))
print("level-events: {}".format(levelevents_raw.count()))
print("team-assignments: {}".format(teamassignments_raw.count()))
print("team: {}".format(team_raw.count()))
print("user-session: {}".format(usersession_raw.count()))
print("users: {}".format(users_raw.count()))

# In[12]:

adclicks_raw.printSchema()

# In[13]:

adclicks_raw.describe().toPandas().transpose()

# In[14]:

buyclicks_raw.printSchema()

# In[15]:

buyclicks_raw.describe().toPandas().transpose()

# In[16]:

gameclicks_raw.printSchema()

# In[17]:

gameclicks_raw.describe().toPandas().transpose()

# In[18]:

levelevents_raw.printSchema()

# In[19]:

levelevents_raw.describe().toPandas().transpose()

# In[20]:

teamassignments_raw.printSchema()

# In[21]:

teamassignments_raw.describe().toPandas().transpose()

# In[22]:

team_raw.printSchema()

# In[23]:

team_raw.describe().toPandas().transpose()

# In[24]:

usersession_raw.printSchema()

# In[25]:

usersession_raw.describe().toPandas().transpose()

# In[26]:

users_raw.printSchema()

# In[27]:

users_raw.describe().toPandas().transpose()

# In[ ]:


# # USER
# * COUNTRY - REGION -> pycountry
# * dob: date-of-birth -> age

# In[ ]:


# In[28]:

# retrieve country codes from user table
country_codes = users_raw.select('country').toPandas()["country"]

# In[29]:

# convert country to continent codes
continent_codes = []
for x in country_codes.tolist():
    try:
        continent_codes.append(pc.country_alpha2_to_continent_code(x))
    except Exception as e:
        print(e)

# In[30]:

# translate continent code to names
continent_codes_names_dict = {
    'AF': 'Africa',
    'AN': 'Antarctica',
    'AS': 'Asia',
    'EU': 'Europe',
    'NA': 'North America',
    'OC': 'Oceania',
    'SA': 'South America'
}
continent_names = [continent_codes_names_dict[x] for x in continent_codes]

# In[31]:

# assess number of lines 
# -> 'dropped' values because some unavailable country codes in pycountry
print(len(country_codes))
print(len(continent_codes))
print(len(continent_names))

# In[32]:

# shape into a DataFrame
continent_codes_df = pd.DataFrame(continent_names,
                                  columns=["continentname"])
continent_codes_df["count"] = 1
# continent_codes_df.shape

# In[33]:

# number of users per continent
# remark: the underlying data as 9-10 users per country
# remark: the data is simulated (cf techincal appendix, github)
continent_codes_df.groupby(["continentname"]).count()

# ### Age: from date of birth 'dob'

# In[34]:

users_raw.select('userId', 'timestamp', 'dob').show(5)

# In[35]:

# users_raw.select('timestamp', to_date('timestamp')).show(5)


# In[36]:

# compute user's age
users_age = users_raw.select('userId', round(months_between(users_raw['timestamp'],
                                                            users_raw['dob']) / 12, 0).alias("age"))

# In[37]:

# check few results:
users_age.show(3)

# In[38]:

users_age.describe().toPandas().transpose()

# # BuyClicks
# purchase behavior of users
# * based on buyId, price
# * amount spent: total, yearly, monthly?
# * frequency of purchase?
# * (average price -> already in Classification: Iphone (buy expensive) vs ...)

# In[39]:

buyclicks_raw.printSchema()

# In[40]:

buyclicks_raw.show(3)

# In[41]:

buyclicks_raw.count()

# In[42]:

copy_df = buyclicks_raw
copy_df_noNa = copy_df.na.drop()
copy_df_noNa.count()

# In[43]:

# our dataset covers only the year 2016, and the months of May and June.
# -> we can group by months:
# group the purchases per Month, per Day, and count #items bought:
buyclicks_perDay = buyclicks_raw.select(month("timestamp").alias("month"),
                                        dayofmonth("timestamp").alias("day"), "userId", "buyId",
                                        "price").groupBy("month", "day").count()

# In[44]:

buyclicks_perDay.show(100)  # show all 22 days of data

# In[45]:

# NUMBER ITEMS per USER
buyclicks_raw.groupBy("userId").count().orderBy("count", ascending=False).show(15)

# In[46]:

# NUMBER OF ITEMS per SESSION per USER
buyclicks_raw.groupBy("userId", "userSessionId").count().orderBy(["count", "userId", "userSessionId"],
                                                                 ascending=False).show(5)

# In[47]:

# sum price, pivoted by prices
buyclicks_pivotPrice = buyclicks_raw.groupBy("userId",
                                             "buyId").pivot("price").sum("price")

# In[48]:

buyclicks_pivotPrice.orderBy("userId").show(15)

# In[49]:

# ORDERED table by USER, SESSION, TIME, ITEM
buyclicks_raw.orderBy("userId", "userSessionId",
                      "timestamp", "buyId").show(15)

# In[50]:

# TOTAL SPENT BY USER
buyclicks_total_by_user = buyclicks_raw.groupBy("userId").agg(_sum("price").alias("totalspent")).orderBy("totalspent",
                                                                                                         ascending=False)

# In[51]:

buyclicks_total_by_user.show(5)

# In[ ]:


# ## AD CLICKS

# In[52]:

adclicks_raw.count()

# In[53]:

adclicks_raw.show(3)

# In[54]:

# NUMBER OF CLICKED AD per USER:
adclicks_clicks_by_user = adclicks_raw.groupBy("userId").count().select("userId", _col("count").alias("adClicks"))

# In[55]:

adclicks_clicks_by_user.show(3)

# In[56]:

adclicks_clicks_by_user.orderBy("adClicks", ascending=False).show(5)

# In[ ]:


# In[57]:

adclicks_raw.groupBy("userSessionId").count().orderBy(["count"],
                                                      ascending=False).show(15)

# In[ ]:


# # GAME CLICK

# In[ ]:


# In[58]:

gameclicks_raw.count()

# In[59]:

gameclicks_raw.show(3)

# In[60]:

gameclicks_count_by_user = gameclicks_raw.groupBy("userId").count()

# In[61]:

gameclicks_hit_by_user = gameclicks_raw.groupBy("userId").sum("isHit")

# In[62]:

type(gameclicks_hit_by_user)

# In[63]:

gameclicks_union = gameclicks_count_by_user.join(gameclicks_hit_by_user,
                                                 on=["userId"])

# In[ ]:


# In[64]:

gameclicks_hitDetails_per_user = gameclicks_union.withColumn("hitRatio",
                                                             round(_col("sum(isHit)") / _col("count"), 4))

# In[65]:

gameclicks_hitRatio_per_user = gameclicks_hitDetails_per_user.select("userId", "hitRatio")

# In[ ]:


# ## CLEANING DATA
# * drop na: myDF.na.drop()

# In[66]:

# union the ad-, buy-clicks data and age
# 1193 users gameclicked
users_age.printSchema()

# In[67]:

buyclicks_total_by_user.printSchema()

# In[68]:

adclicks_clicks_by_user.printSchema()

# In[69]:

adBuyClicks_age_data = users_age.join(buyclicks_total_by_user,
                                      on="userId"). \
    join(adclicks_clicks_by_user,
         on="userId")

# In[70]:

noUserId_adBuyClicks_age_data = adBuyClicks_age_data.select("age", "totalspent", "adClicks")

# In[71]:

print("{} rows x {} columns".format(noUserId_adBuyClicks_age_data.count(),
                                    len(noUserId_adBuyClicks_age_data.columns)))

# In[72]:

clicks_data_clean = noUserId_adBuyClicks_age_data.na.drop()

# In[73]:

clicks_data_clean.count()

# # FEATURES

# In[74]:


# In[75]:

myData = clicks_data_clean.toPandas()

# In[76]:

plt.scatter(myData["age"], myData["totalspent"])
plt.show()

# In[77]:

plt.scatter(myData["age"], myData["adClicks"])
plt.show()

# In[78]:

plt.scatter(myData["adClicks"], myData["totalspent"])
plt.show()

# In[79]:

# 3D
fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
# x-, y-, z-axis
ax.scatter(myData["age"],
           myData["totalspent"],
           myData["adClicks"])
# set axis labels
ax.set_xlabel('age')
ax.set_ylabel('total spent')
ax.set_zlabel('ad clicks')

plt.show()

# In[ ]:


# In[ ]:


# In[80]:

# features used
featuresUsed = ['age', 'totalspent', 'adClicks']

# In[81]:

# Vector Assembler
assembler = VectorAssembler(inputCols=featuresUsed,
                            outputCol="features_unscaled")

# In[82]:

# transform
assembled = assembler.transform(clicks_data_clean)

# In[ ]:


# In[83]:

# Features using StandardScaler
scaler = StandardScaler(inputCol="features_unscaled",
                        outputCol="features",
                        withStd=True,
                        withMean=True)

# In[84]:

# fit, transform
scalerModel = scaler.fit(assembled)
scaledData = scalerModel.transform(assembled)

# In[85]:

# data persist
scaledData = scaledData.select("features")
scaledData.persist()

# In[ ]:


# # CLUSTERING

# In[86]:

# from sklearn.cluster import KMeans as skKMeans


# In[87]:

# KMeans clustering: how many clusters?


# In[88]:

# generate 2 clusters
kmeans = KMeans(k=3, seed=1)

# In[89]:

# fit
model = kmeans.fit(scaledData)

# In[90]:

# transform
transformed = model.transform(scaledData)

# ### PREPARE DATA: for 3D PLOT

# In[91]:

print(transformed.count(), " ", len(transformed.columns))

# In[92]:

transformed.show(5)

# In[93]:

dataClusters = transformed.toPandas()
# dataClusters.shape

# In[94]:

dataClusters.head(5)

# In[95]:

dataClusters_x = []
dataClusters_y = []
dataClusters_z = []
for point in dataClusters.features:
    dataClusters_x.append(point[0])
    dataClusters_y.append(point[1])
    dataClusters_z.append(point[2])

# In[96]:

reshapedDataClusters = pd.DataFrame(data={'x': dataClusters_x,
                                          'y': dataClusters_y,
                                          'z': dataClusters_z,
                                          'prediction': dataClusters.prediction})

# In[97]:

index0 = reshapedDataClusters.prediction == 0
index1 = reshapedDataClusters.prediction == 1
index2 = reshapedDataClusters.prediction == 2

# In[ ]:


# In[98]:

# CENTER OF CLUSTERS


# In[99]:

# get center
centers = model.clusterCenters()

# In[100]:

# print center
print(centers)


# # ANALYSIS

# In[101]:

# analyze of clusters:
# first center is located at: array([ 0.3270975 ,  0.49940114,  0.82038724])
# second center at: array([-0.31410807, -0.47956933, -0.78780869])


# In[102]:

figClus = plt.figure()
axClus = figClus.add_subplot(111, projection='3d')
axClus.scatter(reshapedDataClusters.x[index0],
               reshapedDataClusters.y[index0],
               reshapedDataClusters.z[index0],
               c='blue', label='cluster-1')
axClus.scatter(reshapedDataClusters.x[index1],
               reshapedDataClusters.y[index1],
               reshapedDataClusters.z[index1],
               c='orange', label='cluster-2')
axClus.scatter(reshapedDataClusters.x[index2],
               reshapedDataClusters.y[index2],
               reshapedDataClusters.z[index2],
               c='red', label='cluster-3')

# set labels
axClus.set_xlabel('age')
axClus.set_ylabel('total spent')
axClus.set_zlabel('ad clicks')

# clusters' center
axClus.scatter(centers[0][0],
               centers[0][1],
               centers[0][2],
               c='yellow', label='centroid-1')
axClus.scatter(centers[1][0],
               centers[1][1],
               centers[1][2],
               c='green', label='centroid-2')
axClus.scatter(centers[2][0],
               centers[2][1],
               centers[2][2],
               c='black', label='centroid-3')

plt.show()
