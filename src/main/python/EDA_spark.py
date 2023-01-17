from matplotlib import cycler
import os
from wordcloud import WordCloud
import plotly.express as px
import plotly.graph_objects as go
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
import findspark
from utils import *
findspark.init

# Start Spark session
spark = SparkSession.builder.master("local[*]").getOrCreate()


'''
Load raw data from disk using spark

'''

links = spark.read\
    .format("com.databricks.spark.csv")\
    .option("multiline", True)\
    .option("header", True)\
    .option("escape", "\"")\
    .option("inferschema", True)\
    .csv("../../data/links.csv")

keywords = spark.read\
    .format("com.databricks.spark.csv")\
    .option("multiline", True)\
    .option("header", True)\
    .option("escape", "\"")\
    .option("inferschema", True)\
    .csv("../../data/keywords.csv")

movies_metadata = spark.read\
    .format("com.databricks.spark.csv")\
    .option("multiline", True)\
    .option("header", True)\
    .option("escape", "\"")\
    .option("inferschema", True)\
    .csv("../../data/movies_metadata_updated.csv")

credits = spark.read\
    .format("com.databricks.spark.csv")\
    .option("multiline", True)\
    .option("header", True)\
    .option("escape", "\"")\
    .option("inferschema", True)\
    .csv("../../data/credits.csv")

ratings = spark.read\
    .format("com.databricks.spark.csv")\
    .option("multiline", True)\
    .option("header", True)\
    .option("escape", "\"")\
    .option("inferschema", True)\
    .csv("../../data/ratings.csv")

ratings_sample = spark.read\
    .format("com.databricks.spark.csv")\
    .option("multiline", True)\
    .option("header", True)\
    .option("escape", "\"")\
    .option("inferschema", True)\
    .csv("../../data/ratings_sample.csv")


'''
Save basic info of DF schemas

'''

schema = StructType().add(
    "fileName",
    StringType(),
    nullable=True).add(
        "fileCount",
        IntegerType(),
        nullable=True).add(
            'noOfColumns',
            IntegerType(),
    nullable=True)

dataDictionary = [
    ('Credits.csv', credits.count(), len(
        credits.columns)), ('Keywords.csv', keywords.count(), len(
            keywords.columns)), ('links.csv', links.count(), len(
                links.columns)), ('Movies_metadata.csv', movies_metadata.count(), len(
                    movies_metadata.columns)), ('Ratings.csv', ratings.count(), len(
                        ratings.columns))]

df = spark.createDataFrame(data=dataDictionary, schema=schema)


'''
Data Cleaning

'''

movies_metadata = movies_metadata.where(
    ~col('id').isin('82663', '162372', '215848'))
movies_metadata = movies_metadata.na.drop(
    subset=[
        "production_companies",
        "production_countries",
        "genres"])
movies_metadata = movies_metadata.withColumn(
    'genres',
    when(
        col('genres') == '[]',
        "[{'id': 0, 'name': 'Unknown'}]").otherwise(
            col('genres'))) .withColumn(
                'production_companies',
                when(
                    col('production_companies') == '[]',
                    "[{'name': 'Unknown', 'id': 0}]").otherwise(
                        col('production_companies'))) .withColumn(
                            'production_countries',
                            when(
                                col('production_countries') == '[]',
                                "[{'iso_3166_1': 'Unknown', 'name': 'Unknown'}]").otherwise(
                                    col('production_countries')))

datadict = {
    'Ratings.csv': ratings,
    'Movies_metadata.csv': movies_metadata,
    'Credits.csv': credits,
    'Keywords.csv': keywords,
    'links.csv': links}

for key, value in datadict.items():
    dfStats = value.select([count(when(col(c).isNull() | isnan(c), 'True')).alias(
        c) for c, c_type in value.dtypes if c_type not in ('timestamp', 'boolean')])
    print("Column stats for data file :" + key + "\n")
    dfStats.show()

movies_metadata = movies_metadata.withColumn(
    'budget',
    col('budget').cast('integer')) .withColumn(
        'popularity',
        col('popularity').cast('float')) .withColumn(
            'revenue',
    col('revenue').cast('integer'))


# identify duplicates based on IMDB ID
df_dup = movies_metadata.groupby(
    'imdb_id',
    'title',
    'release_date',
    'overview').count().filter("count > 1").show()

# Total number of duplicate rows
movies_metadata.groupby(
    'imdb_id',
    'title',
    'release_date',
    'overview').count().where(
        f.col('count') > 1).select(
            f.sum('count')).show()

# DROP Duplicates - using drop_duplicates - RETURNS NEW DF
movies_metadata = movies_metadata.drop_duplicates(
    ['imdb_id', 'title', 'release_date', 'overview'])

movies_metadata = movies_metadata.withColumnRenamed("id", "id_ori")\
    .withColumnRenamed("poster_path", "poster_path_ori")

# Attach schema to Json object column
df = movies_metadata.withColumn(
    "belongs_to_collection_value", from_json(
        movies_metadata.belongs_to_collection, MapType(
            StringType(), StringType())))


# Get the distinct Keys of Json object
key_df = df.select(
    explode(
        map_keys(
            col('belongs_to_collection_value')))).distinct()

# Convert Key collection object to a List
keylst = list(map(lambda row: row[0], key_df.collect()))

# Retrive values based on Keys into a seperate column
key_cols = map(
    lambda f: df['belongs_to_collection_value'].getItem(f).alias(
        str(f)), keylst)
# df2=df2.select(col('belongs_to_collection'),*key_cols)
df = df.select(*movies_metadata.columns, *key_cols)


# Step 1 - Define  schema of Json array type
schema = ArrayType(StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('name', StringType(), nullable=False)]))

# Step 2 - UDF function to convert list to column seperated values. As Data is in Json array, extracting
# values based on Json keys will  produce list
convertUDF = udf(lambda s: ','.join(map(str, s)), StringType())

# Step 3 - Json parsing
df = df.withColumn(
    "production_companies_values",
    when(
        col('production_companies') == '[]',
        '').otherwise(
            convertUDF(
                from_json(
                    movies_metadata.production_companies,
                    schema).getField("name")))) .withColumn(
                        "production_countries_values",
                        convertUDF(
                            from_json(
                                movies_metadata.production_countries,
                                schema).getField("name"))) .withColumn(
                                    "genres_value",
                                    convertUDF(
                                        from_json(
                                            movies_metadata.genres,
                                            schema).getField("name")))

'''
EDA

'''

# Show statistics of key columns (numerics)
df.select(
    'runtime',
    'budget',
    'revenue',
    'vote_average',
    'vote_count',
    'popularity') .summary(
        "count",
        "min",
        "25%",
        "75%",
    "max").show()

df_pd = df.select(
    "runtime",
    "revenue",
    "vote_average",
    "vote_count").toPandas()


colors = cycler('color',
                ['#EE6666', '#3388BB', '#9988DD',
                 '#EECC55', '#88BB44', '#FFBBBB'])
plt.rc('axes', facecolor='#ffffff', edgecolor='none',
       axisbelow=True, grid=True)
plt.rc('grid', color='w', linestyle='solid')

plt.rc('xtick', direction='out', color='gray')
plt.rc('ytick', direction='out', color='gray')
plt.rc('patch', edgecolor='#E6E6E6')
plt.rc('lines', linewidth=2)
plt.rc(('xtick', 'ytick'), labelsize=15)
plt.rcParams['axes.labelsize'] = 15


fig = plt.figure(figsize=(20, 20))
grid = plt.GridSpec(3, 2, wspace=0.2, hspace=0.2)


f1 = sns.histplot(data=df_pd, x="runtime", kde=True,
                  ax=fig.add_subplot(grid[0, :]))
f1.set_xlabel('Runtime in Minutes')

f2 = sns.histplot(data=df_pd, x="revenue", kde=False,
                  ax=fig.add_subplot(grid[1, :]))
f2.set_xlabel('revenue in USD Millions')

f3 = sns.histplot(data=df_pd, x="vote_average", kde=False,
                  ax=fig.add_subplot(grid[2, :1]))
f3.set_xlabel('Vote Average')

f3 = sns.histplot(data=df_pd, x="vote_count", kde=False,
                  ax=fig.add_subplot(grid[2, 1]))
f3.set_xlabel('Vote Count')
f3.set_xlim(0, 5000)
f3.xaxis.set_ticks(np.arange(-500, 5000, 500))
f3.set_ylim(0, 2500)

plt.show()


data_for_corr = df.select(
    'budget',
    'popularity',
    'revenue',
    'runtime',
    'vote_average',
    'vote_count') .withColumn(
        "profit_loss",
        coalesce(
            col('revenue'),
            lit(0)) -
    coalesce(
            col('budget'),
            lit(0)))

# convert to Pandas dataframe and rows which has null values
data_for_corr_pd = data_for_corr.toPandas().dropna(how='any')


correlations = data_for_corr_pd.corr()
f, ax = plt.subplots(figsize=(10, 6))
sns.heatmap(correlations, annot=True, cmap="YlGnBu", linewidths=.5)

plt.show()


# Top 10 voted movies
df_pd_votecount = df.select(
    'original_title',
    'vote_count',
    'vote_average').toPandas()
df_pd_votecount = df_pd_votecount.sort_values(
    by='vote_count', ascending=False).iloc[0:21, :].reset_index(drop=True)

fig = plt.figure(figsize=(20, 10))
ax = plt.subplot(111)
bar_plot = sns.barplot(
    x=df_pd_votecount['original_title'],
    y=df_pd_votecount['vote_count'],
    ax=ax)
for tick in bar_plot.get_xticklabels():
    tick.set_rotation(90)


_show_on_single_plot(bar_plot)
plt.show()


# Top 10 movies by budget & revenue
df_moviesBybudget = df.select(
    'id_ori',
    'original_title',
    'budget',
    'revenue').orderBy(
        col('budget'),
    ascending=False)

# convert to Pandas dataframe
df_pd_top15moviesBybudget = df_moviesBybudget.toPandas().head(10)


ax = df_pd_top15moviesBybudget.plot(
    x="original_title", y=[
        "budget", "revenue"], kind="bar", figsize=(
            15, 10))
formatter = FuncFormatter(currency)
ax.yaxis.set_major_formatter(formatter)
ax.set_xlabel('original_title', fontsize=20)
ax.set_ylabel('Budget', fontsize=20)
ax.set_title('Top 10 movies by Budget', fontsize=20, loc='center')
plt.show()

# Top 10 movies just by revenue
df_moviesByRevenue = df.select(
    'id_ori',
    'original_title',
    'revenue',
    'budget').orderBy(
        col('revenue'),
    ascending=False)

# convert to Pandas dataframe
df_pd_top15moviesByRevenue = df_moviesByRevenue.toPandas().head(10)

ax = df_pd_top15moviesByRevenue.plot(
    x="original_title", y=[
        "budget", "revenue"], kind="bar", figsize=(
            15, 10))
formatter = FuncFormatter(currency)
ax.yaxis.set_major_formatter(formatter)

ax.set_title('Top 10 movies by Revenue', fontsize=20, loc='center')
ax.set_xlabel('original_title', fontsize=20)
ax.set_ylabel('Revenue', fontsize=20)
plt.legend()
plt.show()


# Top 10 box office flops
df_moviesByLoss = df.filter(
    col('revenue') > 0) .withColumn(
        "profit_loss",
        coalesce(
            col('revenue'),
            lit(0)) -
    coalesce(
            col('budget'),
            lit(0))) .filter(
    col('profit_loss') < 0) .select(
    'id_ori',
    'original_title',
    'revenue',
    'budget',
    "profit_loss").orderBy(
    col('profit_loss'),
    ascending=True)

df_pd_moviesByLoss = df_moviesByLoss.toPandas().head(10)

ax = df_pd_moviesByLoss.plot(
    x="original_title", y=[
        "budget", "revenue", 'profit_loss'], kind="bar", figsize=(
            15, 10))
formatter = FuncFormatter(currency)
ax.yaxis.set_major_formatter(formatter)

ax.set_title('Top 10 Box office flops', fontsize=20, loc='center')
ax.set_xlabel('original_title', fontsize=20)
ax.set_ylabel('Revenue', fontsize=20)
plt.legend()
plt.show()

# Movies by year, countries
df_pd_moviesperYear = df.filter(
    col('release_date').isNotNull()).select(
        'original_title',
        'release_date',
        year(
            to_date(
                df.release_date,
                'yyyy-MM-dd')).alias('release_year')) .groupBy('release_year').count().toPandas()

df_pd_moviesperYear = df_pd_moviesperYear.fillna(0).astype({'release_year': int}).sort_values(
    by='release_year', ascending=True, inplace=False, ignore_index=True)


fig = px.bar(
    df_pd_moviesperYear,
    y='count',
    x='release_year',
    text='count',
    title='Number of Succesful Movies launched by year of the 20th Century')
fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
fig.update_layout(uniformtext_minsize=8, uniformtext_mode='hide')
fig.update_xaxes(
    range=[1900, 2018],  # sets the range of xaxis
    constrain="domain",  # meanwhile compresses the xaxis by decreasing its "domain"
)
fig.show()


prod_cntry_schema = ArrayType(StructType([
    StructField('iso_3166_1', StringType(), nullable=False),
    StructField('name', StringType(), nullable=False)]))


df_prodcntry = df.where(
    col('production_countries_values') != "").select(
        'id_ori',
        'original_title',
    'production_countries')

df_prodcntry = df_prodcntry.select(
    *df_prodcntry.columns,
    explode(
        from_json(
            col('production_countries'),
            prod_cntry_schema)).alias('parsedval')) .select(
                *df_prodcntry.columns,
                col('parsedval').getItem('iso_3166_1').alias('cntry_code'),
    col('parsedval').getItem('name').alias('cntry_name'))


df_pd_cntry = df_prodcntry.groupby(
    col('cntry_code'),
    col('cntry_name')).count().orderBy(
        col('count').desc()).toPandas()

fig = px.bar(
    df_pd_cntry.head(20),
    y='count',
    x='cntry_name',
    text='count',
    title='Number of Movies launched by countries')
fig.show()

fig = go.Figure(data=go.Choropleth(
    locations=df_pd_cntry['cntry_name'],
    z=df_pd_cntry['count'],
    text=df_pd_cntry['cntry_name'],
    colorscale='Blues',
    autocolorscale=False,
    locationmode='country names',
    reversescale=True,
    marker_line_color='darkgray',
    marker_line_width=0.5,
    colorbar_title='Movies count',
))


fig.update_layout(
    title_text='Count of movies produced per country',
    geo=dict(
        showframe=False,
        showcoastlines=False,
        projection_type='equirectangular'
    ),
    annotations=[dict(
        x=0.55,
        y=0.1,
        xref='paper',
        yref='paper',
        showarrow=False
    )]
)

# Title count
df_pd_collection = df.where(
    col('name').isNotNull()).select(
        col('name')).groupBy(
            col('name')).count().orderBy(
                col('count').desc()).toPandas()
df_pd_collection.head(20)


# WordCloud
genres_schema = ArrayType(StructType([
    StructField('id', StringType(), nullable=False),
    StructField('name', StringType(), nullable=False)]))

df_wc = df.select(
    explode(
        from_json(
            col('genres'),
            genres_schema).getField("name").alias('genres_val')))
df_wc.groupby(
    col('col').alias('gener_values')).count().orderBy(
        col('count').desc()).show()
