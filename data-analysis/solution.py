from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def read_data_from_file(file_path):
	return spark.read.csv(file_path, header=True, inferSchema=True, ignoreLeadingWhiteSpace=True)


def remove_dups(df, duplicate_columns=[], remove=False):
	"""
	Return dataframe with only the first occurance of duplicate records, or 
	if the remove flag is set, remove duplicates entirely from the dataset.
	"""
	if remove:
		return df.groupby(duplicate_columns).count().where(F.col("count") == 1)
	return df.dropDuplicates(subset=duplicate_columns)


if __name__ == '__main__':
	spark = SparkSession\
			.builder\
			.appName("spark-app")\
			.getOrCreate()
	sample_data = read_data_from_file("/tmp/data/DataSample.csv")
	poi_list = read_data_from_file("/tmp/data/POIList.csv")

	# 0. Cleanup
	df = remove_dups(sample_data, ["TimeSt", "Latitude", "Longitude"])
	poi_df = remove_dups(poi_list, ["Latitude", "Longitude"])


	# 1. Label
	poi_map = {r["POIID"]: (r["Latitude"], r["Longitude"]) 
				for r in poi_df.collect()}

	# compute distance to each POI (euclidean and haversine)
	r = 6371 # radius of earth in km
	for poi, (lat, lon) in poi_map.items():
		print(poi, lat, lon)
		df = df.withColumn("dist_to_{0:s}".format(poi),
							F.array([F.sqrt((F.col("Longitude") - F.lit(lat))**2
							 				+ (F.col("Latitude") - F.lit(lon))**2),
							F.lit(poi)]))
		df = df.withColumn("h_dist_to_{0:s}".format(poi),
							F.array([2.0*r 
								* F.asin(F.sqrt(F.sin(0.5 * (F.radians(F.col("Latitude")) - F.radians(F.lit(lat))))**2
												+ F.cos(F.radians(F.col("Latitude")))
												  * F.cos(F.radians(F.lit(lat)))
												  * F.sin(0.5 * (F.radians(F.col("Longitude")) -F.radians(F.lit(lon))))**2)),
								F.lit(poi)]))

	# Find the nearest POI
	df = (df 
		  .withColumn("label",
		  			  F.least(*[F.col("dist_to_{0:s}".format(poi))
		  			  			for poi in poi_map]))
		  .drop(*["dist_to_{0:s}".format(poi) for poi in poi_map]))

	df = (df
		  .withColumn("h_label",
		  			  F.least(*[F.col("h_dist_to_{0:s}".format(poi))
		  			  			for poi in poi_map]))
		  .drop(*["h_dist_to_{0:s}".format(poi) for poi in poi_map]))


	# 2. Analysis
	## i. Calculate avg and std wrt poi
	df = df.withColumn("poi", F.col("h_label")[1]).withColumn("distance", F.col("h_label")[0])
	df_stats = df.groupBy("poi").agg(F.avg("distance").alias("avg"), F.stddev("distance").alias("std"), 
									 F.max("distance").alias("max"), F.min("distance").alias("min"))

	## ii. write data to file for further visualization
	df_stats.coalesce(1).write.csv("/tmp/data/stats.csv", header='true')


	# 3. Model
	## i. map density of POI to [-10, 10]
	## ii. POI hypotheses


