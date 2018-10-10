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
	print(sample_data.count() == 22025)
	print(poi_list.count() == 4)

	# 0. Cleanup
	df = remove_dups(sample_data, ["TimeSt", "Latitude", "Longitude"])
	poi_df = remove_dups(poi_list, ["Latitude", "Longitude"])
	print(df.count() == 19999)
	print(poi_df.count() == 3)


	# 1. Label
	poi_map = {r["POIID"]: (r["Latitude"], r["Longitude"]) 
				for r in poi_df.collect()}

	# compute distance to each POI
	for poi, (lat, lon) in poi_map.items():
		print(poi, lat, lon)
		df = df.withColumn("dist_to_{0:s}".format(poi),
							F.array([F.sqrt((F.col("Longitude") - F.lit(lat))**2
							 				+ (F.col("Latitude") - F.lit(lon))**2),
							F.lit(poi)]))
		df = df.withColumn("h_dist_to_{0:s}".format(poi),
							F.array([2.0
								* F.asin(F.sqrt(F.sin(0.5 * F.radians(F.col("Latitude")) - F.radians(F.lit(lat)))**2
												+ F.cos(F.radians(F.col("Latitude")))
												  * F.cos(F.radians(F.lit(lat)))
												  * F.sin(0.5 * (F.radians(F.col("Longitude")) -F.radians(F.lit(lon))))**2)),
								F.lit(poi)]))

	# Find the nearest POI
	df = (df 
		  .withColumn("label",
		  			  F.least(*[F.col("dist_to_{0:s}".format(poi))
		  			  			for poi in poi_map])[1])
		  .drop(*["dist_to_{0:s}".format(poi) for poi in poi_map]))

	df = (df
		  .withColumn("h_label",
		  			  F.least(*[F.col("h_dist_to_{0:s}".format(poi))
		  			  			for poi in poi_map])[1])
		  .drop(*["h_dist_to_{0:s}".format(poi) for poi in poi_map]))

	df.show(5)


	# 2. Analysis
	## i. Calculate avg and std wrt poi



	# groupby the assigned POI and then use agg to get the avg and std of all points assigned to each POI

	## ii. Draw circles centered at POI, find radius and density


	# 3. Model
	## i. map density of POI to [-10, 10]
	## ii. POI hypotheses


