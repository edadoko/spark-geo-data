import pyspark.sql.functions as F


def read_data_from_file(file_path):
	return spark.read.csv(file_path, header=True)


def remove_dups(df, duplicate_columns=[], remove=False):
	"""
	Return dataframe with only the first occurance of duplicate records, or 
	if the remove flag is set, remove duplicates entirely from the dataset.
	"""
	if remove:
		return df.groupby(duplicate_columns).count().where(F.col("count") > 1)
	return df.dropDuplicates(subset=duplicate_columns)


def haversine(lon1, lat1, lon2, lat2):
	"""
	Calculate the great circle distance between two points 
	on the earth (specified in decimal degrees)
	"""
	# convert decimal degrees to radians 
	lon1, lat1, lon2, lat2 = map(F.radians, [lon1, lat1, lon2, lat2])
	# haversine formula 
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = F.sin(dlat/2)**2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon/2)**2
	c = 2 * F.asin(F.sqrt(a))
	r = 6371 # Radius of earth in kilometers (3956 for miles)
	return c * r


def get_min_dist(df, poi):
	# assuming that poi_list is never going to be big enough to need to be paralelized
	poi_list = [list(row) for row in poi.collect()]
	lon = 2
	lat = 1
	dist1 = haversine(df["Longitude"], df["Latitude"], poi_list[0][lon], poi_list[0][lat]) 
	dist2 = haversine(df["Longitude"], df["Latitude"], poi_list[1][lon], poi_list[1][lat]) 
	dist3 = haversine(df["Longitude"], df["Latitude"], poi_list[2][lon], poi_list[2][lat]) 
	distance = F.least(dist1, dist2, dist3)

	return df.withColumn("distance", distance)#.withColumn("label", name)


if __name__ == '__main__':
	sample_data = read_data_from_file("/tmp/data/DataSample.csv")
	poi_list = read_data_from_file("/tmp/data/POIList.csv")
	# sample_data.count() == 22025
	# poi_list.count() == 4

	# 0. Cleanup
	df = remove_dups(sample_data, [" TimeSt", "Latitude", "Longitude"])
	poi = remove_dups(poi_list, [" Latitude", "Longitude"])
	# clean_df.count() == 19999
	# clean_poi.count == 3

	# 1. Label
	df = get_min_dist(df, poi)
	





	 # 2. Analysis
	 ## i. Calculate avg and std wrt poi
	 ## ii. Draw circles centered at POI, find radius and density


	 # 3. Model
	 ## i. map density of POI to [-10, 10]
	 ## ii. POI hypotheses


