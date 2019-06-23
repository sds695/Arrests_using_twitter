from pyspark import SparkContext
from pyspark import SparkConf
def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = C_TRACT.value
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
	match = index.intersection((p.x, p.y, p.x, p.y))
	for idx in match:
		if zones.geometry[idx].contains(p):
			return zones.GEOID_Data[idx]
		else:
			return None

def extractTweet(partitionId, rows):
#     if partitionId==0:
#         next(rows)
    text_file1 = open("drug_illegal.txt", "r")
    keyword_list1 = text_file1.read().split('\n')
    text_file1.close()
    text_file2 = open("drug_sched2.txt", "r")
    keyword_list2 = text_file2.read().split('\n')
    text_file2.close()
    keyword_list = keyword_list1 + keyword_list2
    import csv
    reader = csv.reader(rows,delimiter='|')
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index, zones = createIndex('census_data.shp')
    #neighborhoods = gpd.read_file('500cities_tracts.geojson')
    for field in reader:
        flag = 0
        for keyword in keyword_list:
            keyword_with_space = " "+keyword+" "
            if(keyword_with_space in " "+field[5]+" "):
                #yield((field[1],field[2]),1)
                try:
                    p = geom.Point(proj(float(field[2]), float(field[1])))
                    zoneN = findZone(p, index, zones)
                    if(zoneN is not None):# and (neighborhoods['plctrpop10'][zoneN]):
                        yield(zoneN,1)
                        #yield((neighborhoods['plctract10'][zoneN],neighborhoods['plctrpop10'][zoneN]),1)   
#                     if (zoneB is not None):
#                         import geopandas as gpd
#                         neighborhoods = gpd.read_file('500cities_tracts.geojson')
#                         yield (neighborhoods['plctract10'][zoneB], 1)
                except:
                    pass
def main(sc):
	import sys
	import csv
	filename = sys.argv[1]
	tweets = sc.textFile(filename)
	tweet_list = tweets.mapPartitionsWithIndex(extractTweet).reduceByKey(lambda x,y:x+y).sortByKey().cache()
	with open('final_project_output.csv','w') as out:
		csv_out=csv.writer(out)
		csv_out.writerow(['census_tract','count'])
		for row in tweet_list.collect():
			csv_out.writerow(row)
if __name__ == "__main__":
	#conf = (SparkConf().setAppName("FinalProject").set("spark.shuffle.service.enabled", "false").set("spark.dynamicAllocation.enabled", "false"))
	sc = SparkContext()
	import geopandas as gpd
	import fiona.crs
	zones = gpd.read_file('census_data.shp').to_crs(fiona.crs.from_epsg(2263))
	C_TRACT = sc.broadcast(zones)
# Execute the main function
	main(sc)

