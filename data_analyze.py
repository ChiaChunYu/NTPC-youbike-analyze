import happybase
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def get_bike_data(table_name):
    connection = happybase.Connection('localhost')  
    table = connection.table(table_name)
    bike_data = []

    for key, data in table.scan():
        info_value = json.loads(data[b'data:info'].decode('utf-8'))
        bemp = info_value["bemp"]
        sna = info_value["sna"]
        sbi = int(info_value["sbi"])
        bike_data.append((sna, sbi, bemp))

    connection.close()
    return bike_data

def cal_top10_busiest(bike_data,spark):
    columns = ["sna", "sbi"]
    bike_df = spark.createDataFrame(bike_data, columns)

    window_spec = Window().partitionBy("sna").orderBy("sna")
    total_change_df = bike_df.withColumn("total_change", F.abs(F.col("sbi") - F.lag("sbi").over(window_spec)))

    popularity_df = total_change_df.groupBy("sna").agg(F.sum("total_change").alias("total_change"))

    top_stations = popularity_df.orderBy("total_change", ascending=False).limit(10)
    top10_busiest = top_stations.collect()
    
    return top10_busiest

def cal_top10_avg_bemp(bike_data, spark):
    columns = ["sna", "bemp"]
    bike_df = spark.createDataFrame(bike_data, columns)

    avg_bemp_df = bike_df.groupBy("sna").agg(F.avg("bemp").alias("avg_bemp"))
    
    top10_avg_bemp_df = avg_bemp_df.orderBy("avg_bemp", ascending=False).limit(10)
    
    top10_avg_bemp = top10_avg_bemp_df.collect()

    return top10_avg_bemp



def main():
    table_name = 'Youbike_data'  
    spark = SparkSession.builder.appName("BikePopularity").getOrCreate()
    
    bike_data = get_bike_data(table_name)
    top10_busiest = cal_top10_busiest(bike_data,spark)
    top10_avg_bemp = cal_top10_avg_bemp(bike_data, spark)

    # print the top 10 busiest_station
    i=1
    for row in top10_busiest:
        sna = row["sna"]
        total_change = row["total_change"]
        print(f"Top{i} busiest station: {sna}, Total Change: {total_change}")
        i = i + 1
    
    print("------------------------------------------------------------------------")
    #print the top10 avg_bemp station
    i = 1
    for row in top10_avg_bemp:
        sna = row["sna"]
        avg_bemp = row["avg_bemp"]
        print(f"Top{i} avg_bemp station: {sna}, Avg BEMP: {avg_bemp}")
        i += 1

    spark.stop()

if __name__ == "__main__":
    main()
