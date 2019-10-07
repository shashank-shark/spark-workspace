package project3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;

public class Application {
	
	public static void main(String[] args) {
		
		// create a spark instance
		SparkSession spark = new SparkSession.Builder()
				.master("local")
				.getOrCreate();
		
		Dataset<Row> durhamDf = buildDurhamParksDataFrame (spark);
		durhamDf.printSchema();
		durhamDf.show(10);
		
	}
	
	public static Dataset<Row> buildDurhamParksDataFrame (SparkSession spark) {
		
		Dataset<Row> df = spark.read().format("json").option("multiline", true)
		.load("src/main/resources/durham-parks.json");
		
		df = df.withColumn("park_id", 
				concat(
					df.col("datasetid"),
					lit("_"),
					df.col("fields.objectid"),
					lit("_Durham")
						)
				)
				.withColumn("park_name", df.col("fields.park_name"))
				.withColumn("city", lit("Durhma"))
				.withColumn("has_playground", df.col("fields.playground"))
				.withColumn("zipcode", df.col("fields.zip"))
				.withColumn("land_in_acres", df.col("fields.acres"))
				.withColumn("geoX", df.col("geometry.coordinates").getItem(0))
				.withColumn("geoY", df.col("geometry.coordinates").getItem(1));
				
	
		return df;
	}
}
