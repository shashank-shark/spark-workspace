package project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JSONLineParser {
	
	public void parseJSONLines () {
		
		SparkSession spark = new SparkSession.Builder()
				.appName("From JSON to Dataframe")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> df = spark.read().format("json")
				.load("src/main/resources/simple.json");
		
		df.show (5, 150);
		df.printSchema();
	}
}
