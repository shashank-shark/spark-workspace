package learningspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Application {
	public static void main(String[] args) 
	{
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV to DB")
				.master("local")
				.getOrCreate();
		
		// get data
		Dataset<Row> df = spark.read().format("csv")
		.option("header", true)
		.load("src/main/resources/resources_and_comments");
		
		df.show();
	}
}
