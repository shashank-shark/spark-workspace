package project2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {
	
	public void printSchema () {
		
		SparkSession spark = new SparkSession.Builder()
				.appName("Complex CSV to Dataset")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> df = spark.read().format("csv")
				.option("header", true)
				.option("multiline", true)
				.option("sep",";")
				.option("quote", "^")
				.option("dataframe", "M/d/y")
				.option("inferSchema", true)
				.load("src/main/resources/amazonProducts.txt");
		
		System.out.println("The dataframe content");
		
		df.show(7, 90);
		
		System.out.println("Dataframe's Schema : ");
		df.printSchema();
	}

}
