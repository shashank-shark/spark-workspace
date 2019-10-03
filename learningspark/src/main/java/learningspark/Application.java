package learningspark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import java.util.Properties;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

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
		
		// concat first and last column - Transformation
        df = df.withColumn("full_name", concat(df.col("last_name"), lit(", "), df.col("first_name")));

        // only the comments with bumbers
        df = df.filter(df.col("comment").rlike("\\d+"))
                .orderBy(df.col("last_name").asc());
        // display the dataset
        String dbConnectionUrl = "jdbc::postgresql://localhost/course_data";
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "postgres");
        prop.setProperty("password","password");

        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "project1", prop);
	}
}
