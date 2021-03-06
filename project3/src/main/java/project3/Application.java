package project3;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;


public class Application {
	
	public static void main(String[] args) {
		
		// create a spark instance
		SparkSession spark = new SparkSession.Builder()
				.master("local")
				.getOrCreate();
		
		Dataset<Row> durhamDf = buildDurhamParksDataFrame (spark);
		durhamDf.show(10);
		
		Dataset<Row> dfBuildPP = buildPPD(spark);
		dfBuildPP.show(10);
		
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
	
	public static Dataset<Row> buildPPD (SparkSession spark) {

        // populate the dataframe
        Dataset<Row> df = spark.read().format("csv").option("multiline", true)
                .option("header", true)
                .load("src/main/resources/philadelphia_recreations.csv");

        df = df.filter(
                lower(df.col("USE_"))
                .like("%park%")
        );

        df = df.withColumn("park_id",
                    concat(
                            lit("phil_"),
                            df.col("OBJECTID")
                    )
                )
        .withColumnRenamed(
                "ASSET_NAME",
                "park_name"
        )
        .withColumn(
                "city",
                lit(
                        "Philadelphia"
                )
        )
        .withColumnRenamed(
                "ADDRESS",
                "address"
        )
        .withColumn(
                "has_playground",
                lit("UNKNOWN")
        )
        .withColumnRenamed(
                "ZIPCODE",
                "zipcode"
        )
        .withColumn(
                "land_in_acres",
                df.col("ACREAGE")
        )
        .withColumn(
                "geoX",
                lit("UNKNOWN")
        )
        .withColumn(
                "geoY",
                lit("UNKNOWN")
        )
        .drop(
                df.col("ACREAGE")
        )
        .drop(
                df.col("SITE_NAME")
        )
        .drop(
                df.col("OBJECTID")
        )
        .drop(
                df.col("CHILD_OF")
        )
        .drop(
                df.col("USE_")
        )
        .drop(
                "DESCRIPTION"
        )
        .drop(
                "SQ_FEET"
        )
        .drop(
                "SITE_NAME"
        )
        .drop(
                "ALLIAS"
        )
        .drop(
                "CHRONOLOGY"
        )
        .drop(
                "NOTES"
        )
        .drop(
                "DATE_EDITED"
        )
        .drop(
                "EDITED_BY"
        )
        .drop(
                "OCCUPANT"
        )
        .drop(
                "TENANT"
        )
        .drop(
                "LABEL"
        );

        return df;
    }
}
