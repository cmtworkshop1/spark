import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.log4j.*;

public class JavaSparkDataFrameExample {
	
	public static void main(String[] args) {
		
		Logger.getLogger("org").setLevel(Level.ERROR);
		
	    SparkSession spark = SparkSession
	    	      .builder()
	    	      .appName("JavaWordCount")
	    	      .master("local[*]")
	    	      .getOrCreate();
	    
	    try {
			runBasicDataFrameExample(spark);
		} catch (AnalysisException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
	    Dataset<Row> df = spark.read().json("D:/data/people.json");

	    df.show();
	    // Print the schema in a tree format
	    df.printSchema();

	    // Select only the "name" column
	    df.select("name").show();

	    // Select everybody, but increment the age by 1
	    df.select(col("name"), col("age").plus(1)).show();
	    // +-------+---------+
	    // |   name|(age + 1)|
	    // +-------+---------+
	    // |Michael|     null|
	    // |   Andy|       31|
	    // | Justin|       20|
	    // +-------+---------+

	    // Select people older than 21
	    df.filter(col("age").gt(21)).show();
	    // +---+----+
	    // |age|name|
	    // +---+----+
	    // | 30|Andy|
	    // +---+----+

	    // Count people by age
	    df.groupBy("age").count().show();
	    // +----+-----+
	    // | age|count|
	    // +----+-----+
	    // |  19|    1|
	    // |null|    1|
	    // |  30|    1|
	    // +----+-----+

	    // Register the DataFrame as a SQL temporary view
	    df.createOrReplaceTempView("people");

	    Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
	    sqlDF.show();
	    // +----+-------+
	    // | age|   name|
	    // +----+-------+
	    // |null|Michael|
	    // |  30|   Andy|
	    // |  19| Justin|
	    // +----+-------+

	    df.createGlobalTempView("people");

	    // Global temporary view is tied to a system preserved database `global_temp`
	    spark.sql("SELECT * FROM global_temp.people").show();
	    // +----+-------+
	    // | age|   name|
	    // +----+-------+
	    // |null|Michael|
	    // |  30|   Andy|
	    // |  19| Justin|
	    // +----+-------+

	    // Global temporary view is cross-session
	    spark.newSession().sql("SELECT * FROM global_temp.people").show();
	    // +----+-------+
	    // | age|   name|
	    // +----+-------+
	    // |null|Michael|
	    // |  30|   Andy|
	    // |  19| Justin|
	    // +----+-------+
	    // $example off:global_temp_view$
	  }
	

}
