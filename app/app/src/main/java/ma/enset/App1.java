package ma.enset;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

public class App1 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TP1-Ex1-VentesParVille")
                .getOrCreate();

        String input = System.getProperty("input", "/shared/ventes.txt");
        String sep = System.getProperty("sep", ",");

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .option("delimiter", sep)
                .csv(input)
                .select(
                        col("date"),
                        col("ville"),
                        col("produit"),
                        col("prix").cast("double").alias("prix")
                );

        df.groupBy("ville")
          .agg(round(sum("prix"), 2).alias("total_ventes"))
          .orderBy(desc("total_ventes"))
          .show(false);

        spark.stop();
    }
}
