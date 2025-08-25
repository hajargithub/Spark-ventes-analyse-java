package ma.enset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class App {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("AnalyseVentes")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("delimiter", " ")
                .csv("ventes.txt")
                .toDF("date", "ville", "produit", "prix");

        df = df.withColumn("prix", col("prix").cast("float"));

        Dataset<Row> totalParVille = df.groupBy("ville")
                .agg(sum("prix").alias("total_ventes"));

        totalParVille.show();
    }
}