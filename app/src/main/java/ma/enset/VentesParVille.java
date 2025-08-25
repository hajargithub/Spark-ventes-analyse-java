package ma.enset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class VentesParVille {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Ventes Par Ville")
                .master("spark://spark-master:7077")  // Cluster Spark Docker
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("delimiter", " ")
                .csv("/shared/ventes.txt")
                .toDF("date", "ville", "produit", "prix");

        df = df.withColumn("prix", col("prix").cast("float"));

        Dataset<Row> result = df.groupBy("ville")
                .agg(sum("prix").alias("total_ventes"));

        result.show();
        spark.stop();
    }
}
