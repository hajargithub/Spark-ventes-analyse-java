package ma.enset;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class VentesParVilleEtAnnee {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Ventes Par Ville et Année")
                .master("spark://spark-mastergit :7077")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("delimiter", " ")
                .csv("/shared/ventes.txt")
                .toDF("date", "ville", "produit", "prix");

        df = df.withColumn("prix", col("prix").cast("float"))
                .withColumn("annee", year(to_date(col("date"), "yyyy-MM-dd")));

        Dataset<Row> result = df.groupBy("ville", "annee")
                .agg(sum("prix").alias("total_ventes"));

        result.show();
        spark.stop();
    }
}
