package org.home.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * org.home.spark
 *
 * @author Robert Nad
 */
public class SparkApplication {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().getOrCreate();
        String orderPath = sparkSession.conf().get("spark.home.orderPath");
        String productPath = sparkSession.conf().get("spark.home.productPath");
        int groupNumber = Integer.valueOf(sparkSession.conf().get("spark.home.groupNumber", "20"));
        String outputPath = sparkSession.conf().get("spark.home.dayRevenuePath");
        Dataset<Row> orderDS = sparkSession.read().format("json").load(orderPath);
        Dataset<Row> productDs = sparkSession.read().format("json").load(productPath);
        WoldWideDailyRevenueSummary woldWideDailyRevenueSummary = new WoldWideDailyRevenueSummary();
        woldWideDailyRevenueSummary
                .process(orderDS, productDs, groupNumber)
                .repartition(1)
                .write()
                .mode("overwrite")
                .format("csv")
                .option("header", "true")
                .save(outputPath);
    }
}
