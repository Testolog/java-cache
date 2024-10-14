package org.home.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;

import java.util.ArrayList;
import java.util.List;

/**
 * org.home.spark
 *
 * @author Robert Nad
 */
class DayRevenue {
    String origination_country;
    String order_day;
    int day_amount;
    int day_number_orders;
    float day_revenue;
}

public class WoldWideDailyRevenueSummary {

    public Dataset<Row> aggregateOrderByDay(Dataset<Row> order) {
        return order
                .withColumn("order_day", functions.date_format(functions.date_format(order.col("order_dt"), "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd"))
                .groupBy(
                        functions.col("order_day"),
                        order.col("product_id")
                )
                .agg(
                        functions.sum(order.col("amount")).as("day_amount"),
                        functions.count("order_id").as("day_number_orders")
                )
                .select(
                        functions.col("order_day"),
                        order.col("product_id"),
                        functions.col("day_amount"),
                        functions.col("day_number_orders")// this is just additional, to understand we get mostly from single order or many small orders.
                );
    }

    // this is could looks as weird however, when we make in this way this will evenly distribute. If there is no in general resources we can split in this to sub job. just need to know how many groups we should to have. Also we can use simple function.hash
    public Dataset<Row> joinWithProduct(Dataset<Row> order, Dataset<Row> product, int numberGroups) {
        product = product
                .withColumn("rn", functions.row_number().over(Window.partitionBy(product.col("origination_country")).orderBy(product.col("name"))))
                .withColumn("joinGroups", functions.col("rn").mod(functions.lit(numberGroups)));
        List<Dataset<Row>> datasets = new ArrayList<>(numberGroups);
        for (int i = 0; i < numberGroups; i++) {
            datasets.add(
                    product.where(product.col("joinGroups").equalTo(functions.lit(i)))
                            .join(order, order.col("product_id").equalTo(product.col("product_id")), "inner")
                            .select(
                                    product.col("origination_country"),
                                    order.col("order_day"),
                                    order.col("day_amount"),
                                    order.col("day_number_orders"),
                                    order.col("day_amount").multiply(product.col("unit_price")).as("day_revenue")
                            )
            );
        }
        return datasets.subList(1, numberGroups).stream().reduce(datasets.get(0), Dataset::union);
    }

    public Dataset<Row> aggregateRevenueByCountry(Dataset<Row> aggregated) {
        return aggregated
                .groupBy(
                        aggregated.col("origination_country"),
                        aggregated.col("order_day")
                )
                .agg(
                        functions.sum(aggregated.col("day_amount")).as("day_amount"),
                        functions.sum(aggregated.col("day_number_orders")).as("day_number_orders"),
                        functions.sum(aggregated.col("day_revenue")).as("day_revenue")
                );
    }

    public Dataset<DayRevenue> process(Dataset<Row> order, Dataset<Row> product, int numberGroups) {
        return order
                .transform(this::aggregateOrderByDay)
                .transform((base) -> joinWithProduct(base, product, numberGroups))
                .transform(this::aggregateRevenueByCountry)
                .as(Encoders.bean(DayRevenue.class));

    }
}
