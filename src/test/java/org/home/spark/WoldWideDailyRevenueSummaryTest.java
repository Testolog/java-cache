package org.home.spark;

import com.google.common.collect.Lists;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * org.home.spark
 *
 * @author Robert Nad
 */
class WoldWideDailyRevenueSummaryTest {
    SparkSession sparkSession;
    WoldWideDailyRevenueSummary woldWideDailyRevenueSummary = new WoldWideDailyRevenueSummary();
    StructType ordersStruct = new StructType()
            .add("order_id", DataTypes.IntegerType, false, Metadata.empty())
            .add("customer_id", DataTypes.IntegerType, false, Metadata.empty())
            .add("product_id", DataTypes.IntegerType, false, Metadata.empty())
            .add("amount", DataTypes.IntegerType, false, Metadata.empty())
            .add("order_dt", DataTypes.StringType, false, Metadata.empty());
    StructType productStruct = new StructType()
            .add("product_id", DataTypes.IntegerType, false, Metadata.empty())
            .add("category", DataTypes.StringType, false, Metadata.empty())
            .add("name", DataTypes.StringType, false, Metadata.empty())
            .add("origination_country", DataTypes.StringType, false, Metadata.empty())
            .add("unit_price", DataTypes.DoubleType, false, Metadata.empty());

    @BeforeEach
    void setUp() {
        sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
    }

    @Test
    void testCompileJob() {
        Dataset<Row> orderDS = sparkSession.createDataFrame(Lists.newArrayList(), ordersStruct);
        Dataset<Row> productDS = sparkSession.createDataFrame(Lists.newArrayList(), productStruct);
        Dataset<DayRevenue> res = woldWideDailyRevenueSummary.process(orderDS, productDS, 10);
        try {
            res.show();
            Assertions.assertTrue(true);
        } catch (Exception e) {
            Assertions.fail(e);
        }
    }

    @Test
    void testDayAggregation() {
        List<Row> rows = Lists.newArrayList();
        rows.add(RowFactory.create(1, 1, 1, 2, "2022-05-05 10:01:01"));
        rows.add(RowFactory.create(1, 2, 1, 2, "2022-05-05 10:01:01"));
        rows.add(RowFactory.create(2, 1, 2, 3, "2022-05-06 10:01:01"));
        rows.add(RowFactory.create(2, 2, 2, 3, "2022-05-06 10:01:01"));
        rows.add(RowFactory.create(3, 2, 3, 7, "2022-05-07 10:01:01"));
        rows.add(RowFactory.create(3, 3, 3, 7, "2022-05-07 10:01:01"));
        Dataset<Row> orderDS = sparkSession.createDataFrame(rows, ordersStruct);
        Dataset<Row> res = woldWideDailyRevenueSummary.aggregateOrderByDay(orderDS);
        Dataset<Row> expect = sparkSession.createDataFrame(
                Lists.newArrayList(
                        RowFactory.create("2022-05-05", 1, 4, 2),
                        RowFactory.create("2022-05-06", 2, 6, 2),
                        RowFactory.create("2022-05-07", 3, 14, 2)

                ),
                new StructType(new StructField[]{
                        new StructField("order_day", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("product_id", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("day_amount", DataTypes.IntegerType, false, Metadata.empty()),
                        new StructField("day_number_orders", DataTypes.IntegerType, false, Metadata.empty())
                })
        );
        Assertions.assertEquals(res.exceptAll(expect).count(), 0);
    }


    @Test
    void testJoinWithProduct() {
        StructType processed = new StructType(new StructField[]{
                new StructField("order_day", DataTypes.StringType, false, Metadata.empty()),
                new StructField("product_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("day_amount", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("day_number_orders", DataTypes.IntegerType, false, Metadata.empty())
        });
        List<Row> orders = Lists.newArrayList(
                RowFactory.create("2022-05-05", 1, 4, 2),
                RowFactory.create("2022-05-06", 2, 6, 2),
                RowFactory.create("2022-05-07", 3, 14, 2)

        );
        List<Row> products = Lists.newArrayList(
                RowFactory.create(1, "books", "Effective Java", "USA", 2.0),
                RowFactory.create(2, "vegetables", "apples", "UA", 2.0),
                RowFactory.create(3, "non-alcohol drinks", "orange juice", "UA", 2.0)
        );
        Dataset<Row> orderDS = sparkSession.createDataFrame(orders, processed);
        Dataset<Row> productDS = sparkSession.createDataFrame(products, productStruct);
        Dataset<Row> res = woldWideDailyRevenueSummary.joinWithProduct(orderDS, productDS, 2);
        Dataset<Row> expect = sparkSession.createDataFrame(Lists.newArrayList(
                        RowFactory.create("UA", "2022-05-07", 14, 2, 28.0),
                        RowFactory.create("UA", "2022-05-06", 6, 2, 12.0),
                        RowFactory.create("USA", "2022-05-05", 4, 2, 8.0)
                ),
                new StructType(
                        new StructField[]{
                                new StructField("origination_country", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("order_day", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("day_amount", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("day_number_orders", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("day_revenue", DataTypes.DoubleType, false, Metadata.empty())
                        }
                ));
        Assertions.assertEquals(res.exceptAll(expect).count(), 0);
    }

    @Test
    void testAggregationByCountTry() {
        Dataset<Row> aggregatedByDay = sparkSession.createDataFrame(Lists.newArrayList(
                        RowFactory.create("UA", "2022-05-07", 14, 2, 28.0),
                        RowFactory.create("UA", "2022-05-07", 14, 2, 28.0),
                        RowFactory.create("UA", "2022-05-06", 6, 2, 12.0),
                        RowFactory.create("UA", "2022-05-06", 6, 2, 12.0),
                        RowFactory.create("USA", "2022-05-05", 4, 2, 8.0),
                        RowFactory.create("USA", "2022-05-05", 4, 2, 8.0)
                ),
                new StructType(
                        new StructField[]{
                                new StructField("origination_country", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("order_day", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("day_amount", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("day_number_orders", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("day_revenue", DataTypes.DoubleType, false, Metadata.empty())
                        }
                ));
        Dataset<Row> res = woldWideDailyRevenueSummary.aggregateRevenueByCountry(aggregatedByDay);
        Dataset<Row> expect = sparkSession.createDataFrame(Lists.newArrayList(
                        RowFactory.create("UA", "2022-05-07", 28, 4, 56.0),
                        RowFactory.create("UA", "2022-05-06", 12, 4, 24.0),
                        RowFactory.create("USA", "2022-05-05", 8, 4, 16.0)
                ),
                new StructType(
                        new StructField[]{
                                new StructField("origination_country", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("order_day", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("day_amount", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("day_number_orders", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("day_revenue", DataTypes.DoubleType, false, Metadata.empty())
                        }
                ));
        Assertions.assertEquals(res.exceptAll(expect).count(), 0);
    }

    @Test
    void testWholeCalculation() {
        List<Row> rows = Lists.newArrayList();
        rows.add(RowFactory.create(1, 1, 1, 2, "2022-05-05 10:01:01"));
        rows.add(RowFactory.create(1, 2, 1, 2, "2022-05-05 10:01:01"));
        rows.add(RowFactory.create(2, 1, 2, 3, "2022-05-06 10:01:01"));
        rows.add(RowFactory.create(2, 2, 2, 3, "2022-05-06 10:01:01"));
        rows.add(RowFactory.create(3, 2, 3, 7, "2022-05-07 10:01:01"));
        rows.add(RowFactory.create(3, 3, 3, 7, "2022-05-07 10:01:01"));
        List<Row> products = Lists.newArrayList(
                RowFactory.create(1, "books", "Effective Java", "USA", 2.0),
                RowFactory.create(2, "vegetables", "apples", "UA", 2.0),
                RowFactory.create(3, "non-alcohol drinks", "orange juice", "UA", 2.0)
        );
        Dataset<Row> orderDS = sparkSession.createDataFrame(rows, ordersStruct);
        Dataset<Row> productDS = sparkSession.createDataFrame(products, productStruct);
        Dataset<DayRevenue> res = woldWideDailyRevenueSummary.process(orderDS, productDS, 10);
        //easy to copy/paste from prev test, instead a make by correct with create new instances of expected class
        Dataset<DayRevenue> expect = sparkSession.createDataFrame(Lists.newArrayList(
                        RowFactory.create("UA", "2022-05-07", 14, 2, 28.0),
                        RowFactory.create("UA", "2022-05-06", 6, 2, 12.0),
                        RowFactory.create("USA", "2022-05-05", 4, 2, 8.0)
                ),
                new StructType(
                        new StructField[]{
                                new StructField("origination_country", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("order_day", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("day_amount", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("day_number_orders", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("day_revenue", DataTypes.DoubleType, false, Metadata.empty())
                        }
                )).as(Encoders.bean(DayRevenue.class));

        Assertions.assertEquals(res.exceptAll(expect).count(), 0);
    }
}