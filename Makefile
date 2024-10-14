.PHONY: package docker-run

clean:
	mvn clean

test:
	mvn test

package:
	mvn package -Dmaven.test.skip

docker-run: package
	docker run -t --rm -v $(PWD):/opt/spark/work-dir/  apache/spark /opt/spark/bin/spark-submit --class org.home.spark.SparkApplication \
	--conf spark.home.orderPath=/opt/spark/work-dir/order.json \
	--conf spark.home.productPath=/opt/spark/work-dir/product.json \
	--conf spark.home.groupNumber=10 \
	--conf spark.home.dayRevenuePath=/opt/spark/work-dir/daily_revenue \
	./target/robert_home.jar
