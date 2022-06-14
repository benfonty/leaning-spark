package com.example.learningSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import scala.Tuple2;

public class LearningSparkApplication {

	public static void main(String[] args) {

		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startSpark").setMaster("local[*]");

		example3(conf);
	}

	private static void example3(SparkConf conf) {
		try (JavaSparkContext sc = new JavaSparkContext(conf)) {
			sc.textFile()
		}
	}

	private static void example2(SparkConf conf) {
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 7 September 1942");

		try (JavaSparkContext sc = new JavaSparkContext(conf)){
			JavaRDD<String> originalLogs = sc.parallelize(inputData);
			JavaPairRDD<String, String> pairs = originalLogs.mapToPair(value -> {
				String[] s = value.split(":");
				return new Tuple2(s[0], s[1]);
			});

			pairs
					.groupByKey()
					.mapValues(l -> StreamSupport.stream(l.spliterator(), false).count())
					.collect()
					.forEach(System.out::println);
			pairs
					.mapValues(v -> 1L)
					.reduceByKey((v1, v2) -> v1 + v2)
					.collect()
					.forEach(System.out::println);

			originalLogs
					.flatMap(v -> Arrays.asList(v.split(" ")).iterator())
					.filter(v -> v.length() > 1)
					.filter(v -> !v.contains(":"))
					.collect()
					.forEach(System.out::println);
		}
	}

	private static void example1(SparkConf conf) {
		List<Double> inputData = new ArrayList<>();
		inputData.add(35.5);
		inputData.add(12.49943);
		inputData.add(90.32);
		inputData.add(20.32);

		try (JavaSparkContext sc = new JavaSparkContext(conf)){
			JavaRDD<Double> myRdd = sc.parallelize(inputData);
			Tuple2<Double, Double> result = myRdd
					.map(d -> new Tuple2<>(d, d * 2))
					.reduce((d1, d2) -> new Tuple2<>(d1._1 + d2._1, d1._2 + d2._2));

			myRdd.collect() // collect all data on the computer because println is not serializable thus spark cannot send it to other nodes
					.forEach(System.out::println);
			System.out.println(result);
		}
	}
}
