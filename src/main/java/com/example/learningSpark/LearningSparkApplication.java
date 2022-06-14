package com.example.learningSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LearningSparkApplication {

	public static void main(String[] args) {
		List<Double> inputData = new ArrayList<>();
		inputData.add(35.5);
		inputData.add(12.49943);
		inputData.add(90.32);
		inputData.add(20.32);


		SparkConf conf = new SparkConf().setAppName("startSpark").setMaster("local[*]");

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
