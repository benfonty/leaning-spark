package com.example.learningSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

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
			System.out.println(myRdd.reduce((d1, d2) -> d1 + d2));
		}
	}
}
