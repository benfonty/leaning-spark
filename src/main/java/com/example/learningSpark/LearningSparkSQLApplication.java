package com.example.learningSpark;

import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class LearningSparkSQLApplication {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        try (SparkSession session = SparkSession.builder().appName("testingSQL").master("local[*]").getOrCreate()) {
            inMemory(session);
        }
    }

    private static void inMemory(SparkSession session) {
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("the name", 2017));
        rows.add(RowFactory.create("the name2", 2018));
        StructField fields[] = {
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("year", DataTypes.IntegerType, false, Metadata.empty())
        };
        Dataset<Row> dataset = session.createDataFrame(rows, new StructType(fields));
        dataset.show();
    }

    private static void filtering(SparkSession session) {
        Dataset<Row> dataset = session.read().option("header", true).csv("src/main/resources/exams/students.csv");

        // filtering
        dataset.filter("subject = 'Modern Art' and year >= 2007").show();
        dataset.filter((FilterFunction<Row>) r -> r.getAs("subject").equals("Modern Art") && Integer.parseInt(r.getAs("year")) >= 2007).show();
        dataset.filter(col("subject").eqNullSafe("Modern Art")
                .and(col("year").geq(2007))
        ).show();
        dataset.createOrReplaceTempView("toto");
        session.sql("select year, avg(score) from toto group by year order by year").show();
    }


}
