package com.virtualpairprogrammers;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Tuples {

	public static void main(String[] args) {
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(3235);
		inputData.add(95);
		inputData.add(45);
		
		//CTRL + SHFT + O ----> Importing
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> originalItems = sc.parallelize(inputData);
		
		//Tuples - Multiple values
		JavaRDD<Tuple2<Integer, Double>> sqrtRdd = originalItems.map(value -> new Tuple2<>(value, Math.sqrt(value)) );
		
		sc.close();
		
		//Operation in a RDD crunch a dataset in a single answer we use reduce
		
		//PAIR RDDs
		//Key-Value kind of dictionaries, grouping by key
		
		
	}

}
