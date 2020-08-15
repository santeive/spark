package com.virtualpairprogrammers;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

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
		
		//Load a collection and turn it into a ODD
		//Is comunicating with scala RDD
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		
		//Distributes across multiple partitions and nodes
		Integer result = myRdd.reduce((value1,value2) -> value1 + value2 );
		
		JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
		
		//sqrtRdd.foreach( value -> System.out.println(value));
		sqrtRdd.collect().forEach( System.out::println );
		
		System.out.println(result);
		
		//How many elements in sqrtRdd
		//Using just map and reduce
		JavaRDD<Long> singlrIntegerRdd = sqrtRdd.map(value -> 1L);
		Long count = singlrIntegerRdd.reduce((value1, value2) -> value1 + value2);
		System.out.println(count);
		
		//Easy step
		System.out.println( sqrtRdd.count() );
		
		sc.close();
		
		//Operation in a RDD crunch a dataset in a single answer we use reduce
		
		
	}

}
