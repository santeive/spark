package com.virtualpairprogrammers;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Pairs {

	public static void main(String[] args) {
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 september 0734");
		inputData.add("ERROR: Tuesday 4 september 6784");
		inputData.add("FATAL: Friday 4 september 0331");
		inputData.add("FATAL: Sunday 4 september 1734");
		inputData.add("WARN: Saturday 4 september 6734");
		
		//CTRL + SHFT + O ----> Importing
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		sc.parallelize(inputData).mapToPair(rawValue -> new Tuple2<>(rawValue.split(":")[0], 1L ))
			.reduceByKey((value1, value2) -> value1 + value2)
			.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + "instances"));
		
		
		
		//We want to transform
		/*JavaPairRDD<String, Long> pairRdd = originalMessages.mapToPair( rawValue -> { 
			
			String[] columns = rawValue.split(":");
			String level = columns[0];
			
			return new Tuple2<>(level, 1L);
		} );
		
		
		JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey((value1, value2) -> value1 + value2);
		sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		*/
		
		sc.close();
		
	}

}
