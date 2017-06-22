package com.ciandt.spark.poc;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

public class StreamInterpreter {
	SparkConf sc = new SparkConf().setMaster("local[2]").setAppName("StreamInterpreter");
	JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(2000));
	
	// Use netcat to create a local stream of data @ localhost:9999.
	JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
	JavaDStream<String> words = lines.flatMap(
		new FlatMapFunction<String, String>() {
		    @Override public Iterator<String> call(String x) {
		        return Arrays.asList(x.split(" ")).iterator();
		    }
		});

	JavaPairDStream<String, Integer> pairs = words.mapToPair(
		new PairFunction<String, String, Integer>() {
		    @Override public Tuple2<String, Integer> call(String s) {
			    return new Tuple2<>(s, 1);
			}
		});

	JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
	    new Function2<Integer, Integer, Integer>() {
		    @Override public Integer call(Integer i1, Integer i2) {
		        return i1 + i2;
		    }
		});
	
	wordCounts.print();
	
	jsc.start();
	jsc.awaitTermination();
	
}
