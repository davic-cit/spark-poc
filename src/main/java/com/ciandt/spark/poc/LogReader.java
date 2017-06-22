package com.ciandt.spark.poc;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

import com.ciandt.spark.helper.FindRsaErrors;

public class LogReader
{
    public static void main( String[] args )
    {
       SparkConf sc = new SparkConf().setAppName("Log-Reader").setMaster("local[2]");
       JavaSparkContext jsc = new JavaSparkContext(sc);
       JavaRDD<String> logFile = jsc.textFile("input/error.log");
       
       // Lambda(Java 8) example.
       JavaRDD<String> junErrors = logFile.filter(new Function<String, Boolean>() {
    	   public Boolean call(String f) { return f.contains("Jun 18"); }
       }).cache();
       
       long allJunErrors = junErrors.count();
       
       // Example declaring the function as a class that implements Function.
       long mailErrors = junErrors.filter(new FindRsaErrors()).count();
       
       System.out.printf("%d Total errors in June. %d of them are related to RSA errors", allJunErrors, mailErrors);
       
       junErrors.unpersist();
       jsc.close();
       
    }
}
