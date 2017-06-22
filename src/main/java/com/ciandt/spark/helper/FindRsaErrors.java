package com.ciandt.spark.helper;

import org.apache.spark.api.java.function.Function;

@SuppressWarnings("serial")
public class FindRsaErrors implements Function<String, Boolean> {

	public Boolean call(String s) {
		return s.contains("RSA");
	}

}
