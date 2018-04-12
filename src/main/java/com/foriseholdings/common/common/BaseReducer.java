package com.foriseholdings.common.common;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class BaseReducer extends Reducer<Text, Text, Text, Text> {

	protected Text outKey = new Text();
	protected Text outValue = new Text();
	protected Iterable<Text> itValues;
	public static Logger logger;

	protected Iterable<Text> values;
	protected Text key;
	protected Context context;

	static public String buss_code;
	static public String type; // 算法类型
	// public String buss_code;

	// public static String getBuss_code() {
	// return buss_code;
	// }
	//
	// public static void setBuss_code(String buss_code) {
	// BaseReducer.buss_code = buss_code;
	// }

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		this.values = values;
		this.key = key;
		this.context = context;

		if (!myreduce()) {
			return;
		}
	}

	protected boolean myreduce() {
		return true;

	}

	protected void write() {
		try {
			context.write(outKey, outValue);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}
