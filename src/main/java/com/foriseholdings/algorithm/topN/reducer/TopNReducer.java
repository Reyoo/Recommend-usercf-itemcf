package com.foriseholdings.algorithm.topN.reducer;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class TopNReducer extends BaseReducer {
	private IntWritable result1s = new IntWritable();

	List<String>  proId = new ArrayList<String>();
	protected boolean myreduce() {
		int sum = 0;
		for (Text text : values) {
			// text :行号_值
			sum+=Integer.parseInt(text.toString());

		}
		
		String prodId = key.toString();
		proId.add(key.toString());
		outKey.set(prodId);
		outValue.set(String.valueOf(sum));
		write();
		return true;

	}
}
