package com.foriseholdings.algorithm.content.reducer;

import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class ContentReducer1 extends BaseReducer {


	//key :列号  values
	protected boolean myreduce(){
		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			//text :行号_值
			sb.append(text + ",");
		}

		String line = null;
		if (sb.toString().endsWith(",")) {
			line = sb.substring(0, sb.length() - 1);
		}

		outKey.set(key);
		outValue.set(line);
//		context.write(outKey, outValue);
		write();
		return true;

	}

}