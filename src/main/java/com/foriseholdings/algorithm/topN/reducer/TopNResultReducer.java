package com.foriseholdings.algorithm.topN.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class TopNResultReducer extends BaseReducer {
	String prods_times = null;

	protected boolean myreduce() {
		
		
		
		StringBuffer strBuf = new StringBuffer();
		for (Text text : values) {
			strBuf.append(text.toString() + ",");
		}
		Configuration conf =context.getConfiguration();
		buss_code = conf.get("buss_code");
		
//		if (strBuf.toString().endsWith(",")) {
//			prods_times = strBuf.toString().substring(0, strBuf.toString().length() - 1);
//		}
		outKey.set(key.toString());
		outValue.set(strBuf.toString() + "	" + "topN" + "	" + buss_code);
		write();
		return true;
	}
}
