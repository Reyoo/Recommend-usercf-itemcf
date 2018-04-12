package com.foriseholdings.algorithm.content.reducer;

import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class ContentReducer2 extends BaseReducer {
	
	
	protected boolean myreduce() {
		StringBuffer sb = new StringBuffer();
		for (Text text : values) {
			sb.append(text + ",");
		}
		
		String result = null;
		if(sb.toString().endsWith(",")){
			result= sb.substring(0,sb.length()-1);
		}
		//outKey :
		outKey.set(key);
		outValue.set(result);
//		context.write(outKey, outValue);
		write();
		return true;
	}
	
}
