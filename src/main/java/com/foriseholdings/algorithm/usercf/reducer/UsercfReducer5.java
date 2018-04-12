package com.foriseholdings.algorithm.usercf.reducer;

import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class UsercfReducer5 extends BaseReducer {

	
	protected boolean myreduce() {
		StringBuilder sBuilder = new StringBuilder();
		for(Text value :values){
			sBuilder.append(value +",");
			
		}
		String line = null;
		if(sBuilder.toString().endsWith(",")){
			line = sBuilder.substring(0,sBuilder.length() -1);
		}
			
		outKey.set(key);
		outValue.set(line);
		write();
		return true;

	}
}