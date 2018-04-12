package com.foriseholdings.algorithm.Itemcf.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class ItemcfReducer5 extends BaseReducer {

//	private Text outKey = new Text();
//	private Text outValue = new Text();
	
	
	protected boolean myreduce() {
	
		StringBuilder sBuilder = new StringBuilder();
		for(Text value :values){
			sBuilder.append(value +",");
			
		}
		
		Configuration conf =context.getConfiguration();
		buss_code = conf.get("buss_code");
		type = conf.get("type");
		String line = null;
		if(sBuilder.toString().endsWith(",")){
			line = sBuilder.substring(0,sBuilder.length() -1);
		}
		outKey.set(key);
		outValue.set(line +"	" +"type" +"	"+buss_code);
//		context.write(outKey, outValue);
		write();
		return true;
			
		}

	}
	
