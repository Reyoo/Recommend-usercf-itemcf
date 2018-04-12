package com.foriseholdings.algorithm.Itemcf.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class ItemcfMapper1 extends BaseMapper {
	
	
	protected boolean calcProc() {
		String[] values = value.toString().split(",");
		String itemID = values[0];
		String userID = values[1];
		String score = values[2];
//		System.out.println("itemID-->"+itemID);
//		System.out.println("userID-->"+userID);
//		System.out.println("score-->"+score);
		outKey.set(itemID);
		outValue.set(userID + "_" + score);
		write();
		return true;
	}
	
	
//	@Override
//	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
//			throws IOException, InterruptedException {
//
//		
//		String[] values = value.toString().split(",");
//		String itemID = values[0];
//		String userID = values[1];
//		String score = values[2];
//		
//		outKey.set(itemID);
//		outValue.set(userID + "_" + score);
//		super.map(key, value, context);
//	}

}