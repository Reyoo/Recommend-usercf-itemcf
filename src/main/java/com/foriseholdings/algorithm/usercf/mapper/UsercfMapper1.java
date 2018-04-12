package com.foriseholdings.algorithm.usercf.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class UsercfMapper1 extends BaseMapper {

	
	
//	calcProc
	
	protected boolean calcProc() {
		String[] values= value.toString().split(",");
		String userID =values[0];
		String itemID = values[1];
		String score = values[2];
		outKey.set(userID);
		outValue.set(itemID +"_"+score);
		write();
		return true;
	}

	
	
}