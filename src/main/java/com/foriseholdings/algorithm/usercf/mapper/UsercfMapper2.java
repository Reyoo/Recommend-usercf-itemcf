package com.foriseholdings.algorithm.usercf.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class UsercfMapper2 extends BaseMapper {

//	boolean haveCache = true;
	
	protected boolean calcProc() {
//		haveCache();
		getCosine();
		return true;
	}
	
}