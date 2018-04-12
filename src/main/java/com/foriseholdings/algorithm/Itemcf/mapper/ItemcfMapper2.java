package com.foriseholdings.algorithm.Itemcf.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class ItemcfMapper2   extends BaseMapper {
	

	protected boolean calcProc() {
//		haveCache();
		getCosine();
		return true;
	}
	
}
