package com.foriseholdings.algorithm.topN.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class TopNResultMapper extends BaseMapper {

	// calcProc
	// 21 12781_3.0
	// 25 11966_2.0,12092_2.0,11809_3.0,12090_2.0
	// 6022
	// 11812_6.0,1_90.0,4_12.0,5_6.0,12752_9.0,12475_3.0,12226_39.0,TC1512457323212_3.0

	protected boolean calcProc() {
		
		StringBuffer strBuf = new StringBuffer();
		String shopIDAndProdID = value.toString().split("\t")[0];
		//B00003@20128
		String shopSn = shopIDAndProdID.split("@")[0];
		String prodId = shopIDAndProdID.split("@")[1];
		String behaveTms = value.toString().split("\t")[1];
		strBuf.append(prodId+"_"+behaveTms);
		outKey.set("topN"+"_"+shopSn);
		outValue.set(strBuf.toString());
		write();
		return true;
	}

}
