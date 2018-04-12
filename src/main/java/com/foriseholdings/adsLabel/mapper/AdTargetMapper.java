package com.foriseholdings.adsLabel.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class AdTargetMapper extends BaseMapper {

	// 输入 6 20826_1,20934_9,20932_2,20930_2
	protected boolean calcProc() {
		System.out.println(value);
		String uuid = value.toString().split("\t")[0];
		String shopId_scores = value.toString().split("\t")[1];
		
		outKey.set(uuid);
		outValue.set(shopId_scores);
		write();
		return false;
	}

}
