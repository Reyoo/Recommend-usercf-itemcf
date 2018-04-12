
package com.foriseholdings.algorithm.topN.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class TopNMapper extends BaseMapper {

	// calcProc
	// 21 12781_3.0
	// 25 11966_2.0,12092_2.0,11809_3.0,12090_2.0
	// 6022
	// 11812_6.0,1_90.0,4_12.0,5_6.0,12752_9.0,12475_3.0,12226_39.0,TC1512457323212_3.0

	protected boolean calcProc() {
		// value =
		// System.out.println(key.toString());
		// 取出所有商品 ,格式 商品ID_分数， 只取商品ID
		String shopIdAndScoreStr = value.toString().split("\t")[1];
		String shopId = null;
		String num = "1";
		if (!shopIdAndScoreStr.contains(",")) {
			shopId = shopIdAndScoreStr.split("_")[0];
			outKey.set(shopId);
			outValue.set(num);
			write();
		} else {
			String shopIdsAndScore[] = shopIdAndScoreStr.split(",");
			for (String shopIds : shopIdsAndScore) {
				shopId = shopIds.split("_")[0];
				outKey.set(shopId);
				outValue.set(num);
				write();
			}
		}
		return true;
	}

}