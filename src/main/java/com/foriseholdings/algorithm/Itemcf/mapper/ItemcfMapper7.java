package com.foriseholdings.algorithm.Itemcf.mapper;

import com.foriseholdings.common.common.BaseMapper;

/**
 * @author sunqi
 * @Desc 将基于物品推荐的矩阵转置为“ 用户 商品_分值” 格式
 */
public class ItemcfMapper7 extends BaseMapper {

	// 输入格式 : 11809 6026_0.24,6030_0.12,6036_0.03
	// 11809 为商品ID 6026 为用户ID , 0.24 为分值

	// 22
	// 20774_22.0,20752_55.0,20751_5.0,ZCX201801191007344661\20003$20003_14.0,20782_12.0,20780_12.0,20858_2.0,20748_5.0,20756_2.0,20766_5.0,20710_22.0,20754_9.0,ZCX$20540_7.0,CX201801191007344661\20003_14.0

	protected boolean calcProc() {
		String[] rowAndLine = value.toString().split("\t");
		// 矩阵的行号
		String userId = rowAndLine[0];
		String[] lines = rowAndLine[1].split(",");

		for (int i = 0; i < lines.length; i++) {
			String prodId = lines[i].split("_")[0];
			String score = lines[i].split("_")[1];
			// key:行号 value :行号_值
			outKey.set(prodId);
			outValue.set(userId + "_" + score);
			write();
		}
		return true;
	}
}
