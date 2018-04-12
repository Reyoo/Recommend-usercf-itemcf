package com.foriseholdings.algorithm.usercf.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class UsercfMapper3 extends BaseMapper {


	/**
	 * key :1 value :1 1_0,2_3,3_-1,4_2,5_-3
	 */
	protected boolean calcProc() {

		String[] rowAndLine = value.toString().split("\t");

		// 矩阵的行号
		String row = rowAndLine[0];
		String[] lines = rowAndLine[1].split(",");

		// {"1_0","2_3","3_-1","4_2","5_-3"}
		for (int i = 0; i < lines.length; i++) {

			String column = lines[i].split("_")[0];
			String valueStr = lines[i].split("_")[1];
			// key:行号 value :行号_值
			outKey.set(column);
			outValue.set(row + "_" + valueStr);
			write();
	}
		return true;
	}
	
	
}

