package com.foriseholdings.algorithm.usercf.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class UsercfMapper4 extends BaseMapper {


	/**
	 * @param key
	 *            ：行号
	 * @param value
	 *            行 tab 列_值
	 */
	
	
	protected boolean calcProc() {
		
//		haveCache();
		try {
			// 行
			// row_matrix1 是用户
			String row_matrix1 = value.toString().split("\t")[0];
			// 列_值(数组)
			// 列_值 是商品和分值
			String[] column_value_array_matrix1 = value.toString().split("\t")[1].split(",");
			// cacheList 商品_用户_行为
			for (String line : cacheList) {
				// 右侧矩阵的行line
				// 列_值(数组)
				String row_matrix2 = line.toString().split("\t")[0];
				String[] column_value_array_matrix2 = line.toString().split("\t")[1].split(",");

				double result = 0;
				// 遍历左矩阵的第一行的每一列
				for (String column_value_matrix1 : column_value_array_matrix1) {
					// 用户相似度矩阵
					String column_matrix1 = column_value_matrix1.split("_")[0];
					String value_matrix1 = column_value_matrix1.split("_")[1];

					// 遍历右矩阵每一行的每一列
					for (String column_value_matrix2 : column_value_array_matrix2) {

						if (column_value_matrix2.startsWith(column_matrix1 + "_")) {
							String value_matrix2 = column_value_matrix2.split("_")[1];
							// 将两列的值成，并累加
							result += Double.valueOf(value_matrix1) * Double.valueOf(value_matrix2);
						}
					}

				}

				if (result == 0) {
					continue;
				}
				// result是结果矩阵中的某元素 ，坐标为 行 :row_matrix1 ,列: row_matrix2(因为右矩阵已经转置)
				outKey.set(row_matrix1);
				outValue.set(row_matrix2 + "_" + df.format(result));
				// 输出格式key :行 value ：列_值
//				context.write(outKey, outValue);
				write();

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;

	}

}
