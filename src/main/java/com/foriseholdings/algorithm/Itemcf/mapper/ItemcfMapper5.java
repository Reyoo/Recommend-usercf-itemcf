package com.foriseholdings.algorithm.Itemcf.mapper;

import com.foriseholdings.common.common.BaseMapper;

public class ItemcfMapper5 extends BaseMapper {

	protected boolean calcProc() {
		// '/t' tab 键
		String item_matrix1 = value.toString().split("\t")[0];
		//评分数组
		String[] user_score_array_matrix1 = value.toString().split("\t")[1].split(",");
		for (String line : cacheList) {
			String item_matrix2 = line.toString().split("\t")[0];
			// logger.info("item_matrix2------" + item_matrix2); // 10162
			String[] user_score_array_matrix2 = line.toString().split("\t")[1].split(",");

			// 如果物品ID 相同
			if (item_matrix1.equals(item_matrix2)) {

				// 遍历matrix1的列 就是步骤4 的输出结果
				for (String user_score_matrix1 : user_score_array_matrix1) {
					boolean flag = false;
					String user_matrix1 = user_score_matrix1.split("_")[0];
					// logger.info("user_matrix1------"+user_matrix1); //108
					String score_matrix1 = user_score_matrix1.split("_")[1];
					// logger.info("score_matrix1------"+score_matrix1);//0.12
					// 遍历matrix2 的列
					/**
					 * 1 A_2,C_5 2 A_10,B_3 3 C_15 4 A_3,C_5 5 B_3 6 A_5,B_5
					 */
					for (String user_score_matrix2 : user_score_array_matrix2) {
						String user_matrix2 = user_score_matrix2.split("_")[0];
						// logger.info("user_matrix2------"+user_matrix2); // 第一次 22 第二次 2 第三次 37
						if (user_matrix1.equals(user_matrix2)) {
							flag = true;
							// outKey.set(user_matrix1);
							// outValue.set("null_0");
							// write();
						}
					}
					if (flag == false) {
						outKey.set(user_matrix1);
						outValue.set(item_matrix1 + "_" + score_matrix1);
						write();
					}
				}
			}
		}
		return true;
	}
}
