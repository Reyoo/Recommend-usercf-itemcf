package com.foriseholdings.algorithm.content.mapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.foriseholdings.common.common.BaseMapper;

public class ContentMapper4 extends BaseMapper {
	protected boolean calcProc() {
		try {
			// 行
			String row_matrix1 = value.toString().split("\t")[0];
			// 列_值(数组)
			String[] column_value_array_matrix1 = value.toString().split("\t")[1].split(",");
			// 每一行长度
			Map<String, String> store = new HashMap<String, String>();
			// 计算左侧矩阵的空间距离
			for (String column_value : column_value_array_matrix1) {
				String itmeID = column_value.split("_")[0];
				String score = column_value.split("_")[1];
				store.put(itmeID, score);
			}

			List<Map.Entry<String, String>> list = new ArrayList<Map.Entry<String, String>>(store.entrySet());
//			System.out.println(list.size());
			Collections.sort(list, new Comparator<Map.Entry<String, String>>() {
				// 降序排序
				public int compare(Entry<String, String> o1, Entry<String, String> o2) {
					return o2.getValue().compareTo(o1.getValue());
				}
			});
			int i = 0;
			for (Map.Entry<String, String> mapping : list) {
				if (i < 3) {
//					System.out.println(mapping.getKey() + ":" + mapping.getValue());
					// result 是结果矩阵中的某元素，坐便为 行：row_matrix1 列：row_matrix2 因为右矩阵一已经转置
					outKey.set(row_matrix1); // 用户的ID
					outValue.set(mapping.getKey() + "_" + mapping.getValue());
					// 输出格式 用户ID 商品_分值
					context.write(outKey, outValue);
				}
				i++;
			}

		} catch (Exception e) {
			logger.info(e);
			e.printStackTrace();
		}
		return true;
	}
}
