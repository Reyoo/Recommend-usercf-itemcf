//package com.foriseholdings.commons.utils;
//
//import java.io.IOException;
//import java.text.DecimalFormat;
//import java.util.List;
//
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper.Context;
//
//public class CosineSimilarity {
//
//	private static DecimalFormat df = new DecimalFormat("0.00");
//
//	/**
//	 * 计算余弦相似度
//	 * 
//	 * @param key
//	 * @param value
//	 * @param context
//	 * @param cacheList
//	 *            缓存文件
//	 */
//	public  void getCosine( Text value, List<String> cacheList, Text outKey,
//			Text outValue) {
//		
//		// 行
//		try {
//			System.out.println("values------>"+value);
//			String row_matrix1 = value.toString().split("\t")[0];
//			// 列_值(数组)
//			String[] column_value_array_matrix1 = value.toString().split("\t")[1].split(",");
//
//			double denominator1 = 0;
//			// 计算左侧矩阵的空间距离
//			for (String column_value : column_value_array_matrix1) {
//				String score = column_value.split("_")[1];
//				denominator1 += Double.valueOf(score) * Double.valueOf(score);
//			}
//			denominator1 = Math.sqrt(denominator1);
//
//			for (String line : cacheList) {
//				// 右侧矩阵的行line
//				// 格式 :行 tab 列_值，列_值，列_值，列_值，列_值，列_值
//				String row_matrix2 = line.toString().split("\t")[0];
//				String[] column_value_array_matrix2 = line.toString().split("\t")[1].split(",");
//				double denominator2 = 0;
//				// 计算右侧矩阵的空间距离
//				for (String column_value : column_value_array_matrix2) {
//					String score = column_value.split("_")[1];
//					denominator2 += Double.valueOf(score) * Double.valueOf(score);
//				}
//				denominator2 = Math.sqrt(denominator2);
//
//				// 矩阵两行相乘得到的结果
//				int numerator = 0;
//				// 遍历右侧矩阵的每一行的每一列
//				for (String column_value_matrix1 : column_value_array_matrix1) {
//					String column_matrix1 = column_value_matrix1.split("_")[0];
//					String value_matrix1 = column_value_matrix1.split("_")[1];
//
//					// 遍历右侧矩阵每一行的每一列
//					for (String column_value_matrix2 : column_value_array_matrix2) {
//
//						if (column_value_matrix2.startsWith(column_matrix1 + "_")) {
//							String value_matrix2 = column_value_matrix2.split("_")[1];
//							// 将两列的值成，并相加
//							numerator += Integer.valueOf(value_matrix1) * Integer.valueOf(value_matrix2);
//						}
//					}
//				}
//
//				double cos = numerator / (denominator1 * denominator2);
//
//				if (cos == 0) {
//					continue;
//				}
//
//				
//				
//				// result 是结果矩阵中的某元素，为 行：row_matrix1 列：row_matrix2 因为右矩阵一已经转置
//				outKey.set(row_matrix1);
//				outValue.set(row_matrix2 + "_" + df.format(cos));
//				// 输出格式key :行 value :列_值
////				context.write(outKey, outValue);
//				
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//	}
//
//}
