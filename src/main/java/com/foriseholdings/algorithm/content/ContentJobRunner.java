package com.foriseholdings.algorithm.content;

import com.foriseholdings.algorithm.content.mapper.ContentMapper1;
import com.foriseholdings.algorithm.content.mapper.ContentMapper2;
import com.foriseholdings.algorithm.content.mapper.ContentMapper3;
import com.foriseholdings.algorithm.content.mapper.ContentMapper4;
import com.foriseholdings.algorithm.content.reducer.ContentReducer1;
import com.foriseholdings.algorithm.content.reducer.ContentReducer2;
import com.foriseholdings.algorithm.content.reducer.ContentReducer3;
import com.foriseholdings.algorithm.content.reducer.ContentReducer4;
import com.foriseholdings.common.common.BaseRunner;
import com.foriseholdings.common.tools.ReturnFileFormat;
import com.foriseholdings.common.util.PropertyUtil;

public class ContentJobRunner extends BaseRunner {

	// public static String content_step1_inPath = "/Content/step1_input";
	// // 输出文件的相对路径
	// public static String content_step1_outPath = "/Content/step1_output";
	//
	// public static String content_step2_inPath = "/Content/step2_input";
	// // 输出文件的相对路径
	// public static String content_step2_outPath = "/Content/step2_output";
	// // 将step1 输出的转置矩阵作为全局缓存
	// public static String content_step2_cache =
	// "/Content/step1_output/part-r-00000";
	//
	// // 输出文件的相对路径
	// public static String content_step3_outPath = "/Content/step3_output";
	//
	// // 输出文件的相对路径
	// public static String content_step4_outPath = "/Content/step4_output";

	// String date = DefDateformat.getStringDateShort(new Date());

	public static String content_base_path = PropertyUtil.getProperty("content_base_path");

	public String content_step1_inPath = PropertyUtil.getProperty("content_step1_inPath");
	// 输出文件的相对路径
	public String content_step1_outPath = PropertyUtil.getProperty("content_step1_outPath");

	public String content_step2_inPath = PropertyUtil.getProperty("content_step2_inPath");
	// 输出文件的相对路径
	public String content_step2_outPath = PropertyUtil.getProperty("content_step2_outPath");
	// 将step1 输出的转置矩阵作为全局缓存
	public String content_step2_cache = PropertyUtil.getProperty("content_step2_cache");
	// 输出文件的相对路径
	public String content_step3_outPath = PropertyUtil.getProperty("content_step3_outPath");
	// 输出文件的相对路径
	public static String content_step4_outPath = PropertyUtil.getProperty("content_step4_outPath");
	// hdfs 地址

	@Override
	public boolean baseStart(String bus_code) {
		if(step1(bus_code)) {
			return true;
		}
		return false;
//		step2();
//		step3();
//		step4();
	}

	@Override
	public boolean step1(String bus_code) {
		String input = ReturnFileFormat.getPathAddr(content_base_path, content_step1_inPath,bus_code);
		String output = ReturnFileFormat.getPathAddr(content_base_path, content_step1_outPath,bus_code);
		stepMapper = new ContentMapper1();
		stepReducer = new ContentReducer1();
		state = runMain.runTask(stepMapper, stepReducer, "", "step1", input, output,"","content","");

		if (state == 1) {
//			System.out.println("Content_step1执行成功");
			return true;
		}
		return false;
	}

	/**
	 * 第二步： 利用评分矩阵，构建物品与物品的相似度矩阵 输入:步骤1的输出 缓存：步骤1的输出 输出:物品ID(行)-------------物品ID(列)
	 * ----------------相似度
	 */

	@Override
	public boolean step2(String bus_code) {

		String input = ReturnFileFormat.getPathAddr(content_base_path, content_step2_inPath,"");
		String output = ReturnFileFormat.getPathAddr(content_base_path, content_step2_outPath,"");
		String cache = ReturnFileFormat.getPathAddr(content_base_path, content_step2_cache,"");
		stepMapper = new ContentMapper2();
		stepReducer = new ContentReducer2();
		runMain.runTask(stepMapper, stepReducer, cache, "step2", input, output,"","content","");
		return true;
	}

	/**
	 * 第三步： 将评分矩阵转置 输入:步骤1 的输出 输出 :用户ID(行)
	 * ------------物品ID(ID)列---------------------分值
	 */

	@Override
	public boolean step3(String bus_code) {

		String input = ReturnFileFormat.getPathAddr(content_base_path, content_step1_inPath,"");
		String output = ReturnFileFormat.getPathAddr(content_base_path, content_step3_outPath,"");
		String cache = ReturnFileFormat.getPathAddr(content_base_path, content_step2_outPath,"");
		cache += "part-r-00000";

		stepMapper = new ContentMapper3();
		stepReducer = new ContentReducer3();
		runMain.runTask(stepMapper, stepReducer, cache, "step3", input, output,"","content","");
		return true;
	}

	/**
	 * 第四步 物品与物品相似度矩阵 X 评分矩阵 (经过步骤3转置) 输入步骤2的输出 缓存:步骤3的输出 输出:物品ID(行)
	 * ----------用户ID(列)---------分值
	 */
	public boolean step4(String bus_code) {
		String input = ReturnFileFormat.getPathAddr(content_base_path, content_step3_outPath,"");
		String output = ReturnFileFormat.getPathAddr(content_base_path, content_step4_outPath,"");
		
//		content_step3_outPath += "part-r-00000";
		stepMapper = new ContentMapper4();
		stepReducer = new ContentReducer4();
		runMain.runTask(stepMapper, stepReducer, "", "step4", input, output,"","content","");
		return true;
	}
}