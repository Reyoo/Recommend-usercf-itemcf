package com.foriseholdings.algorithm.topN;

import com.foriseholdings.algorithm.topN.mapper.TopNMapper;
import com.foriseholdings.algorithm.topN.mapper.TopNResultMapper;
import com.foriseholdings.algorithm.topN.reducer.TopNReducer;
import com.foriseholdings.algorithm.topN.reducer.TopNResultReducer;
import com.foriseholdings.common.common.BaseRunner;
import com.foriseholdings.common.tools.ReturnFileFormat;
import com.foriseholdings.common.util.PropertyUtil;
import com.foriseholdings.write2mysql.action.ClearDataTable;
import com.foriseholdings.write2mysql.action.MysqlShopSnAndUserDBOutput;

public class TopNJobRunner extends BaseRunner {

	// String userCF_s1_inPath = "/UserCF_demo/step1_input/new2.txt";
	// String userCF_s2_inPath= "/userCF/step1_output/";
	// String userCF_s1_outPath = "/userCF/step1_output/";
	// 取每一行的数据 判断是否有, 如果有 则根据_拆分,如果没有 写入reduce
	//
	// 21 12781_3.0
	// 25 11966_2.0,12092_2.0,11809_3.0,12090_2.0
	// 6022
	// 11812_6.0,1_90.0,4_12.0,5_6.0,12752_9.0,12475_3.0,12226_39.0,TC1512457323212_3.0

	String base_path = PropertyUtil.getProperty("base_path");
	String parse_outPath = PropertyUtil.getProperty("parse_outPath");
	String topN_output = PropertyUtil.getProperty("topN_output");
	String topN_result = PropertyUtil.getProperty("topN_result");// 最終輸出結果路徑
	String hdfs = PropertyUtil.getProperty("hdfs");

	public static void main(String[] args) {
		TopNJobRunner uj = new TopNJobRunner();
		uj.baseStart("BC1001");
	}

	@Override
	public boolean baseStart(String bus_code) {
		ClearDataTable.clearAdsTb(bus_code);
		step1(bus_code);
		step4(bus_code);
		System.out.println("程序正确执行");
		return true;
	}

	public boolean step1(String bus_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, parse_outPath, bus_code);
		// String input = "/foriseholdings/Algorithm/applogs/mytest";
		String output = ReturnFileFormat.getPathAddr(base_path, topN_output, bus_code);
		System.out.println(input);
		System.out.println(output);
		stepMapper = new TopNMapper();
		stepReducer = new TopNReducer();
		runMain.runTask(stepMapper, stepReducer, "", "step1", input, output, "", "topn", "");
		return true;
	}

	public boolean step4(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, topN_output, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, topN_result, buss_code);
		stepMapper = new TopNResultMapper();
		stepReducer = new TopNResultReducer();
		runMain.runTask(stepMapper, stepReducer, "", "topNResult", input, output, buss_code, "topn", "");

		try {
			MysqlShopSnAndUserDBOutput.start(hdfs + output);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

}