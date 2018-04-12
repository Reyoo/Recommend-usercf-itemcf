package com.foriseholdings.algorithm.usercf;

import com.foriseholdings.algorithm.Itemcf.mapper.ItemcfMapper6;
import com.foriseholdings.algorithm.Itemcf.reducer.ItemcfReducer6;
import com.foriseholdings.algorithm.usercf.mapper.UsercfMapper2;
import com.foriseholdings.algorithm.usercf.mapper.UsercfMapper3;
import com.foriseholdings.algorithm.usercf.mapper.UsercfMapper4;
import com.foriseholdings.algorithm.usercf.mapper.UsercfMapper5;
import com.foriseholdings.algorithm.usercf.reducer.UsercfReducer2;
import com.foriseholdings.algorithm.usercf.reducer.UsercfReducer3;
import com.foriseholdings.algorithm.usercf.reducer.UsercfReducer4;
import com.foriseholdings.algorithm.usercf.reducer.UsercfReducer5;
import com.foriseholdings.common.common.BaseRunner;
import com.foriseholdings.common.tools.ReturnFileFormat;
import com.foriseholdings.common.util.PropertyUtil;
import com.foriseholdings.write2mysql.action.MysqlShopSnAndUserDBOutput;

public class UserCFJobRunner extends BaseRunner {

	// 配置文件十分重要 ，如果一旦不能成功执行，需要先查看配置文件的路径， 是否存在，缓存是否准确到缓存文件名

	String base_path = PropertyUtil.getProperty("base_path");
	String parse_outPath = PropertyUtil.getProperty("parse_outPath");
	String userCF_s2_outPath = PropertyUtil.getProperty("userCF_s2_outPath");
	String userCF_s3_outPath = PropertyUtil.getProperty("userCF_s3_outPath");
	String userCF_s4_outPath = PropertyUtil.getProperty("userCF_s4_outPath");
	String userCF_s5_outPath = PropertyUtil.getProperty("userCF_s5_outPath");
	String userCF_s6_outPath = PropertyUtil.getProperty("userCF_s6_outPath");
	String step2_cache = PropertyUtil.getProperty("step2_cache");
	String s4_cache = PropertyUtil.getProperty("userCF_s4_cache");

	String hdfs = PropertyUtil.getProperty("hdfs");

	public static void main(String[] args) {
		UserCFJobRunner uj = new UserCFJobRunner();
		uj.baseStart("BC1006");
	}

	@Override
	public boolean baseStart(String buss_code) {
		step2(buss_code);
		step3(buss_code);
		step4(buss_code);
		step5(buss_code);
		step6(buss_code);
		// if (step6(buss_code)) {
		// System.out.println("程序正确执行");
		// return true;
		// }
		return true;
	}

	// @Override
	// public boolean step1() {
	//
	// // String input = ReturnFileFormat.getPathAddr(userCF_base_Path,
	// // userCF_s1_outPath);
	// String output = ReturnFileFormat.getPathAddr(userCF_base_Path,
	// userCF_s2_outPath);
	// String cache = ReturnFileFormat.getPathAddr(userCF_base_Path, s2_5_cache);
	//
	// stepMapper = new UsercfMapper1();
	// stepReducer = new UsercfReducer1();
	// state = runMain.runTask(stepMapper, stepReducer, "", "step1",
	// userCF_s1_inPath, userCF_s1_outPath);
	//
	// if (state == 1) {
	// System.out.println("Usercf_step1执行成功");
	// }
	// return true;
	// }

	/**
	 * 第二步： 用户相似度矩阵 ----------------相似度
	 */
	@Override
	public boolean step2(String buss_code) {

		String input = ReturnFileFormat.getPathAddr(base_path, parse_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, userCF_s2_outPath, buss_code);
		String cache = ReturnFileFormat.getPathAddr(base_path, step2_cache, buss_code);

		System.out.println("input:" + input);
		System.out.println("output:" + output);
		System.out.println("cache:" + cache);
		stepMapper = new UsercfMapper2();
		stepReducer = new UsercfReducer2();
		runMain.runTask(stepMapper, stepReducer, cache, "step2", input, output, buss_code, "usercf", "");
		return true;
	}

	/**
	 * 第三步： 将评分矩阵转置 输入:步骤1 的输出 输出 :用户ID(行)
	 * ------------物品ID(ID)列---------------------分值
	 */

	@Override
	public boolean step3(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, parse_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, userCF_s3_outPath, buss_code);

		// input += "part-r-00000";
		stepMapper = new UsercfMapper3();
		stepReducer = new UsercfReducer3();
		runMain.runTask(stepMapper, stepReducer, "", "step3", input, output, buss_code, "usercf", "");
		return true;
	}

	/**
	 * 第四步 物品与物品相似度矩阵 X 评分矩阵 (经过步骤3转置) 输入步骤2的输出 缓存:步骤3的输出 输出:物品ID(行)
	 * ----------用户ID(列)---------分值
	 */
	public boolean step4(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, userCF_s2_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, userCF_s4_outPath, buss_code);
		String cache = ReturnFileFormat.getPathAddr(base_path, s4_cache, buss_code);
		stepMapper = new UsercfMapper4();
		stepReducer = new UsercfReducer4();
		runMain.runTask(stepMapper, stepReducer, cache, "step4", input, output, buss_code, "usercf", "");
		return true;
	}

	/**
	 * 得到推荐列表
	 * 
	 * @return
	 */
	public boolean step5(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, userCF_s4_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, userCF_s5_outPath, buss_code);
		String cache = ReturnFileFormat.getPathAddr(base_path, step2_cache, buss_code);
		// cache+= "/20180117195203";
		// String cache = "/foriseholdings/Algorithm/parseLog/20180117/step_output/";
		stepMapper = new UsercfMapper5();
		stepReducer = new UsercfReducer5();
		runMain.runTask(stepMapper, stepReducer, cache, "step5", input, output, buss_code, "usercf", "");
		return true;
	}

	/**
	 * 
	 * @return
	 */
	public boolean step6(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, userCF_s5_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, userCF_s6_outPath, buss_code);

		stepMapper = new ItemcfMapper6();
		stepReducer = new ItemcfReducer6();
		runMain.runTask(stepMapper, stepReducer, "", "step6", input, output, buss_code, "usercf", "");

		try {
			MysqlShopSnAndUserDBOutput.start(hdfs + output);
			System.out.println("userCF正確執行" + "----->" + buss_code);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

}
