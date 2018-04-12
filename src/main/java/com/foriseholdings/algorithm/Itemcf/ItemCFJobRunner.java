package com.foriseholdings.algorithm.Itemcf;

import com.foriseholdings.algorithm.Itemcf.mapper.ItemcfMapper2;
import com.foriseholdings.algorithm.Itemcf.mapper.ItemcfMapper3;
import com.foriseholdings.algorithm.Itemcf.mapper.ItemcfMapper4;
import com.foriseholdings.algorithm.Itemcf.mapper.ItemcfMapper5;
import com.foriseholdings.algorithm.Itemcf.mapper.ItemcfMapper6;
import com.foriseholdings.algorithm.Itemcf.mapper.ItemcfMapper7;
import com.foriseholdings.algorithm.Itemcf.reducer.ItemcfReducer2;
import com.foriseholdings.algorithm.Itemcf.reducer.ItemcfReducer3;
import com.foriseholdings.algorithm.Itemcf.reducer.ItemcfReducer4;
import com.foriseholdings.algorithm.Itemcf.reducer.ItemcfReducer5;
import com.foriseholdings.algorithm.Itemcf.reducer.ItemcfReducer6;
import com.foriseholdings.algorithm.Itemcf.reducer.ItemcfReducer7;
import com.foriseholdings.common.common.BaseMapper;
import com.foriseholdings.common.common.BaseReducer;
import com.foriseholdings.common.common.BaseRunner;
import com.foriseholdings.common.tools.ReturnFileFormat;
import com.foriseholdings.common.util.PropertyUtil;
import com.foriseholdings.write2mysql.action.MysqlShopSnAndUserDBOutput;
import com.foriseholdings.write2mysql.action.MysqlSimilarProd;

/*
 * 算法思想：
 * 给用户推荐那些和他们之前喜欢的物品相似的物品
 */
public class ItemCFJobRunner extends BaseRunner {

	String parse_outPath = PropertyUtil.getProperty("parse_outPath");
	String base_path = PropertyUtil.getProperty("base_path");

	String itemCF_s1_outPath = PropertyUtil.getProperty("itemCF_s1_outPath");
	String itemCF_s2_outPath = PropertyUtil.getProperty("itemCF_s2_outPath");
	String itemCF_s3_outPath = PropertyUtil.getProperty("itemCF_s3_outPath");
	String itemCF_s4_outPath = PropertyUtil.getProperty("itemCF_s4_outPath");
	String itemCF_s5_outPath = PropertyUtil.getProperty("itemCF_s5_outPath");
	String itemCF_s6_outPath = PropertyUtil.getProperty("itemCF_s6_outPath");
	String itemCF_s7_outPath = PropertyUtil.getProperty("itemCF_s7_outPath");
	String hdfs = PropertyUtil.getProperty("hdfs");
	String itemCF_s2_5_cache = PropertyUtil.getProperty("itemCF_s2_5_cache");
	String s4_cache = PropertyUtil.getProperty("s4_cache");

	protected BaseMapper stepMapper;
	protected BaseReducer stepReducer;
	int state = -1;

	public static void main(String[] args) {
		ItemCFJobRunner it = new ItemCFJobRunner();
		it.baseStart("BC1006");
	}

	@Override
	public boolean baseStart(String buss_code) {
		step1(buss_code); // 将 解析后结果转制成基于商品推荐的矩阵
		step2(buss_code);
		step3(buss_code);
		step4(buss_code);
		step5(buss_code);
		//
		// step6(buss_code);
		if (step6(buss_code)) {
			return true;
		}
		return false;
	}

	/**
	 * 第二步： 利用评分矩阵，构建物品与物品的相似度矩阵 输入:步骤1的输出 缓存：步骤1的输出 输出:物品ID(行)-------------物品ID(列)
	 * ----------------相似度矩阵
	 */

	public boolean step2(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, itemCF_s1_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, itemCF_s2_outPath, buss_code);
		String cache = ReturnFileFormat.getPathAddr(base_path, itemCF_s2_5_cache, buss_code);
		// String input =
		// "/foriseholdings/Algorithm/cafe_itemcf/20180122/step1_output/part-r-00000";
		// String
		// output="/foriseholdings/Algorithm/cafe_itemcf/20180122/step100_output";
		// String cache
		// ="/foriseholdings/Algorithm/cafe_itemcf/20180122/step1_output/part-r-00000" ;
		// cache = cache +"part-r-00000";
		System.out.println(cache);
		stepMapper = new ItemcfMapper2();
		stepReducer = new ItemcfReducer2();
		runMain.runTask(stepMapper, stepReducer, cache, "step2", input, output, buss_code, "itemcf","");

		try {
			MysqlSimilarProd.start(hdfs + output);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	/**
	 * 第三步： 将评分矩阵转置 输入:步骤1 的输出 输出 :用户ID(行)
	 * ------------物品ID(ID)列---------------------分值
	 */

	public boolean step3(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, itemCF_s1_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, itemCF_s3_outPath, buss_code);

		stepMapper = new ItemcfMapper3();
		stepReducer = new ItemcfReducer3();
		runMain.runTask(stepMapper, stepReducer, "", "step3", input, output, "", "itemcf","");
		return true;
	}

	/**
	 * 第四步 物品与物品相似度矩阵 X 评分矩阵 (经过步骤3转置) 输入步骤2的输出 缓存:步骤3的输出 输出:物品ID(行)
	 * ----------用户ID(列)---------分值
	 */
	public boolean step4(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, itemCF_s2_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, itemCF_s4_outPath, buss_code);
		String cache = ReturnFileFormat.getPathAddr(base_path, s4_cache, buss_code);
		System.out.println("input:" + input);
		System.out.println("output:" + output);
		System.out.println("cache:" + cache);
		stepMapper = new ItemcfMapper4();
		stepReducer = new ItemcfReducer4();
		runMain.runTask(stepMapper, stepReducer, cache, "step4", input, output, buss_code, "itemcf","");
		return true;
	}

	/**
	 * 得到推荐列表 ,用户已经有过行为的商品评分置0
	 * 
	 * @return
	 */
	public boolean step5(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, itemCF_s4_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, itemCF_s5_outPath, buss_code);
		String cache = ReturnFileFormat.getPathAddr(base_path, itemCF_s2_5_cache, buss_code);

		stepMapper = new ItemcfMapper5();
		stepReducer = new ItemcfReducer5();
		runMain.runTask(stepMapper, stepReducer, cache, "step5", input, output, buss_code, "itemcf","");
		return true;
	}

	/**
	 * 外层循环step(6) 前是毫无意义的 ，应该单独把这步拿出来 第六步相对与第五步 只是在输出格式上增加了 业务编码以及类型
	 * 
	 * @param bus_code
	 * @return
	 */
	public boolean step6(String buss_code) {

		String input = ReturnFileFormat.getPathAddr(base_path, itemCF_s5_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, itemCF_s6_outPath, buss_code);
		stepMapper = new ItemcfMapper6();
		stepReducer = new ItemcfReducer6();
		runMain.runTask(stepMapper, stepReducer, "", "step6", input, output, buss_code, "itemcf","");

		try {
			MysqlShopSnAndUserDBOutput.start(hdfs + output);

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 物品 用户_分值
	 */
	public boolean step1(String buss_code) {
		String input = ReturnFileFormat.getPathAddr(base_path, parse_outPath, buss_code);
		String output = ReturnFileFormat.getPathAddr(base_path, itemCF_s1_outPath, buss_code);
		System.out.println(input);
		System.out.println(output);
		stepMapper = new ItemcfMapper7();
		stepReducer = new ItemcfReducer7();
		runMain.runTask(stepMapper, stepReducer, "", "step1", input, output, buss_code, "itemcf","");
		return true;
	}
}
