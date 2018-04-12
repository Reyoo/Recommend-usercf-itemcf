package com.foriseholdings.parseLogs;

import java.util.ArrayList;
import java.util.List;

import com.foriseholdings.common.common.BaseRunner;
import com.foriseholdings.common.tools.JudgeProdStatus;
import com.foriseholdings.common.tools.ReturnFileFormat;
import com.foriseholdings.common.util.PropertyUtil;
import com.foriseholdings.parseLogs.mapper.ParseMapper2;
import com.foriseholdings.parseLogs.reducer.ParseReduer2;

public class ParseMain2 extends BaseRunner {

	private static String parse_inPath = PropertyUtil.getProperty("parse_inPath");
	// 输出文件的相对路径
	private static String parse_outPath = PropertyUtil.getProperty("parse_outPath");

	private static String base_path = PropertyUtil.getProperty("base_path");

	public static void main(String[] args) {
		ParseMain2 pm = new ParseMain2();
		pm.baseStart("BC1006");
	}

	public boolean baseStart(String buss_code) {
		System.out.println("执行parse");
		if (step1(buss_code)) {
			return true;
		}
		return false;
	}

	public boolean step1(String buss_code) {

		//
		String output = ReturnFileFormat.getPathAddr(base_path, parse_outPath, buss_code);
		String input = ReturnFileFormat.getPathAddr(base_path, parse_inPath, buss_code);

		List<String> shopSn = null;
		shopSn = JudgeProdStatus.getShopSnList(buss_code);
		String json = JudgeProdStatus.getOnSellProdList(buss_code, shopSn);
		System.out.println(json);

//		String input = "/foriseholdings/Algorithm/applogs/BC1006/20180412";
//		String output = "/foriseholdings/Algorithm/parseLog/BC1006/test";
		stepMapper = new ParseMapper2();
		stepReducer = new ParseReduer2();
		state = runMain.runTask(stepMapper, stepReducer, "", "step1", input, output, buss_code, "parse", json);

		if (state == 1) {
			System.out.println("parse执行成功");
			return true;
		}
		return false;
	}

}
