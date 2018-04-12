package com.foriseholdings.adsLabel;

import com.foriseholdings.adsLabel.mapper.AdTargetMapper;
import com.foriseholdings.adsLabel.mapper.LabelParseMapper;
import com.foriseholdings.adsLabel.reducer.AdTargetReducer;
import com.foriseholdings.adsLabel.reducer.LabelParseReducer;
import com.foriseholdings.adsLabel.write2Mysql.action.AdTargetDBOutput;
import com.foriseholdings.common.common.BaseMapper;
import com.foriseholdings.common.common.BaseReducer;
import com.foriseholdings.common.common.BaseRunner;
import com.foriseholdings.common.tools.ReturnFileFormat;
import com.foriseholdings.common.util.PropertyUtil;

public class AdTargetJobRunner extends BaseRunner {

	private static String parse_inPath = PropertyUtil.getProperty("parse_inPath");
	// 输出文件的相对路径
	private static String parse_outPath = PropertyUtil.getProperty("parse_outPath");
	private static String base_path = PropertyUtil.getProperty("base_path");
	private static String label_outPath = PropertyUtil.getProperty("label_outPath");
	String hdfs = PropertyUtil.getProperty("hdfs");
	protected BaseMapper stepMapper;
	protected BaseReducer stepReducer;
	int state = 0;

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		AdTargetJobRunner ad = new AdTargetJobRunner();
		ad.baseStart("BC1006");

		// try {
		// // AdTargetDBOutput.start(base_path+target_outPath);
		// AdTargetDBOutput.start(c);
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }
	}

	public boolean baseStart(String bus_code) {
		// ClearDataTable.clearLabelTb(bus_code);
		// adTargeParseStep1(bus_code);
		getTargetStep(bus_code);
		System.out.println("程序正确执行");
		return true;
	}

	// 用户ID /t 商品ID_次数
	// 6 20826_1,20934_9,20932_2,20930_2
	public boolean adTargeParseStep1(String bus_code) {
		String output = ReturnFileFormat.getPathAddr(base_path, parse_outPath, bus_code);
		String input = ReturnFileFormat.getPathAddr(base_path, parse_inPath, bus_code);
		// String input = "/applogs/20171210";
		// String input = "/foriseholdings/Algorithm/applogs/BC1006/20180309/";
		// String output = "/foriseholdings/Algorithm/parseLog/BC1006/20180309/";
		stepMapper = new LabelParseMapper();
		stepReducer = new LabelParseReducer();
		state = runMain.runTask(stepMapper, stepReducer, "", "step1", input, output, bus_code, "target", "");

		if (state == 1) {
			System.out.println("parse执行成功");
			return true;
		}
		return false;
	}

	public boolean getTargetStep(String bus_code) {

		String output = ReturnFileFormat.getPathAddr(base_path, label_outPath, bus_code);
		String input = ReturnFileFormat.getPathAddr(base_path, parse_outPath, bus_code);

		// String input = "/applogs/20171210";
		// String input = "/foriseholdings/Algorithm/parseLog/BC1006/20180309/";
		// String output = "/foriseholdings/Algorithm/Label/BC1006/20180309";
		System.out.println(input);
		System.out.println(output);
		stepMapper = new AdTargetMapper();
		stepReducer = new AdTargetReducer();
		state = runMain.runTask(stepMapper, stepReducer, "", "step2", input, output, bus_code, "target", "");
		if (state == 1) {
			System.out.println("parse执行成功");

			try {
				AdTargetDBOutput.start(hdfs + output);
				// AdTargetDBOutput.start("hdfs://192.168.152.194:8020//foriseholdings/Algorithm/Label/BC1006/20180309");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return true;
		}
		return false;
	}

}
