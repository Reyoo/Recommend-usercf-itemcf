package com.foriseholdings;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.foriseholdings.adsLabel.AdTargetJobRunner;
import com.foriseholdings.algorithm.Itemcf.ItemCFJobRunner;
import com.foriseholdings.algorithm.taskscheduling.model.ResultModer;
import com.foriseholdings.algorithm.taskscheduling.service.ParseJson;
import com.foriseholdings.algorithm.taskscheduling.service.RequestTask;
import com.foriseholdings.algorithm.topN.TopNJobRunner;
import com.foriseholdings.algorithm.usercf.UserCFJobRunner;
import com.foriseholdings.common.common.BaseRunner;
import com.foriseholdings.common.util.PropertyUtil;
import com.foriseholdings.parseLogs.ParseMain2;
import com.foriseholdings.upload2hdfs.UploadToHdfs;

/**
 * @author qisun
 * @date 2018年4月2日 项目入口
 */
public class CoreScheduledExecutor implements Runnable {

	static Logger logger = Logger.getLogger(CoreScheduledExecutor.class);
	static final String GET_URL = PropertyUtil.getProperty("GET_URL");
	static final String UPDATE_URL = PropertyUtil.getProperty("UPDATE_URL");
	static final String FAILL = "111111";
	static final String HAVE_LOGS = "000000";
	static final String NOTASK = "222222";
	static String ADVERTISMENT = "ads";
	static String LABEL = "labels";
	public static String task_url = null;
	BaseRunner baseRun;

	private String jobName = "";

	public CoreScheduledExecutor() {
	}

	public CoreScheduledExecutor(String jobname) {
		super();
		this.jobName = jobname;
	}

	@Override
	public void run() {
		jobEntrance();
		System.out.println("进度 " + jobName);
	}

	public static void main(String[] args) {
		CoreThreadPoolExecutor exec = new CoreThreadPoolExecutor();
		exec.init();
		ExecutorService pool = exec.getCustomThreadPoolExecutor();
		while (true) {
			try {
				pool.execute(new CoreScheduledExecutor());
				// 睡眠1小时
				Thread.sleep(3600000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 项目执行总入口
	 */
	public void jobEntrance() {
		CoreScheduledExecutor tt = new CoreScheduledExecutor();
		// 从接口中获取业务编码 "result":"BC1001,BC1006"
		String responseJson = RequestTask.sendPost("", GET_URL);
		// // 需要替换掉@ 符号如果不替换 解析json报错
		String orgJson = responseJson.replace("@", "//");
		List<String> busCodeInfo = new ArrayList<String>();
		busCodeInfo = ParseJson.getbusCodeInfo(orgJson);

		for (String busCode : busCodeInfo) {
			// String label_url = "http://223.223.202.230:8880/task/get/" + busCode + "/12";
			// String recommend_url = "http://223.223.202.230:8880/task/get/" + busCode +
			// "/10";
			// String getLabelTaskUrl = "http://192.168.92.7:8880/task/get/" + busCode +
			// "/12";
			// String getRecommendTaskUrl = "http://192.168.92.7:8880/task/get/" + busCode +
			// "/10";
			// 测试库
			String getLabelTaskUrl = "http://119.253.65.210:8990/task/get/" + busCode + "/12";
			String getRecommendTaskUrl = "http://119.253.65.210:8990/task/get/" + busCode + "/10";

			List<String> requestUrlList = new ArrayList<String>();
			requestUrlList.add(getLabelTaskUrl);
			requestUrlList.add(getRecommendTaskUrl);

			for (String taskUrl : requestUrlList) {
				boolean flag = false;
				responseJson = RequestTask.sendPost("", taskUrl);
				orgJson = responseJson.replace("@", "//");
				ResultModer resultModer = ParseJson.getBaseInfo(orgJson);

				if (resultModer == null) {

					logger.info(new Date());
					logger.info("未获取到任务");
					logger.info(new Date());
					continue;
				}

				else if (FAILL.equals(resultModer.getCode()) || NOTASK.equals(resultModer.getCode())) {
					logger.info("code is 111111 或者 222222");
					flag = false;
				} else if (HAVE_LOGS.equals(resultModer.getCode())) {
					String ftpPath = resultModer.getFtpPath();
					// 上传
					String businessCode = resultModer.getBusCode();

					String mrType = resultModer.getMrType();
					UploadToHdfs.run(ftpPath, businessCode);
					// /String buss_code = "BC1001";
					flag = tt.getTask(businessCode, mrType);
					if (flag) {
						JSONObject jsonObj = JSONObject.parseObject(orgJson);
						jsonObj.clear();
						jsonObj.put("id", resultModer.getId());
						jsonObj.put("flag", "2");
						jsonObj.put("reason", "调取任务执行成功");
						RequestTask.sendPost(jsonObj.toString(), UPDATE_URL);
					}
					// System.gc();
				}
			}
		}
		// System.gc();
	}

	public boolean getTask(String busCode, String mrType) {
		boolean flag = false;
		// 解析日志 解析输出结果 是所有算法的输入， 解析输入 是上传路径

		baseRun = new ParseMain2();
		flag = baseRun.baseStart(busCode);
		if (ADVERTISMENT.equals(mrType)) {
			// flag = ClearDataTable.clearTb(buss_code);
			// topN算法
			if (flag) {
				baseRun = new TopNJobRunner();
				flag = baseRun.baseStart(busCode);
			}
			// 基于物品
			if (flag) {
				baseRun = new ItemCFJobRunner();
				flag = baseRun.baseStart(busCode);
			}
			if (flag) {
				baseRun = new UserCFJobRunner();
				flag = baseRun.baseStart(busCode);
			}
		} else if (LABEL.equals(mrType)) {
			baseRun = new AdTargetJobRunner();
			flag = baseRun.baseStart(busCode);
		}
		return flag;
	}

}
