package com.foriseholdings.algorithm.taskscheduling.service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.foriseholdings.algorithm.taskscheduling.model.ResultModer;
import com.foriseholdings.common.util.PropertyUtil;

public class ParseJson {

	static String busCode = null;
	static String execState = null;
	static Date job_time = null;
	static Integer timespan = null;

	String jdbcDri = PropertyUtil.getProperty("jdbcDri");
	String username = PropertyUtil.getProperty("username");
	String password = PropertyUtil.getProperty("password");

	public static void main(String[] args) {
		//
		String orgJson = "{	\"@type\":\"com.spencer.dap.common.result.BaseResult\",	\"code\":\"000000\",	\"message\":\"业务处理成功！\",	\"result\":\"DAP1001,BC1006\"}";
		String orgJsonStr = orgJson.replace("@", "\\");
		getbusCodeInfo(orgJsonStr);
		// getResult(orgJsonStr);
		//
	}

	public static ResultModer getBaseInfo(String orgJson) {

		JSONObject jsonObj = JSONObject.parseObject(orgJson);
		// [{"jobTime":"2018-01-01","busCode":"BC1006","ftpPath":"/BC1006/d5log/201801/01","id":3006}]
		String successFlag = jsonObj.getString("code");
		JSONArray array = jsonObj.getJSONArray("result");

		if (array == null || array.isEmpty()) {
			return null;
		}

		// 如果有任務
		List<ResultModer> jobs = JSON.parseArray(array.toString(), ResultModer.class);
		for (ResultModer job : jobs) {
			System.out.println(job.getFtpPath());
			job.setCode(successFlag);
			return job;
		}
		return null;
	}

	// { "@type":"com.spencer.dap.common.result.BaseResult", "code":"000000",
	// "message":"业务处理成功！", "result":"DAP1001,BC1006"}
	public static List<String> getbusCodeInfo(String orgJson) {
		List<String> busList = new ArrayList<String>();
		JSONObject jsonObj = JSONObject.parseObject(orgJson);
		// [{"jobTime":"2018-01-01","busCode":"BC1006","ftpPath":"/BC1006/d5log/201801/01","id":3006}]
		String successFlag = jsonObj.getString("code");
		String busCodes = jsonObj.getString("result");

		if (!successFlag.equals("000000")) {
			return null;
		}
		if (busCodes.contains(",")) {
			for (String busCode : busCodes.split(",")) {
				busList.add(busCode);
			}
		} else {
			busList.add(busCodes);
		}

		return busList;
	}

	// public static Map<String, String> getResult(String jsonStr) {
	// JSONObject jsonObj = JSONObject.parseObject(jsonStr);
	// Map<String, Object> mapJson = JSONObject.parseObject(jsonStr);
	// for (Entry<String, Object> entry : mapJson.entrySet()) {
	// String strkey1 = entry.getKey();
	// Object strval1 = entry.getValue();
	// System.out.println("KEY:" + entry.getKey() + " --> Value:" + entry.getValue()
	// + "\n");
	// }
	// String arrayJson = mapJson.get("result").toString();
	// JSONArray array = JSONArray.parseArray(arrayJson);
	//
	// List<Map> jobs = JSON.parseArray(array.toString(), Map.class);
	// for (int i = 0; i < jobs.size(); i++) {
	// Map<String, Object> obj = jobs.get(i);
	// // for (Entry<String, Object> entry1 : obj.entrySet()) {
	// // String strkey12 = entry1.getKey();
	// // Object strval12 = entry1.getValue();
	// // System.out.println("KEY:" + strkey12 + " --> Value:" + strval12 +"\n");
	// // }
	// }
	// return null;
	// }

}
