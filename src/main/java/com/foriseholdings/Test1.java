package com.foriseholdings;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.foriseholdings.common.tools.JudgeProdStatus;

public class Test1 {
	public static void main(String[] args) {
		Test1 test = new Test1();
		String buss_code = "BC1006";
		List<String> shopSn = null;
		shopSn = JudgeProdStatus.getShopSnList(buss_code);
		String json = JudgeProdStatus.getOnSellProdList(buss_code, shopSn);
		System.out.println(json);
		test.stringToMap(json);
	}

	void stringToMap(String json) {
		Map<String, Object> shopSnProds = new HashMap<String, Object>();
		shopSnProds = JSON.parseObject(json);
		JSONArray ja = (JSONArray) shopSnProds.get("B00001");
		System.out.println(ja.contains("20160"));

		System.out.println("---");
	}

}
