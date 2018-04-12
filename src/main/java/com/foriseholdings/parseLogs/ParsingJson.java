package com.foriseholdings.parseLogs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class ParsingJson {

	public static Map<String, Map<String, String>> valueRe(String endPoint) {

		String url = null;
		String userId = null; // 用戶ID
		String productId = null;// 商品ID
		String shopSn = null; // 门店编码

		ArrayList<String> typeCode = new ArrayList<String>();

		typeCode.add("OR200001");
		typeCode.add("OR200104");
		typeCode.add("PPM00001");
		typeCode.add("PR900001");

		Map<String, Map<String, String>> url_userAct = new HashMap<String, Map<String, String>>();
		Map<String, String> userAct = new HashMap<String, String>();

		JSONObject jsonObject = JSON.parseObject(endPoint);

		// if (jsonObject.containsKey("url")) {
		// // System.out.println("url:" + obj.getString("url"));
		// String[] urls = jsonObject.get("url").toString().split("business");
		// url = urls[1].substring(1, urls[1].length() - 3);
		// }

		if (jsonObject.containsKey("url")) {
			String urlStr = jsonObject.get("url").toString();
			if (!urlStr.contains(".do")) {
				return null;
			} else {
				String[] urls = urlStr.split("business");
				url = urls[1].substring(1, urls[1].length() - 3);
			}
		}

		/*
		 * 这行代码很容易出错
		 */
		if (!typeCode.contains(url)) {
			return null;
		}

		// SP111111_101
		// XS111111_101
		// TG111111_101
		// TJ111111_101
		// TC111111_101
		// CX111111_101
		// ZCX111111_101&101

		JSONObject belowJson = (JSONObject) jsonObject.get("form");
		if (belowJson.containsKey("userId")) {
			// null
			userId = (String) belowJson.get("userId").toString();
			if (userId == null || userId.equals("")) {
				userId = "用戶ID为空";
			}
		}

		if (belowJson.containsKey("shopSn")) {
			shopSn = (String) belowJson.get("shopSn").toString();
			if (shopSn == null || shopSn.equals("")) {
				shopSn = "门店编码为空";
			}
		}

		if (url.equals("OR200104")) {
			if (belowJson.containsKey("productIds")) {
				productId = (String) belowJson.get("productIds").toString();
//				if (productId.contains("ZCX")) {
//					return null;
//				}

			}
		} else {
			if (belowJson.containsKey("productId")) {
				productId = (String) belowJson.get("productId").toString();
//				if (productId.contains("ZCX")) {
//					return null;
//				}
			}
		}

		// System.out.println("userId --->"+userId);
		// System.out.println("productId --->"+productId);

		userAct.put(userId, productId);
		url_userAct.put(url, userAct);

		return url_userAct;
	}

}
