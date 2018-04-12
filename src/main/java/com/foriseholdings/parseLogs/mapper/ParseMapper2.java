package com.foriseholdings.parseLogs.mapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.foriseholdings.common.common.BaseMapper;

public class ParseMapper2 extends BaseMapper {
	ArrayList<String> typeCode = new ArrayList<String>();
	List<String> onSellProdList = null;

	String productID = null;
	String url = null;
	String userId = null; // 用戶ID

	String shopSn = null; // 门店编码
	String branch = null;
	String score = null;
	String productIds = null;
	String buss_code;

	/**
	 * 封装方法 传入门店编码以及商品id 判断是否存在商品
	 */
	Boolean hasProd(String shopSn, String prodId) {
		if (prodId == null || prodId.equals("")) {
			return false;
		} else if (prodId.equals("null")) {
			return false;
		}
		Configuration conf = context.getConfiguration();
		String onSellProdStr = conf.get("onSellProd");
		Map<String, Object> shopSnProds = new HashMap<String, Object>();
		shopSnProds = JSON.parseObject(onSellProdStr);
		JSONArray ja = (JSONArray) shopSnProds.get(shopSn);
		//门店也有关店的情况 好坑 我r
		if(ja == null) {
			return false;
		}
		
		// System.out.println(ja.contains("20160"));
		System.out.println("==========");
		System.out.println(prodId.toString());
		System.out.println("==========");
		return ja.contains(prodId);

	}

	/**
	 * OR200104 提交订单 OR200001 加入购物车 PPM00001 添加收藏 PR900001 查看
	 * http://117.78.40.253:8981/org/index.do zhangzhengwang zhzhw7822038
	 */

	protected boolean calcProc() {
		// 从配置中获取商品门边信息 转存map 通过JSONArray.contain()判断是否存在商品

		if (value == null) {
			System.out.println("value返回空");
			return false;
		}
		String productId = null;// 商品ID
		String[] values = value.toString().split("===>");
		typeCode.clear();
		typeCode.add("OR200001"); // 提交订单
		typeCode.add("OR200104"); // 加入购物车
		typeCode.add("PPM00001"); // 添加收藏
		typeCode.add("PR900001"); // 查看

		if (values.length != 2) {
			return false;
		}
		if (!values[0].contains("请求数据")) {
			return false;
		}
		// 获取到实际操作对象
		String nodeEnd = values[1];
		// System.out.println("nodeEnd---->" + nodeEnd);
		// {"url":"http://49.4.5.233:8117/org/business/OR200001.do","method":"POST","json":true,"headers":{"Content-Type":"application/x-www-form-urlencoded;
		// charset=UTF-8","merchantNo":"000006","signType":"MD5","developToken":"000006","tokenPassword":"1qaz2wsx"},"form":{"isInvite":"1","productId":"12204","activityproid":"","price":"0","type":"add","isSupplierPro":"0","supplierId":"0","stock":"9990","channelNo":"1000001","userId":38,"shopSn":"B00001","isApp":"2","num":"1","userShopPlatformKey":"38_B00001_platform5","branch":"1002","channel":"2"}}
		JSONObject jsonObject = JSON.parseObject(nodeEnd);

		if (jsonObject.containsKey("url")) {
			String urlStr = jsonObject.get("url").toString();
			if (!urlStr.contains(".do")) {
				return false;
			} else {
				String[] urls = urlStr.split("business");
				url = urls[1].substring(1, urls[1].length() - 3);
			}
		}

		if (!typeCode.contains(url)) {
			return false;
		}

		switch (url) {
		case "OR200104":
			score = "6";
			break;
		case "OR200001":
			score = "3";
			break;
		case "PPM00001":
			score = "2";
			break;
		case "PR900001":
			score = "1";
			break;
		default:
			score = "0";
		}
		JSONObject belowJson = (JSONObject) jsonObject.get("form");
		// 获取到用户ID
		// 如果是查看PR900001 操作 会出现没有userId , 但是存在uuid情况
		if (belowJson.containsKey("userId")) {
			userId = (String) belowJson.get("userId").toString();
		}
		if (userId == null || userId.length() <= 0) {
			// userId = "未登陆只进行查看操作"; 未登录用户屏蔽
			return false;
		}
		// 获取到门店编码
		if (belowJson.containsKey("shopSn")) {
			shopSn = (String) belowJson.get("shopSn").toString();
			if (shopSn == null || shopSn.length() <= 0) {
				shopSn = "门店编码为空";
				return false;
			}
		}

		// 获取到商品ID
		if (belowJson.containsKey("productId")) {
			productId = belowJson.get("productId").toString();
			productId = getFormatProdId(productId);
			if (!hasProd(shopSn, productId)) {
				return false;
			}

		}

		// 多商品情況 为什么会存在一个id的情况？
		if (belowJson.containsKey("productIds")) {
			String prodIds[] = null;
			productIds = belowJson.get("productIds").toString();
			if (productIds.contains(",")) {
				prodIds = productIds.split(",");
				for (String product_Id : prodIds) {
					productId = getFormatProdId(product_Id);
					if (!hasProd(shopSn, productId)) {
						return false;
					}
					outKey.set(userId);
					outValue.set(shopSn + "@" + productId + "_" + score);
					write();
				}
			} else {
				productId = getFormatProdId(productIds);
				if (!hasProd(shopSn, productId)) {
					return false;
				}
			}
		}

		outKey.set(userId);
		outValue.set(shopSn + "@" + productId + "_" + score);
		write();
		return true;

	}

	// 应该封装一个方法 输入字符串 如果含有"_" 则替换 成 "\\"
	public String getFormatProdId(String str) {
		System.out.println(str);
		if (str == null) {
			str = "ID为空";
			// return false; // 商品ID为空的情况
		}
		if (str.equals("null")) {
			str = "ID为空";
			// return false;
		}
		if (str.length() <= 0) {
			str = "ID为空";
			// return false;
		}
		if (str.contains("_")) {
			str = str.split("_")[1];
		}

		return str;
	}

}
