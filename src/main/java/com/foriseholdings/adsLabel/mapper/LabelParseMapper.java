package com.foriseholdings.adsLabel.mapper;

import java.util.ArrayList;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.foriseholdings.common.common.BaseMapper;

/**
 * 
 * @author qisun
 * 解析日志功能    陈辉已经在etl中做了 这部分可能将来版本中精简掉
 */
public class LabelParseMapper extends BaseMapper {

//	ArrayList<String> typeCode = new ArrayList<String>();
//
//	String url = null;
//	String user = null;
//	String productID = null;
//
//	protected boolean calcProc() {
//
//		{
//			String[] values = value.toString().split("===>");
//
//			if (values.length != 2) {
//				return false;
//			}
//
//			if (!values[0].contains("请求数据")) {
//				return false;
//			}
//
//			Map<String, Map<String, String>> urlmap = new HashMap<>();
//			Map<String, String> userPro = new HashMap<String, String>();
//
//			// 这个地方每次循环都要重新clear 和add 效率低
//			typeCode.clear();
//			typeCode.add("OR200001");
//			typeCode.add("OR200104");
//			typeCode.add("PPM00001");
//			typeCode.add("PR900001");
//
//			/**
//			 * OR200104 提交订单 OR200001 加入购物车 PPM00001 添加收藏 PR900001 查看
//			 * http://117.78.40.253:8981/org/index.do zhangzhengwang zhzhw7822038
//			 * 
//			 */
//
//			// 获取到实际操作对象
//			String nodeEnd = values[1];
//
//			urlmap = ParsingJson.valueRe(nodeEnd.trim());
//
//			if (urlmap == null) {
//
//				return false;
//			}
//
//			Iterator iterator = urlmap.entrySet().iterator();
//			while (iterator.hasNext()) {
//
//				Map.Entry<String, Map<String, String>> enter = (Entry<String, Map<String, String>>) iterator.next();
//				// OR200001 {6025=MJ1512107582191_11970}
//				// System.out.println("分割线--------------》");
//				// System.out.println(enter.getKey() + "\t" + enter.getValue());
//				url = enter.getKey();
//
//				if (!typeCode.contains(url)) {
//					return false;
//				}
//				userPro = (Map<String, String>) enter.getValue();
//
//				Iterator iterUserAndPro = userPro.entrySet().iterator();
//				while (iterUserAndPro.hasNext()) {
//					Map.Entry<String, String> enter2 = (Entry<String, String>) iterUserAndPro.next();
//					user = enter2.getKey();
//
//					if (user == null) {
//						return false;
//					}
//
//					if (user.equals("") || user.equals("null")) {
//						user = "用户ID空";
//					}
//					productID = enter2.getValue();
//
//					productID = getFormatProdId(productID);
//
//					if (productID.contains("undefined")) {
//						return false;
//					}
//
//					if (productID.contains(",")) {
//						String[] singleIDs = productID.split(",");
//						String prodIds = null;
//						for (String prodId : singleIDs) {
//							// \S
//							prodIds = prodId.replace("_", "\\");
//							outKey.set(user);
//							outValue.set(prodIds);
//							// context.write(outKey, outValue);
//							write();
//						}
//					} else {
//						outKey.set(user);
//						outValue.set(productID);
//						// context.write(outKey, outValue);
//						write();
//					}
//
//				}
//				urlmap.clear();
//				userPro.clear();
//			}
//		}
//		return true;
//	}
//
//	public String getFormatProdId(String str) {
//		if (str == null) {
//			str = "ID为空";
//			// return false; // 商品ID为空的情况
//		}
//		if (str.equals("null")) {
//			str = "ID为空";
//			// return false;
//		}
//		if (str.length() <= 0) {
//			str = "ID为空";
//			// return false;
//		}
//		if (str.contains("_")) {
//			str = str.replace("_", "\\");
//		}
//
//		return str;
//	}
	ArrayList<String> typeCode = new ArrayList<String>();

	String productID = null;
	String url = null;
	String userId = null; // 用戶ID

	String shopSn = null; // 门店编码
	String branch = null;
	String score = null;
	String productIds = null;

	/**
	 * OR200104 提交订单 OR200001 加入购物车 PPM00001 添加收藏 PR900001 查看
	 * http://117.78.40.253:8981/org/index.do zhangzhengwang zhzhw7822038
	 */
	protected boolean calcProc() {
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
			}
		}

		// 获取到平台编码 由于公用一套代码，平台编码用于区分业务系统
		// if (belowJson.containsKey("branch")) {
		// branch = (String) belowJson.get("branch").toString();
		// if (shopSn == null || shopSn.equals("")) {
		// branch = "平台编码为空";
		// }
		// }

		// 获取到商品ID
		if (belowJson.containsKey("productId")) {
			productId = belowJson.get("productId").toString();
			productId = getFormatProdId(productId);

		}

		// 多商品情況 为什么会存在一个id的情况？
		if (belowJson.containsKey("productIds")) {
			String prodIds[] = null;
			productIds = belowJson.get("productIds").toString();
			if (productIds.contains(",")) {
				prodIds = productIds.split(",");
				for (String product_Id : prodIds) {
					productId = getFormatProdId(product_Id);
					outKey.set(userId);
					outValue.set(shopSn + "@" + productId + "_" + score);
					write();
				}
			} else {
				productId = getFormatProdId(productIds);
			}
		}

		outKey.set(userId);
		outValue.set(shopSn + "@" +productId + "_" + score);
		write();
		return true;

	}

	// 应该封装一个方法 输入字符串 如果含有"_" 则替换 成 "\\"
	public String getFormatProdId(String str) {
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
			str = str.replace("_", "\\");
		}

		return str;
	}

}
