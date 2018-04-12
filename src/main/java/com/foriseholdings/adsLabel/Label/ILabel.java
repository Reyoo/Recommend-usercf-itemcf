package com.foriseholdings.adsLabel.Label;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.foriseholdings.adsLabel.findListFromMysql.GetLabelValueMap;
import com.foriseholdings.adsLabel.findListFromMysql.bean.TargetBean;

public class ILabel {

	protected String result;
	// protected Map<String,String> result = null;
	// protected String proportion = null; // 比例
	protected Map<String, Double> proportion = null; // 比例
	// protected double proportionRate = 0.0;
	protected Map<String, Double> proportionRate;
	protected Map<String, String> labelNameMap;
	protected Map<String, String> labelIdMap;
	// List<String> analysisList = null;
	Map<String, List<String>> label_prodIDs = null; // 标签 商品id _ list
	protected Map<String, Integer> countMap = null;
	private int total = 0;

	Map<String, Map<String, String>> beanMap = null;

	/**
	 * 初始化 获取到商品ID集合
	 * 
	 * @param type_id
	 *            标签类型, rate 满足标签的比例
	 */
	public void init(List<TargetBean> beans) {
		proportion = new HashMap<>();
		proportionRate = new HashMap<>();
		label_prodIDs = new HashMap<>();
		labelNameMap = new HashMap<>();
		labelIdMap = new HashMap<>();
		countMap = new HashMap<>();
		for (TargetBean tb : beans) {
			label_prodIDs.put(tb.getLabel_id(), GetLabelValueMap.getGoods(tb.getPortrait_id()));
			proportionRate.put(tb.getLabel_id(), tb.getPortrait_rate());

			proportion.put(tb.getLabel_id(), 0.0d);
			countMap.put(tb.getLabel_id(), 0);
			labelNameMap.put(tb.getLabel_id(), tb.getPortrait_name());
		}

		// String shop_ids =
		// "20203,20125,20047,20189,20188,20187,20198,20043,20186,20193,20191,20129,20126,20203,20125,20045,20133,20141,20129,20107,20204\r\n"
		// + "";
		// analysisList = new ArrayList<String>();
		// for (String shop_id : shop_ids.split(",")) {
		// analysisList.add(shop_id);
		// }
	}

	/**
	 * 逐字符判断 进行运算 ·
	 * 
	 * @param 商品和分数的评分
	 * @return
	 */
	public boolean rowProc(String[] shopId_scores) {
		try {
			total = shopId_scores.length;
			for (String tmp : shopId_scores) {
				String shopId = tmp.split("_")[0];
				for (String key : label_prodIDs.keySet()) {
					if (label_prodIDs.get(key).contains(shopId)) {
						countMap.replace(key, countMap.get(key) + Integer.parseInt(tmp.split("_")[1]));
					}
				}
			}

			// System.out.println(cur_rate);
		} catch (Exception e) {
			return false;
		}
		return true;
	}

	/**
	 * @param label_id
	 *            标签id, label analysis 比例 分析结果 在初始化时 查询到所有标签信息
	 * @return
	 */

	public boolean analysis() {
		String tmpKey = "";
		Integer count = 0;
		String inner_result;
		for (String key : countMap.keySet()) {
			if (countMap.get(key) >= count) {
				tmpKey = key;
				count = countMap.get(key);
			}
		}
		System.out.println(proportionRate == null);
		inner_result = (double) count / total >= proportionRate.get(tmpKey) ? "1" : "0";
		result = tmpKey + "_" + labelNameMap.get(tmpKey) + "_" + inner_result;
		return true;
	}

	/**
	 * 返回結果
	 * 
	 * @return
	 */
	public String getReslt() {
		return result;
	}

}
