package com.foriseholdings.algorithm.Itemcf.reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.foriseholdings.common.common.BaseReducer;

public class ItemcfReducer6 extends BaseReducer {

	// String busi_code = PropertyUtil.getBusiCode("busi_code");

	// public ItemcfReducer6(String bus_code) {
	// this.bus_code = bus_code;
	// }

	protected boolean myreduce() {

		String userID_shopSn = key.toString();
		Configuration conf = context.getConfiguration();
		buss_code = conf.get("buss_code");
		type = conf.get("type");
		for (Text value : values) {
			String prodId_scores = value.toString();
			outKey.set(userID_shopSn);
			outValue.set(prodId_scores + "	" + type + "	" + buss_code);
			write();
		}
		return true;

	}

}
