package com.foriseholdings.adsLabel.reducer;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.foriseholdings.adsLabel.Label.ILabel;
import com.foriseholdings.adsLabel.findListFromMysql.GetTargetValues;
import com.foriseholdings.adsLabel.findListFromMysql.bean.TargetBean;
import com.foriseholdings.common.common.BaseReducer;

public class AdTargetReducer extends BaseReducer {

	// key :列号 values
	protected boolean myreduce() {

		String label_id = null;
		String label_name = null;
		String result = null;
		String shopId_times = null;
		// List<String> shopIdScoreList = Arrays.asList(shopId_scores);
		Configuration conf = context.getConfiguration();
		buss_code = conf.get("buss_code");
		String src_id = key.toString();
		String uuid = src_id;
		for (Text value : values) {
			shopId_times = value.toString();
		}

		// reduce 实现了反射类,在反射中获取到,analysis 的取值
		String[] shopId_scores = shopId_times.split(",");
		// List<TargetBean> targetList = (List<TargetBean>) GetTargetValues.getTarget();
		List<List<TargetBean>> double_target = GetTargetValues.getTarget();
		if (double_target.size() == 0) {
			return true;
		}
		// String uuid = GetTargetValues.getUUID(src_id, buss_code);
		ILabel basLabel = new ILabel();

		// for (TargetBean target : targetList) {
		for (List<TargetBean> target : double_target) {
			if (target.size() == 0) {
				continue;
			}
			basLabel.init(target);
			basLabel.rowProc(shopId_scores);
			basLabel.analysis();
			result = basLabel.getReslt();

			if (uuid == null) {
				uuid = src_id;
			}
			outKey.set(uuid);

			if (result == null) {
				return true;
			}

			if (result.equals("0")) {
				return true;
			}

			outValue.set(result + "_" + buss_code);
			write();
		}

		return true;

	}

}
