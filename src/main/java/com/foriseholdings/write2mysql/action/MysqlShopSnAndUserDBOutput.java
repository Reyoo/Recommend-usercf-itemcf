package com.foriseholdings.write2mysql.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.foriseholdings.common.tools.DefDateformat;
import com.foriseholdings.common.tools.JudgeProdStatus;
import com.foriseholdings.common.util.PropertyUtil;
import com.foriseholdings.write2mysql.bean.ShopAndUserWritable;

public class MysqlShopSnAndUserDBOutput extends Configured implements Tool {

	String jdbcDri = PropertyUtil.getProperty("jdbcDri");
	String username = PropertyUtil.getProperty("username");
	String password = PropertyUtil.getProperty("password");

	static String bus_code = "";

	public static class DBOutputMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class DBOutputReducer extends Reducer<LongWritable, Text, ShopAndUserWritable, ShopAndUserWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
//			List<String> onSellProdList = new ArrayList<String>();
			// 109_B00004 CX201801161059244951\20088_5.64 itemcf BC1006
			Map<String, Double> prodIdAndScoreMap = new HashMap<String, Double>();

			StringBuilder value = new StringBuilder();
			for (Text text : values) {
				value.append(text);
			}
			String shopSn_userID = value.toString().split("\t")[0];
			// System.out.println(shopSn_userID);
			String shopSn = shopSn_userID.split("_")[1];
			String user_id = shopSn_userID.split("_")[0];
			String buss_code = value.toString().split("\t")[3];
			String algorithm_type = value.toString().split("\t")[2];
			String commend_prod_ids_str = value.toString().split("\t")[1];
//			onSellProdList = JudgeProdStatus.getOnSellProdList(buss_code);
			// 20089_8.58,20086_2.70,CX201801221009133752\20089_8.58
			if (commend_prod_ids_str.contains(",")) {
				List<String> commend_prod_ids_list = new LinkedList<String>();
				// List<String> onSellProdList = JudgeProdStatus.getOnSellProdList(buss_code,
				// shopSn);
				String commend_prod_ids[] = commend_prod_ids_str.split(",");

				for (String prod_ids : commend_prod_ids) {
						prodIdAndScoreMap.put(prod_ids.split("_")[0], Double.valueOf(prod_ids.split("_")[1]));
				}

				List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(
						prodIdAndScoreMap.entrySet());
				Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
					public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
						return (o2.getValue()).compareTo(o1.getValue());
					}
				});

				for (Map.Entry<String, Double> entry : list) {
					System.out.println("key:" + entry.getKey() + "  value:" + String.valueOf(entry.getValue()));
					commend_prod_ids_list.add(entry.getKey());

				}
				commend_prod_ids_str = commend_prod_ids_list.toString();
				commend_prod_ids_str = commend_prod_ids_str.substring(1, commend_prod_ids_str.length() - 1);
				commend_prod_ids_list.clear();

			} else {
				commend_prod_ids_str = commend_prod_ids_str.split("_")[0];
				System.out.println("commend_prod_ids_str:" + commend_prod_ids_str);
			}
			String algorithm_date = DefDateformat.getStringDateShort();

			ShopAndUserWritable tool = new ShopAndUserWritable(shopSn, user_id, buss_code, commend_prod_ids_str,
					algorithm_type, algorithm_date);
			context.write(tool, null);

		}
	}

	public static void main(String[] args) {
		try {
			start("hdfs://192.168.92.215:8020//foriseholdings/Algorithm/BC1001/usercf/20180329/step6_output/");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void start(String hdfs) throws Exception {
		// 数据输入路径和输出路径
		String[] args0 = { hdfs };
		int ec = ToolRunner.run(new Configuration(), new MysqlShopSnAndUserDBOutput(), args0);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 读取配置文件
		Configuration conf = new Configuration();

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", jdbcDri, username, password);

		// 新建一个任务
		// Job job = new Job(conf, "DBOutputormatDemo");
		Job job = Job.getInstance(conf, "DBOutputormatDemo");
		// 设置主类
		job.setJarByClass(MysqlShopSnAndUserDBOutput.class);

		// 输入路径
		FileInputFormat.addInputPath(job, new Path(arg0[0]));

		// Mapper
		job.setMapperClass(DBOutputMapper.class);
		// Reducer
		job.setReducerClass(DBOutputReducer.class);

		// mapper输出格式
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		// job.addArchiveToClassPath(new
		// Path("hdfs://192.168.92.215:9000/lib/mysql/mysql-connector-java-5.1.38.jar"));
		// job.addArchiveToClassPath(new
		// Path("/lib/mysql/mysql-connector-java-5.1.38.jar"));

		job.setOutputFormatClass(DBOutputFormat.class);
		// shopsn, user_id, buss_code, commend_prod_id,algorithm_type, algorithm_date
		// 输出到哪些表、字段
		DBOutputFormat.setOutput(job, "elep_shopsn_userid", "shopsn", "user_id", "buss_code", "commend_prod_id",
				"algorithm_type", "algorithm_date");
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
