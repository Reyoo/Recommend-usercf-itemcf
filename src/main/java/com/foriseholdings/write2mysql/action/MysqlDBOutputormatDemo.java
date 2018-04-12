package com.foriseholdings.write2mysql.action;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
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
import com.foriseholdings.common.util.PropertyUtil;
import com.foriseholdings.write2mysql.bean.TblsWritable;

/**
 * @ProjectName DBOutputormatDemo
 * @PackageName
 * @ClassName MysqlDBOutputormatDemo
 * @Description TODO
 * @Author
 * @Date 2017年12月27日14:17:13 重复执行会报主键冲突
 */
public class MysqlDBOutputormatDemo extends Configured implements Tool {

	String jdbcDri = PropertyUtil.getProperty("jdbcDri");
	String username = PropertyUtil.getProperty("username");
	String password = PropertyUtil.getProperty("password");

	static String bus_code = "";
	// MysqlDBOutputormatDemo(String bus_code,String algorithm_type){
	// this.bus_code= bus_code;
	// this.algorithm_type = algorithm_type;
	// }

	public static class DBOutputMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}

	public static class DBOutputReducer extends Reducer<LongWritable, Text, TblsWritable, TblsWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// values只有一个值，因为key没有相同的
			StringBuilder value = new StringBuilder();
			for (Text text : values) {
				value.append(text);
			}
			// [25, 12781_1.32]
			// 6026,12451_3.96,11968_3.74,12752_2.53
			String[] spiltResult = value.toString().split("\t");
			System.out.println(spiltResult.length);
			System.out.println(spiltResult.toString());
			if (StringUtils.isNotBlank(spiltResult[0])) {
				String user_id = spiltResult[0].trim(); // 6026
				String prod_nos = spiltResult[1]; // 12451_3.96,11968_3.74,12752_2.53
				if (prod_nos.contains("[")) {
					prod_nos = prod_nos.substring(1, prod_nos.length() - 1);
				}
				String algorithm_type = spiltResult[2];
				String bus_code = spiltResult[3];
				List<String> prods = new ArrayList<String>();
				prods.add(prod_nos);
				String prodStr = prods.toString().substring(1, prods.toString().length() - 1);
				String algorithm_date = DefDateformat.getStringDateShort();
				TblsWritable tool = new TblsWritable(user_id, prodStr, bus_code, algorithm_type, algorithm_date);
				context.write(tool, null);

			}
		}
	}

	public static void main(String[] args) {
		try {
			start("hdfs://192.168.152.6:8020//foriseholdings/Algorithm/itemcf/20171231/step6_output/");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void start(String hdfs) throws Exception {
		// 数据输入路径和输出路径
		String[] args0 = { hdfs };
		ToolRunner.run(new Configuration(), new MysqlDBOutputormatDemo(), args0);
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
		job.setJarByClass(MysqlDBOutputormatDemo.class);

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

		// 输出到哪些表、字段
		DBOutputFormat.setOutput(job, "elep_recommended_sort", "user_id", "prods", "bus_code", "algorithm_type",
				"algorithm_date");
		return job.waitForCompletion(true) ? 0 : 1;
	}
}