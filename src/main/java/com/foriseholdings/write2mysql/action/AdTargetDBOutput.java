package com.foriseholdings.write2mysql.action;

import java.io.IOException;

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
import com.foriseholdings.write2mysql.bean.AdTargetWritable;

public class AdTargetDBOutput extends Configured implements Tool {

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

	public static class DBOutputReducer extends Reducer<LongWritable, Text, AdTargetWritable, AdTargetWritable> {
		// 6 20826_1,20934_9,20932_2,20930_2

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// SexLabel
			// values只有一个值，因为key没有相同的
			StringBuilder value = new StringBuilder();
			for (Text text : values) {
				value.append(text);
			}

			String user_id = value.toString().split("\t")[0];
			String label_id = value.toString().split("\t")[1].split("_")[0];
			String label_value = value.toString().split("\t")[1].split("_")[2];
			String bus_code = value.toString().split("\t")[1].split("_")[3];
			String timestrap = DefDateformat.getCurrentTime("yyyy-MM-dd HH:mm:ss");
			AdTargetWritable tool = new AdTargetWritable(user_id, bus_code, label_id, label_value, timestrap);
			context.write(tool, null);
		}
	}

	public static void main(String[] args) {
		try {
			start("hdfs://192.168.92.215:8020//foriseholdings/Algorithm/parseLog/20180206/parseTest/");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void start(String hdfs) throws Exception {
		// 数据输入路径和输出路径
		String[] args0 = { hdfs };
		 ToolRunner.run(new Configuration(), new AdTargetDBOutput(), args0);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// 读取配置文件
		Configuration conf = new Configuration();

		DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", jdbcDri, username, password);

		// 新建一个任务
		// Job job = new Job(conf, "DBOutputormatDemo");z
		Job job = Job.getInstance(conf, "DBOutputormatDemo");
		// 设置主类
		job.setJarByClass(AdTargetDBOutput.class);

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
		DBOutputFormat.setOutput(job, "elep_user_label_value", "user_id", "label_id", "label_value", "bus_code",
				"timestrap");
		return job.waitForCompletion(true) ? 0 : 1;
	}
}