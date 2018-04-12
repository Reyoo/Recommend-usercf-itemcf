package com.foriseholdings.adsLabel;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.foriseholdings.common.common.BaseMapper;
import com.foriseholdings.common.common.BaseReducer;
import com.foriseholdings.common.util.PropertyUtil;

public class TargetRunMethod {

	public static String hdfs = PropertyUtil.getProperty("hdfs");
	public String buss_code;

	public int runTask(BaseMapper mapClass, BaseReducer reduceClass, String inPath, String outPath, String buss_code) {
		try {
			// 创建Configutation 对象，用于设置其他选项
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", hdfs);
			conf.set("buss_code", buss_code);
			// 创建作业对象
			Job job = Job.getInstance(conf, "target");
			job.getConfiguration();
			// 设置作业jarfile中 主类的名字
			job.setJarByClass(TargetRunMethod.class);
			// 设置job的Mapper类和Reducer类
			job.setMapperClass(mapClass.getClass());
			job.setReducerClass(reduceClass.getClass());

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// 设置Reducer 的输出类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileSystem fs = FileSystem.get(conf);

			// 设置输入和输出路径
			Path inputPath = new Path(inPath);
			if (fs.exists(inputPath)) {
				FileInputFormat.addInputPath(job, inputPath);
			}
			Path outputPath = new Path(outPath);
			fs.delete(outputPath, true);
			FileOutputFormat.setOutputPath(job, outputPath);
			return job.waitForCompletion(true) ? 1 : -1;

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}
}
