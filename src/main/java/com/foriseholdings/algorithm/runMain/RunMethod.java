package com.foriseholdings.algorithm.runMain;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.foriseholdings.common.common.BaseMapper;
import com.foriseholdings.common.common.BaseReducer;
import com.foriseholdings.common.util.PropertyUtil;

/**
 * @author qisun MapReduce 核心代码
 */
public class RunMethod {

	public static String hdfs = PropertyUtil.getProperty("hdfs");
	public String buss_code;
	// 创建job配置类
	// Configuration conf = new Configuration();

	public int runTask(BaseMapper mapClass, BaseReducer reduceClass, String cache, String step, String inPath,
			String outPath, String buss_code, String type, String isSell) {
		try {
			// 创建Configutation 对象，用于设置其他选项
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", hdfs);
			conf.set("buss_code", buss_code);
			conf.set("type", type);
			conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
			conf.setStrings("onSellProd", isSell);
			// 创建作业对象
			Job job = Job.getInstance(conf, step.toString());
			// job.setJobName(name);
			// System.out.println("================"+cache+"===============");
			if (!cache.equals("")) {
				// System.out.println("输入");
				job.addCacheArchive(new URI(cache + "#cachelist"));
				// System.out.println("输入");
			}
			// 设置作业jarfile中 主类的名字
			job.setJarByClass(RunMethod.class);
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
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return -1;
	}
}
