package com.foriseholdings.common.common;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.foriseholdings.parseLogs.ParsingJson;

public class BaseMapper extends Mapper<LongWritable, Text, Text, Text> {
	public String hdfs = "";
	public String inPath = "";
	public String outPath = "";
	protected Text outKey = new Text();
	protected Text outValue = new Text();
	// 缓存
	protected boolean haveCache = false;
	public List<String> cacheList = new ArrayList<String>();
	public Logger logger;
	protected LongWritable key;
	protected Text value;
	protected Context context;
	protected ParsingJson parseJson;
	protected static DecimalFormat df = new DecimalFormat("0.00");

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		this.key = key;
		this.value = value;
		this.context = context;

		if (!calcProc()) {
			return;
		}
	}

	protected boolean calcProc() {
		return true;
	}

	/**
	 * 缓存影响速度 需要修改
	 */

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		try {
			// 通过输入流将全局缓存中的 右侧矩阵 读入List<String> 中
			FileReader fr = new FileReader("cachelist");
			BufferedReader br = new BufferedReader(fr);

			// 每一行的格式是：行tab 列_值，列_值，列_值，列_值
			String line = null;
			while ((line = br.readLine()) != null) {
				cacheList.add(line);
			}
			fr.close();
			br.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return;
	}

	protected void write() {
		try {
			context.write(outKey, outValue);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	protected void getCosine() {

		String row_matrix1 = value.toString().split("\t")[0];
		// 列_值(数组)
		String[] column_value_array_matrix1 = value.toString().split("\t")[1].split(",");

		double denominator1 = 0;
		// 计算左侧矩阵的空间距离
		for (String column_value : column_value_array_matrix1) {
			String score = column_value.split("_")[1];
			denominator1 += Double.valueOf(score) * Double.valueOf(score);
		}
		denominator1 = Math.sqrt(denominator1);

		for (String line : cacheList) {
			// 右侧矩阵的行line
			// 格式 :行 tab 列_值，列_值，列_值，列_值，列_值，列_值
			String row_matrix2 = line.toString().split("\t")[0];
			String[] column_value_array_matrix2 = line.toString().split("\t")[1].split(",");
			double denominator2 = 0;
			// 计算右侧矩阵的空间距离
			for (String column_value : column_value_array_matrix2) {
				String score = column_value.split("_")[1];
				denominator2 += Double.valueOf(score) * Double.valueOf(score);
			}
			denominator2 = Math.sqrt(denominator2);

			// 矩阵两行相乘得到的结果
			double numerator = 0;
			// 遍历右侧矩阵的每一行的每一列
			for (String column_value_matrix1 : column_value_array_matrix1) {
				String column_matrix1 = column_value_matrix1.split("_")[0];
				String value_matrix1 = column_value_matrix1.split("_")[1];

				// 遍历右侧矩阵每一行的每一列
				for (String column_value_matrix2 : column_value_array_matrix2) {

					if (column_value_matrix2.startsWith(column_matrix1 + "_")) {
						String value_matrix2 = column_value_matrix2.split("_")[1];
						// 将两列的值成，并相加
						numerator += Double.valueOf(value_matrix1) * Double.valueOf(value_matrix2);
					}
				}
			}

			double cos = numerator / (denominator1 * denominator2);

			if (cos == 0) {
				continue;
			}

			// result 是结果矩阵中的某元素，为 行：row_matrix1 列：row_matrix2 因为右矩阵一已经转置
			outKey.set(row_matrix1);
			outValue.set(row_matrix2 + "_" + df.format(cos));
			try {
				context.write(outKey, outValue);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

	}

}