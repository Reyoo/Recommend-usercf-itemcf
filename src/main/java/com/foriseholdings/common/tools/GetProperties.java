package com.foriseholdings.common.tools;

import java.io.FileReader;
import java.util.Properties;

public class GetProperties {

		public static void main(String[] args) throws Exception{
			//通过FileReader将配置文件读取到内存中
			FileReader reader = new FileReader("address.properties");
			//创建一个Properties
			Properties pro = new Properties();
			//通过pro一个load方法将配置文件中的数据读取到map集合中
			pro.load(reader);
			//关闭流
			reader.close();
			//获取dbname的值
			String dbname = pro.getProperty("dbname");
		}
	}