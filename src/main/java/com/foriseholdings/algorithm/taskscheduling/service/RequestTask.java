package com.foriseholdings.algorithm.taskscheduling.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class RequestTask {
	public static final String GET_URL = "http://192.168.92.7:8880/task/getTask/ALG/1";
	public static final String POST_URL = "http://192.168.92.7:8888/task/update/";
	static Logger logger ;
	/**
	 * 接口调用 GET
	 * @return
	 */
	public static String httpURLConectionGET(String urlString) {
		try {
			URL url = new URL(urlString); // 把字符串转换为URL请求地址
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();// 打开连接
			connection.connect();// 连接会话
			// 获取输入流
			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(), "UTF-8"));
			String line;
			StringBuilder sb = new StringBuilder();
			while ((line = br.readLine()) != null) {// 循环读取流
				sb.append(line);
			}
			br.close();// 关闭流
			connection.disconnect();// 断开连接
			String orgJson = sb.toString().replaceAll("@", "//");
			return orgJson;
		} catch (Exception e) {
			e.printStackTrace();
			return "fail";
		}
	}

	/**
	 * 
	 * @param urlPath 请求路径
	 * @param json  post-Body 请求体
	 * @return
	 * @throws Exception
	 */
	public static int postBody(String urlPath, String json) throws Exception {
		try {
			// Configure and open a connection to the site you will send the request
			URL url = new URL(urlPath);
			HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
			// 设置doOutput属性为true表示将使用此urlConnection写入数据
			urlConnection.setDoOutput(true);
			// 定义待写入数据的内容类型，我们设置为application/x-www-form-urlencoded类型
			urlConnection.setRequestProperty("content-type", "application/json");
			urlConnection.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");

			// 得到请求的输出流对象
			OutputStreamWriter out = new OutputStreamWriter(urlConnection.getOutputStream());
			// 把数据写入请求的Body
			out.write(json);
			out.flush();
			out.close();
			
			// 从服务器读取响应
			InputStream inputStream = urlConnection.getInputStream();
			String encoding = urlConnection.getContentEncoding();
			String body = IOUtils.toString(inputStream, encoding);
			if (urlConnection.getResponseCode() == 200) {
				return 200;
			} else {
				throw new Exception(body);
			}
		} catch (IOException e) {
			throw e;
		}
	}
	
	public static String sendPost(String jsonStr, String urlPath) {
		StringBuilder sb = new StringBuilder();
		try {

			URL url = new URL(urlPath);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			 conn.setRequestProperty("Content-Type", "application/json");

			String authString = "kettle:kettle";
			byte[] authEncBytes = Base64.encodeBase64(authString.getBytes());
			String authStringEnc = new String(authEncBytes);
			conn.setRequestProperty("Authorization", "Basic " + authStringEnc);
			OutputStream os = conn.getOutputStream();
			os.write(jsonStr.getBytes("utf-8"));
			os.flush();
			if (conn.getResponseCode() != HttpURLConnection.HTTP_OK
					&& conn.getResponseCode() != HttpURLConnection.HTTP_CREATED) {
				throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
			}
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream()), "UTF-8"));
			String output;
			while ((output = br.readLine()) != null) {
				sb.append(output);
			}
			conn.disconnect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		return sb.toString();
	}
}