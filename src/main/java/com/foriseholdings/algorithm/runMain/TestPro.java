package com.foriseholdings.algorithm.runMain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.codec.binary.Base64;

public class TestPro {

	public static void main(String[] args) throws IOException {

		// String userCF_s2_5_caches = PropertyUtil.getProperty("topN_input");
		// System.out.println(userCF_s2_5_caches);
		String urlPath = "http://localhost:8880/ad/labelInfo?busCodeList=BC1001&labelIDList=测试性别";
		String jsonStr = "";
		System.out.println("hello world");
		System.out.println(uploadMessage(jsonStr,urlPath));
		
	}
	
	public static String uploadMessage(String jsonStr, String urlPath) {
		StringBuilder sb = new StringBuilder();
		try {

			URL url = new URL(urlPath);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			// conn.setRequestProperty("Content-Type", "application/xml");

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
