package com.foriseholdings.upload2hdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.foriseholdings.GetFileNameList;
import com.foriseholdings.common.tools.DefDateformat;
import com.foriseholdings.common.tools.ReturnFileFormat;
import com.foriseholdings.common.util.PropertyUtil;

/**
 * 获取到文件的路径 将文件已map<目录名,List<文件名>> 的形式存放， 上传时 按照map 解析出文件目录 并上传
 * 
 * @author sunqi
 * @date 2018年1月18日09:46:47
 */
public class UploadToHdfs {

	public static final String hdfs = PropertyUtil.getProperty("hdfs");

	public static void main(String[] args) {
		System.out.println(DefDateformat.getCurrentTime("yyyy-MM-dd HH:mm:ss"));
		run("/BC1001/logdata/201804/01", "BC1001");
		System.out.println(DefDateformat.getCurrentTime("yyyy-MM-dd HH:mm:ss"));
	}

	// log_paths---> /BC1001/logdata/201804/01
	public static void run(String log_paths, String bus_code) {
		// /home/bigdata/ftp/BC1001/logdata/201804/01
		String log_path = "/home/bigdata/ftp" + log_paths;
		// log_path = "E:\\home\\bigdata\\ftp\\BC1001\\201712\\10";

		System.out.println("==============");
		System.out.println("log_path" + log_path);
		System.out.println("==============");
		// log_path = "E:\\home\\bigdata\\ftp\\BC1001\\201801\\03";
		Map<String, List<String>> folderMap = new HashMap<String, List<String>>();
		// String month_path = log_path.substring(0, log_path.length() - 3);
		String yearPath = log_path.substring(0, log_path.length() - 9);
		System.out.println(yearPath);
		List<String> folderLists = getFolderList(yearPath);
		if (folderLists.size() != 0) {
			// folderMap = getFileName(folderLists, yearPath);
			upload2Hdfs(yearPath, folderLists, bus_code);
		}

	}

	/**
	 * 循环调用上传文件到hdfs中 上传后删除 循环获取目录名称并放进list中去
	 */

	public static List<String> getFolderList(String realPath) {
		GetFileNameList getFileNameList = new GetFileNameList();
		List<String> folderList = new ArrayList<String>();
		String todayFile = DefDateformat.getStringDateShort(DefDateformat.subSomeDay(1));
		File files = new File(realPath);
		if (!files.exists()) {
			System.out.println(realPath + " not exists");
		}
		try {
			File file[] = files.listFiles();
			if (files.listFiles() != null) {
				for (int i = 0; i < file.length; i++) {
					File fs = file[i];
					if (fs.isDirectory()) {
						System.out.println(fs.getName());

						for (String fileName : getFileNameList.getFileNameLists(fs.toString())) {
							folderList.add(fileName);
						}

						// folderList.add(fs.getName());
						// if (folderList.contains(todayFile)) {
						// folderList.remove(todayFile);
						// }
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return folderList;
	}

	/**
	 * 获取到 含有成功上传日志的所有文件夹名称 并逐个上传文件 暂时弃用
	 */
	public static Map<String, List<String>> getFileName(List<String> folderNames, String realPath) {
		Map<String, List<String>> folderMap = new HashMap<String, List<String>>();
		String actPath = null;
		for (String folderName : folderNames) {
			List<String> fileNames = new ArrayList<String>();
			// System.out.println("folderName" + folderName);
			actPath = realPath + "/" + folderName;
			File files = new File(actPath);
			File file[] = files.listFiles();
			for (int i = 0; i < file.length; i++) {
				File fsName = file[i];
				fileNames.add(fsName.getName());
			}
			folderMap.put(folderName, fileNames);
		}
		return folderMap;
	}

	/**
	 * 上传到hdfs 根据月份
	 * 
	 * @param path
	 * @param forderAndFileNames
	 *            “文件夹” 名称和 “文件” 名称Map
	 * @param bus_code
	 *            业务编码
	 */
	public static void upload2Hdfs(String path, Map<String, List<String>> forderAndFileNames, String bus_code) {

		Configuration conf = new Configuration();
		String logPath = ReturnFileFormat.getPathAddr(PropertyUtil.getProperty("base_path"),
				PropertyUtil.getProperty("parse_inPath"), bus_code);

		FileSystem fs = null;
		try {
			URI uri;
			// uri = new URI("hdfs://192.168.92.215:8020");
			uri = new URI(hdfs);
			fs = FileSystem.get(uri, conf);
			String filePath = null;

			// 遍历map 拿到文件夹 及其下对应的文件名
			for (Map.Entry<String, List<String>> entry : forderAndFileNames.entrySet()) {
				List<String> fileNames = new ArrayList<String>();
				fileNames = entry.getValue();
				filePath = path + "//" + entry.getKey();
				for (String fileName : fileNames) {
					// Path resP = new Path(filePath);
					Path resP = new Path(filePath + "//" + fileName);
					Path destP = new Path(logPath);
					if (!fs.exists(destP)) {
						fs.mkdirs(destP);
					}
					// System.out.println("输出路径：" + destP);
					// GetFileNameList.traverseFolder2(resP);
					fs.copyFromLocalFile(resP, destP);
				}
			}
			fs.close();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static void upload2Hdfs(String path, List<String> forderAndFileNames, String bus_code) {

		Configuration conf = new Configuration();
		String logPath = ReturnFileFormat.getPathAddr(PropertyUtil.getProperty("base_path"),
				PropertyUtil.getProperty("parse_inPath"), bus_code);

		FileSystem fs = null;
		URI uri;
		try {
			// uri = new URI("hdfs://192.168.92.215:8020");

			uri = new URI(hdfs);
			try {
				fs = FileSystem.get(uri, conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String filePath = null;

			// 遍历map 拿到文件夹 及其下对应的文件名
			for (String abFilePath : forderAndFileNames) {
				List<String> fileNames = new ArrayList<String>();
				Path resP = new Path(abFilePath);
				Path destP = new Path(logPath);
				if (!fs.exists(destP)) {
					fs.mkdirs(destP);
				}
//				System.out.println("输出路径：" + destP);
				// GetFileNameList.traverseFolder2(resP);
				fs.copyFromLocalFile(resP, destP);
			}
			fs.close();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
