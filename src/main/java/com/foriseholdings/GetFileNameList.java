package com.foriseholdings;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class GetFileNameList {

	// public static void main(String[] args) {
	// traverseFolder2("E:\\home\\bigdata\\ftp\\BC1001");
	// }

	List<String> fileNameLists = new ArrayList<String>();

	public void traverseFolder2(String path) {
		File file = new File(path);
		if (file.exists()) {
			File[] files = file.listFiles();
			if (files.length == 0) {
				System.out.println("文件夹是空的!");
			} else {
				for (File file2 : files) {
					if (file2.isDirectory()) {
						System.out.println("文件夹:" + file2.getAbsolutePath());
						traverseFolder2(file2.getAbsolutePath());
					} else {
						System.out.println("文件:" + file2.getAbsolutePath());
						fileNameLists.add(file2.getAbsolutePath());
					}
				}
			}
		} else {
			System.out.println("文件不存在!");
		}
	}

	public List<String> getFileNameLists(String path) {
		traverseFolder2(path);
		return fileNameLists;

	}

}