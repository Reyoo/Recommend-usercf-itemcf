package com.foriseholdings.common.common;

import java.util.Date;

import com.foriseholdings.algorithm.runMain.RunMethod;
import com.foriseholdings.common.tools.DefDateformat;

public class BaseRunner {
	
	/**
	 *启动 run方法的基类 
	 */
	protected BaseMapper stepMapper ; 
	protected BaseReducer stepReducer;
	protected RunMethod runMain = new RunMethod();
	protected  String dateFormate = DefDateformat.getStringDateShort(new Date());
	
	protected int state = -1;
	//要修改成start() begin 是通常并发
	public boolean baseStart(String buss_code) {
		boolean flag = false;
		if(doAlgorithm()) { 
			flag= true;
//		System.out.println("begin的基类输出~");
		}
		
		if(endPoint()) {
			flag= true;
//			System.out.println("任务结束");
		}
		return flag;
	}
	
	public boolean doAlgorithm() {
		return true;
	}
	
	public boolean endPoint() {
		return true;
	}
	
	
	
	public boolean step1(String buss_code) {
		System.out.println("step1执行完毕");
		return true;
	}
	public boolean step2(String buss_code) {
		System.out.println("step2执行完毕");
		return true;
	}
	public boolean step3(String buss_code) {
		System.out.println("step3执行完毕");
		return true;
	}
	
	
	
}
