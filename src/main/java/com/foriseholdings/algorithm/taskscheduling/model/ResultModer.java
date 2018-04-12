package com.foriseholdings.algorithm.taskscheduling.model;

public class ResultModer {
	private String jobTime;// 任务时间
	private String busCode;// 业务类型
	private String ftpPath;// ftp路径
	private String mrType; //任务类型

	public String getMrType() {
		return mrType;
	}

	public void setMrType(String mrType) {
		this.mrType = mrType;
	}

	private String id;// 任务id
	private String code; //代码

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getFtpPath() {
		return ftpPath;
	}

	public void setFtpPath(String ftpPath) {
		this.ftpPath = ftpPath;
	}
	
	
	public String getJobTime() {
		return jobTime;
	}

	public void setJobTime(String jobTime) {
		this.jobTime = jobTime;
	}

	public String getBusCode() {
		return busCode;
	}

	public void setBusCode(String busCode) {
		this.busCode = busCode;
	}


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
