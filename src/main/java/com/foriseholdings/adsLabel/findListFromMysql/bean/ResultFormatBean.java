package com.foriseholdings.adsLabel.findListFromMysql.bean;

public class ResultFormatBean {
	
	private String userID ;
	private String targetId;
	private String target_value;
	private String behave_times;
	
	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public String getTargetId() {
		return targetId;
	}
	public void setTargetId(String targetId) {
		this.targetId = targetId;
	}
	public String getTarget_value() {
		return target_value;
	}
	public void setTarget_value(String target_value) {
		this.target_value = target_value;
	}
	public String getBehave_times() {
		return behave_times;
	}
	public void setBehave_times(String behave_times) {
		this.behave_times = behave_times;
	}

}
