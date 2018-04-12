package com.foriseholdings.adsLabel.findListFromMysql.bean;

//标签实体类
public class TargetBean {

	private String id;
	private String label_id;
	private String portrait_name;
	private Double portrait_rate;
	private String portrait_id;
	private String state; 
	private String remmend;
	private String timestrap;
	private String busCode;
	
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
	public String getLabel_id() {
		return label_id;
	}
	public void setLabel_id(String label_id) {
		this.label_id = label_id;
	}

	public String getPortrait_name() {
		return portrait_name;
	}
	public void setPortrait_name(String portrait_name) {
		this.portrait_name = portrait_name;
	}
	public Double getPortrait_rate() {
		return portrait_rate;
	}
	public void setPortrait_rate(Double portrait_rate) {
		this.portrait_rate = portrait_rate;
	}
	public String getPortrait_id() {
		return portrait_id;
	}
	public void setPortrait_id(String portrait_id) {
		this.portrait_id = portrait_id;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getRemmend() {
		return remmend;
	}
	public void setRemmend(String remmend) {
		this.remmend = remmend;
	}
	public String getTimestrap() {
		return timestrap;
	}
	public void setTimestrap(String timestrap) {
		this.timestrap = timestrap;
	}

}
