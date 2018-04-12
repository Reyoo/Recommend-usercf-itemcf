package com.foriseholdings.adsLabel.findListFromMysql.bean;

public class ResultBean {

	private String code;
	private String message;
	private ProdIdsBean result;

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public ProdIdsBean getResult() {
		return result;
	}

	public void setResult(ProdIdsBean result) {
		this.result = result;
	}

}
