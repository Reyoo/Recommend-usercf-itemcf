package com.foriseholdings.adsLabel.write2Mysql.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class AdTargetWritable implements Writable, DBWritable {

	private String user_id; // 用戶ID
	private String bus_code;

	// 计算时间
	private String label_id;;
	// 计算类型
	private String label_value;
	//是否成立 '0' 否， '1' 是
	private String label_true;

	private String timestrap;

	// 业务系统类型

	public AdTargetWritable() {
	}

	// public TblsWritable(String user_id, String pids,String algorithm_type,String
	// algorithm_date) {
	public AdTargetWritable(String uid, String bus_code, String label_id, String label_value, String label_true,String timestrap) {
		this.user_id = uid;
		this.bus_code = bus_code;
		this.label_id = label_id;
		this.label_value = label_value;
		this.label_true = label_true;
		this.timestrap = timestrap;
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		int i = 1;
		statement.setString(i++, this.user_id);
		statement.setString(i++, this.label_id);
		statement.setString(i++, this.label_value);
		statement.setString(i++, this.label_true);
		statement.setString(i++, this.bus_code);
		statement.setString(i++, this.timestrap);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		int i = 1;
		this.user_id = resultSet.getString(i++);
		this.label_id = resultSet.getString(i++);
		this.label_value = resultSet.getString(i++);
		this.label_true = resultSet.getString(i++);
		this.bus_code = resultSet.getString(i++);
		this.timestrap = resultSet.getString(i++);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.user_id);
		out.writeUTF(this.bus_code);
		out.writeUTF(this.label_id);
		out.writeUTF(this.label_value);
		out.writeUTF(this.label_true);
		out.writeUTF(this.timestrap);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.user_id = in.readUTF();
		this.bus_code = in.readUTF();
		this.label_id = in.readUTF();
		this.label_value = in.readUTF();
		this.label_true = in.readUTF();
		this.timestrap = in.readUTF();

	}

	public String toString() {
		return new String(
				this.user_id + " " + this.bus_code + " " + this.label_id + " " + this.label_value + " " + this.label_true + " " + this.timestrap);
		// return new String(this.user_id+ " " +this.prods+ " " + this.algorithm_date+ "
		// " + this.algorithm_type);
	}
}