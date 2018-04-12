package com.foriseholdings.write2mysql.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class TblsWritable implements Writable, DBWritable {

	private String user_id;
	private String prods;

	// private List<String> prodsList;
	// 计算时间
	private String algorithm_date;
	// 计算类型
	private String algorithm_type;

	private String bus_code;

	// 业务系统类型

	public TblsWritable() {
	}

	// public TblsWritable(String user_id, String pids,String algorithm_type,String
	// algorithm_date) {
	public TblsWritable(String user_id, String pids, String bus_code, String algorithm_type, String algorithm_date) {
		// public TblsWritable(String user_id, List<String> pids,String algorithm_date)
		// {
		// prodsList = pids;
		this.user_id = user_id;
		this.prods = pids;
		this.algorithm_date = algorithm_date;
		this.algorithm_type = algorithm_type;
		this.bus_code = bus_code;
	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		int i = 1;
		statement.setString(i++, this.user_id);
		statement.setString(i++, this.prods);
		statement.setString(i++, this.bus_code);
		statement.setString(i++, this.algorithm_type);
		statement.setString(i++, this.algorithm_date);
	}

	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		int i = 1;
		this.user_id = resultSet.getString(i++);
		this.prods = resultSet.getString(i++);
		this.bus_code = resultSet.getString(i++);
		this.algorithm_type = resultSet.getString(i++);
		this.algorithm_date = resultSet.getString(i++);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.user_id);
		out.writeUTF(this.prods);
		out.writeUTF(this.bus_code);
		out.writeUTF(this.algorithm_type);
		out.writeUTF(this.algorithm_date);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.user_id = in.readUTF();
		this.prods = in.readUTF();
		this.bus_code = in.readUTF();
		this.algorithm_date = in.readUTF();
		this.algorithm_type = in.readUTF();

	}

	public String toString() {
		return new String(this.user_id + " " + this.prods + " " + this.algorithm_date + " " + this.algorithm_type + " "
				+ this.bus_code);
		// return new String(this.user_id+ " " +this.prods+ " " + this.algorithm_date+ "
		// " + this.algorithm_type);
	}
}