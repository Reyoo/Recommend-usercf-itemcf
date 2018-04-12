package com.foriseholdings.write2mysql.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class SimilarWritable implements Writable, DBWritable {

	private String prod_id;
	private String similar_prod_ids;
	private String buss_code;
	private String algorithm_type;
	private String algorithm_date;

	public SimilarWritable() {
	}

	public SimilarWritable(String prod_id, String similar_prod_ids, String buss_code, String algorithm_type,
			String algorithm_date) {
		this.prod_id = prod_id;
		this.similar_prod_ids = similar_prod_ids;
		this.buss_code = buss_code;
		this.algorithm_type = algorithm_type;
		this.algorithm_date = algorithm_date;

	}

	@Override
	public void write(PreparedStatement statement) throws SQLException {
		int i = 1;
		statement.setString(i++, this.prod_id);
		statement.setString(i++, this.similar_prod_ids);
		statement.setString(i++, this.buss_code);
		statement.setString(i++, this.algorithm_type);
		statement.setString(i++, this.algorithm_date);
	}
	
	
	@Override
	public void readFields(ResultSet resultSet) throws SQLException {
		int i = 1;
		this.prod_id = resultSet.getString(i++);
		this.similar_prod_ids = resultSet.getString(i++);
		this.buss_code = resultSet.getString(i++);
		this.algorithm_type = resultSet.getString(i++);
		this.algorithm_date = resultSet.getString(i++);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.prod_id);
		out.writeUTF(this.similar_prod_ids);
		out.writeUTF(this.buss_code);
		out.writeUTF(this.algorithm_type);
		out.writeUTF(this.algorithm_date);
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.prod_id = in.readUTF();
		this.similar_prod_ids = in.readUTF();
		this.buss_code = in.readUTF();
		this.algorithm_date = in.readUTF();
		this.algorithm_type = in.readUTF();

	}

	public String toString() {
		return new String(this.prod_id + " " + this.similar_prod_ids +" " + this.buss_code +  " " + this.algorithm_date + " " + this.algorithm_type );
	}

}
