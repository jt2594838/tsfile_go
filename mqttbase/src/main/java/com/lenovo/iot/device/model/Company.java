package com.lenovo.iot.device.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.springframework.jdbc.core.RowMapper;

public class Company {

	private long company_id;
	private String company_name;
	private String company_sk;
	private String private_key;
	private String public_key;
	private Timestamp create_stamp;
	private Timestamp update_stamp;
	
	public Company() {}

	public long getCompany_id() {
		return company_id;
	}

	public void setCompany_id(long company_id) {
		this.company_id = company_id;
	}

	public String getCompany_name() {
		return company_name;
	}

	public void setCompany_name(String company_name) {
		this.company_name = company_name;
	}

	public String getCompany_sk() {
		return company_sk;
	}

	public void setCompany_sk(String company_sk) {
		this.company_sk = company_sk;
	}

	public String getPrivate_key() {
		return private_key;
	}

	public void setPrivate_key(String private_key) {
		this.private_key = private_key;
	}

	public String getPublic_key() {
		return public_key;
	}

	public void setPublic_key(String public_key) {
		this.public_key = public_key;
	}

	public Timestamp getCreate_stamp() {
		return create_stamp;
	}

	public void setCreate_stamp(Timestamp create_stamp) {
		this.create_stamp = create_stamp;
	}

	public Timestamp getUpdate_stamp() {
		return update_stamp;
	}

	public void setUpdate_stamp(Timestamp update_stamp) {
		this.update_stamp = update_stamp;
	}

	public static RowMapper<Company> getMap() {
		return MAP;
	}

	//RowMapper
	 public static final RowMapper<Company> MAP = new RowMapper<Company>(){
		    public Company mapRow(ResultSet rs, int rowNum) throws SQLException {
		    	Company obj = new Company();
		    	obj.setCompany_id(rs.getLong("company_id"));
		    	obj.setCompany_name(rs.getString("company_name"));
		    	obj.setCompany_sk(rs.getString("company_sk"));
		    	obj.setPrivate_key(rs.getString("private_key"));
		    	obj.setPublic_key(rs.getString("public_key"));
		    	obj.setCreate_stamp(rs.getTimestamp("create_stamp"));
		    	obj.setUpdate_stamp(rs.getTimestamp("update_stamp"));
		    	
		    	return obj;
	    }
	};
}