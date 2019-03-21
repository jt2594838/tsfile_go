package com.lenovo.iot.auth.model;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.springframework.jdbc.core.RowMapper;

public class Device {
	private static final String SK = "secret_key";

	private Long id;
	private String device_id;
	private long company_id;
	private String policy_name;
	private String access_key;
	private String secret_key;
	private String device_desc;
	private Timestamp create_stamp;
	private Timestamp update_stamp;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getDevice_id() {
		return device_id;
	}

	public void setDevice_id(String device_id) {
		this.device_id = device_id;
	}

	public long getCompany_id() {
		return company_id;
	}

	public void setCompany_id(long company_id) {
		this.company_id = company_id;
	}

	public String getPolicy_name() {
		return policy_name;
	}

	public void setPolicy_name(String policy_name) {
		this.policy_name = policy_name;
	}

	public String getAccess_key() {
		return access_key;
	}

	public void setAccess_key(String access_key) {
		this.access_key = access_key;
	}

	public String getSecret_key() {
		return secret_key;
	}

	public void setSecret_key(String secret_key) {
		this.secret_key = secret_key;
	}

	public String getDevice_desc() {
		return device_desc;
	}

	public void setDevice_desc(String device_desc) {
		this.device_desc = device_desc;
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

	public static RowMapper<Device> getMap() {
		return MAP;
	}
	
	public Device() {}
	
	private void writeObject(java.io.ObjectOutputStream s) throws IOException {
		throw new IOException("Serialization not supported");
	}

	//RowMapper
	 public static final RowMapper<Device> MAP = new RowMapper<Device>(){
		    public Device mapRow(ResultSet rs, int rowNum) throws SQLException {
		    	Device obj = new Device();
		    	obj.setId(rs.getLong("id"));
		    	obj.setDevice_id(rs.getString("device_id"));
		    	obj.setCompany_id(rs.getLong("company_id"));
		    	obj.setPolicy_name(rs.getString("policy_name"));
		    	obj.setAccess_key(rs.getString("access_key"));
		    	obj.setSecret_key(rs.getString(SK));
		    	obj.setDevice_desc(rs.getString("device_desc"));
		    	obj.setCreate_stamp(rs.getTimestamp("create_stamp"));
		    	obj.setUpdate_stamp(rs.getTimestamp("update_stamp"));
		    	
		    	return obj;
	    }
	};
}