package com.lenovo.iot.device.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.springframework.jdbc.core.RowMapper;

public class DeviceOnlineHistory {

	private long id;
	private String device_id;
	private boolean is_online;
	private String ip;
	private String username;
	private String broker;
	private Timestamp create_stamp;
	private Timestamp online_stamp;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getDevice_id() {
		return device_id;
	}

	public void setDevice_id(String device_id) {
		this.device_id = device_id;
	}

	public boolean isIs_online() {
		return is_online;
	}

	public void setIs_online(boolean is_online) {
		this.is_online = is_online;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getBroker() {
		return broker;
	}

	public void setBroker(String broker) {
		this.broker = broker;
	}

	public Timestamp getCreate_stamp() {
		return create_stamp;
	}

	public void setCreate_stamp(Timestamp create_stamp) {
		this.create_stamp = create_stamp;
	}

	public Timestamp getOnline_stamp() {
		return online_stamp;
	}

	public void setOnline_stamp(Timestamp online_stamp) {
		this.online_stamp = online_stamp;
	}

	public DeviceOnlineHistory() {	}

	//RowMapper
	 public static final RowMapper<DeviceOnlineHistory> MAP = new RowMapper<DeviceOnlineHistory>(){
		    public DeviceOnlineHistory mapRow(ResultSet rs, int rowNum) throws SQLException {
		    	DeviceOnlineHistory obj = new DeviceOnlineHistory();
		    	obj.setId(rs.getLong("id"));
		    	obj.setDevice_id(rs.getString("device_id"));
		    	obj.setIs_online(rs.getBoolean("is_online"));
		    	obj.setIp(rs.getString("ip"));
		    	obj.setUsername(rs.getString("username"));
		    	obj.setBroker(rs.getString("broker"));
		    	obj.setCreate_stamp(rs.getTimestamp("create_stamp"));
		    	obj.setOnline_stamp(rs.getTimestamp("online_stamp"));
		    	
		    	return obj;
	    }
	};
}