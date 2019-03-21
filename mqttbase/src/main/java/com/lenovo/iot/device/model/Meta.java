package com.lenovo.iot.device.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class Meta {

	private Long id;
	private String device_id;
	private String hardware_model;
	private String hardware_manufactor;
	private String hardware_os;
	private String hardware_os_version;
	private String hardware_location;
	private String firmware_name;
	private String firmware_version;
	private String host_ip;
	private Long update_stamp;

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

	public String getHardware_model() {
		return hardware_model;
	}

	public void setHardware_model(String hardware_model) {
		this.hardware_model = hardware_model;
	}

	public String getHardware_manufactor() {
		return hardware_manufactor;
	}

	public void setHardware_manufactor(String hardware_manufactor) {
		this.hardware_manufactor = hardware_manufactor;
	}

	public String getHardware_os() {
		return hardware_os;
	}

	public void setHardware_os(String hardware_os) {
		this.hardware_os = hardware_os;
	}

	public String getHardware_os_version() {
		return hardware_os_version;
	}

	public void setHardware_os_version(String hardware_os_version) {
		this.hardware_os_version = hardware_os_version;
	}

	public String getHardware_location() {
		return hardware_location;
	}

	public void setHardware_location(String hardware_location) {
		this.hardware_location = hardware_location;
	}

	public String getFirmware_name() {
		return firmware_name;
	}

	public void setFirmware_name(String firmware_name) {
		this.firmware_name = firmware_name;
	}

	public String getFirmware_version() {
		return firmware_version;
	}

	public void setFirmware_version(String firmware_version) {
		this.firmware_version = firmware_version;
	}

	public String getHost_ip() {
		return host_ip;
	}

	public void setHost_ip(String host_ip) {
		this.host_ip = host_ip;
	}

	public Long getUpdate_stamp() {
		return update_stamp;
	}

	public void setUpdate_stamp(Long update_stamp) {
		this.update_stamp = update_stamp;
	}

	public static RowMapper<Meta> getMap() {
		return MAP;
	}
	
	public Meta() {}

	//RowMapper
	 public static final RowMapper<Meta> MAP = new RowMapper<Meta>(){
		    public Meta mapRow(ResultSet rs, int rowNum) throws SQLException {
		    	Meta obj = new Meta();
		    	obj.setId(rs.getLong("id"));
		    	obj.setDevice_id(rs.getString("device_id"));
		    	obj.setHardware_model(rs.getString("hardware_model"));
		    	obj.setHardware_manufactor(rs.getString("hardware_manufactor"));
		    	obj.setHardware_os(rs.getString("hardware_os"));
		    	obj.setHardware_os_version(rs.getString("hardware_os_version"));
		    	obj.setHardware_location(rs.getString("hardware_location"));
		    	obj.setFirmware_name(rs.getString("firmware_name"));
		    	obj.setFirmware_version(rs.getString("firmware_version"));
		    	obj.setHost_ip(rs.getString("host_ip"));
		    	obj.setUpdate_stamp(rs.getLong("update_stamp"));
		    	
		    	return obj;
	    }
	};
}