package com.lenovo.iot.auth.dao;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.lenovo.iot.auth.model.Device;

@Repository
public class AuthDao {
	
	@Autowired
	private JdbcTemplate jdbcTemplateMysql;
	
	private static final String SQL_GET_DEVICE = "select id, device_id, company_id, policy_name, access_key, secret_key, device_desc, create_stamp, update_stamp from device where device_id = ?";
	private static final String SQL_GET_DEVICE_BY_ACCESSKEY = "select id, device_id, company_id, policy_name, access_key, secret_key, device_desc, create_stamp, update_stamp from device where company_id = ? and access_key = ?";
	private static final int ZORO = 0;
	
	public Device getDevice(String device_id) throws Exception {
		List<Device> list = jdbcTemplateMysql.query(SQL_GET_DEVICE, new Object[]{ device_id }, Device.MAP);
		if(list != null && list.size() == 1 ){
			return (Device)list.get(ZORO);
		} else {
			throw new Exception("getDevice() return too many records");
		}
	}
	
	public Device getDeviceByAccessKey(long company_id, String access_key) throws Exception {
		List<Device> list = jdbcTemplateMysql.query(SQL_GET_DEVICE_BY_ACCESSKEY, new Object[]{ company_id, access_key }, Device.MAP);
		if(list != null && list.size() == 1 ){
			return (Device)list.get(ZORO);
		} else {
			throw new Exception("getDevice() return too many records");
		}
	}
}
