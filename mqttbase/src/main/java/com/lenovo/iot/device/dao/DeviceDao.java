package com.lenovo.iot.device.dao;

import java.sql.Timestamp;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.lenovo.iot.device.model.Device;
import com.lenovo.iot.device.model.Meta;

@Repository
public class DeviceDao {

	//private Logger log = Logger.getLogger("device");
	private static final String SQL_GET_DEVICE_BY_ACCESSKEY = "select id, device_id, company_id, policy_name, access_key, secret_key, topic_group, device_desc, create_stamp, update_stamp, online from device where company_id = ? and access_key = ?";
	private static final int ZORO = 0;

	@Autowired
	private JdbcTemplate jdbcTemplateMysql;
	@Autowired
	private DataSourceTransactionManager transactionManager;

	public Device getDeviceByAccessKey(long company_id, String access_key) {
		List<Device> list = jdbcTemplateMysql.query(SQL_GET_DEVICE_BY_ACCESSKEY, new Object[]{ company_id, access_key }, Device.MAP);
		if(list != null && list.size() == 1 ){
			return (Device)list.get(ZORO);
		}

		return null;
	}

	public int addDevice(Device device) {
		String sql = "insert into device(device_id, company_id, policy_name, access_key, secret_key, topic_group, device_desc, create_stamp, update_stamp, online) values(?, ?, ?, ?, ?, ?, ?, now(), now(), 0)";
		int row = jdbcTemplateMysql.update(sql, new Object[]{ device.getDevice_id(), device.getCompany_id(), device.getPolicy_name(), device.getAccess_key(), device.getSecret_key(), device.getTopic_group(), device.getDevice_desc() });

		return row;
	}

	public int updateDeviceDesc(String device_id, String device_desc) {
		String sql = "update device set device_desc = ?, update_stamp=now() where device_id = ?";
		int row = jdbcTemplateMysql.update(sql, new Object[]{ device_desc, device_id });

		return row;
	}

//	public Meta getMeta(String device_id) {
//		String sql = "select id, device_id, hardware_model, hardware_manufactor, hardware_location, hardware_os, hardware_os_version, firmware_name, firmware_version, host_ip, update_stamp from device where device_id = ?";
//		List<Meta> list = jdbcTemplateMysql.query(sql, new Object[]{ device_id }, Meta.MAP);
//		if(list != null && list.size() == 1 ){
//			return (Meta)list.get(ZORO);
//		}
//
//		return null;
//	}
//
//	public int addMeta(Meta meta) {
//		String sql = "insert into device_meta(device_id, hardware_model, hardware_manufactor, hardware_location, hardware_os, hardware_os_version, firmware_name, firmware_version, host_ip, update_stamp) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//		int row = jdbcTemplateMysql.update(sql, new Object[]{ meta.getDevice_id(), meta.getHardware_model(), meta.getHardware_manufactor(), meta.getHardware_location(), meta.getHardware_os(), meta.getHardware_os_version(), meta.getFirmware_name(), meta.getFirmware_version(), meta.getHost_ip(), meta.getUpdate_stamp() });
//
//		return row;
//	}
//
//	public int updateMeta(Meta meta) {
//		String sql = "update device_meta set hardware_model=?, hardware_manufactor=?, hardware_location=?, hardware_os=?, hardware_os_version=?, firmware_name=?, firmware_version=?, host_ip=?, update_stamp=? where device_id = ?";
//		int row = jdbcTemplateMysql.update(sql, new Object[]{ meta.getHardware_model(), meta.getHardware_manufactor(), meta.getHardware_location(), meta.getHardware_os(), meta.getHardware_os_version(), meta.getFirmware_name(), meta.getFirmware_version(), meta.getHost_ip(), meta.getUpdate_stamp(), meta.getDevice_id() });
//
//		return row;
//	}
//
//	public int setOnlineStatus(final String device_id, final String username, final int status, final long ts, final String ipaddress) {
//		final String sql1 = "update device set online = ? where device_id = ?";
//		final String sql2 = "insert into device_online_history(device_id, is_online, ip, username, broker, online_stamp, create_stamp) values(?, ?, ?, ?, ?, ?, now())";
//
//		String tsString = String.format("%1$-13s", ts).replace(' ', '0');
//		final Timestamp timestamp = new Timestamp(Long.valueOf(tsString));
//
//		TransactionTemplate transTemp = new TransactionTemplate();
//		transTemp.setTransactionManager(transactionManager);
//		return transTemp.execute(new TransactionCallback<Integer>() {
//            public Integer doInTransaction(TransactionStatus ts) {
//                try {
//            		int row = jdbcTemplateMysql.update(sql1, new Object[]{ status, device_id });
//            		if(row > 0) {
//            			row = jdbcTemplateMysql.update(sql2, new Object[]{ device_id, status, ipaddress, username, "", timestamp });
//            		}
//
//                    return row;
//                } catch (RuntimeException e) {
//                    ts.setRollbackOnly();
//                    return -1;
//                }
//            }
//        });
//	}
}
