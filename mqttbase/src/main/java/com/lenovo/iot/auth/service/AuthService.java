package com.lenovo.iot.auth.service;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lenovo.iot.auth.dao.AuthDao;
import com.lenovo.iot.auth.model.Device;
import com.lenovo.iot.auth.service.AuthService;

@Service
public class AuthService {
	
	@Autowired
	private AuthDao authDao;
	
	private Logger log = Logger.getLogger("register");
	
	/***
	 * 设备接入验证
	 * @param clientId 设备唯一标识
	 * @param username 用户注册时提供的设备标识
	 * @param password Secret Key
	 * @return
	 */
	public boolean auth(String clientId, String username, String sk) {
		log.debug("auth.do\t clientId: " + clientId + " username: " + username + " password: " + sk);

		Device device = null;
		try {
			int ver = Integer.parseInt(clientId.substring(0, 1));
			if(ver == 1) {
				long company_id = Long.parseLong(clientId.substring(1, 11));
				device = authDao.getDeviceByAccessKey(company_id, username);
			} else if(ver == 2) {
				long company_id = Long.parseLong(clientId.substring(1, 9));
				device = authDao.getDeviceByAccessKey(company_id, username);
			}
			if(device != null) {
				boolean result = device.getDevice_id().equals(clientId) && device.getSecret_key().equals(sk);
				if(result) {
					log.debug("register ok.");
				}
				return result;
			} else {
				log.debug("register failed, no device(access key) was found: " + clientId + ", " + username);
			}
		} catch (Exception e) {
			log.debug("register exception, cannot parse device id: " + clientId);
		}
		
		return false;
	}
	
	public boolean acl(String access, String username, String clientId, String ipAddr, String topic) {
		log.debug("acl.do\taccess:" + access + " username:" + username + " clientid:" + clientId + " ipaddr:" + ipAddr + " topic:" + topic);
		return true;
	}

	public boolean superuser(String clientid, String username) {
		log.debug("superuser.do\tclientid:" + clientid + " username:" + username);
		return true;
	}
}
