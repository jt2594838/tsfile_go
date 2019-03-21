package com.lenovo.iot.mqtt.model;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class MyStartupRunner implements CommandLineRunner {
	private static Logger log = Logger.getLogger("mqtt");
	@Autowired
    private JdbcTemplate jdbcTemplateMysql;
	
	@Override
	public void run(String... args) throws Exception {
		String deviceddl = "CREATE TABLE IF NOT EXISTS `device` (\r\n" + 
				"  `id` bigint(20) NOT NULL AUTO_INCREMENT,\r\n" + 
				"  `device_id` varchar(128) NOT NULL,\r\n" + 
				"  `company_id` bigint(20) NOT NULL,\r\n" + 
				"  `policy_name` varchar(64) DEFAULT NULL,\r\n" + 
				"  `access_key` varchar(128) NOT NULL,\r\n" + 
				"  `secret_key` varchar(128) DEFAULT NULL,\r\n" + 
				"  `device_desc` varchar(1024) DEFAULT NULL,\r\n" + 
				"  `create_stamp` timestamp NULL DEFAULT NULL,\r\n" + 
				"  `update_stamp` timestamp NULL DEFAULT NULL,\r\n" + 
				"  `online` int(11) DEFAULT '0',\r\n" + 
				"  `system` int(11) DEFAULT '0',\r\n" + 
				"  `topic_group` varchar(128) DEFAULT NULL,\r\n" + 
				"  PRIMARY KEY (`id`),\r\n" + 
				"  UNIQUE KEY `device_id` (`device_id`),\r\n" + 
				"  UNIQUE KEY `id_UNIQUE` (`id`)\r\n" + 
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8";
		String companyddl = "CREATE TABLE IF NOT EXISTS `company` (\r\n" + 
				"  `company_id` bigint(20) NOT NULL AUTO_INCREMENT,\r\n" + 
				"  `company_name` varchar(128) NOT NULL,\r\n" + 
				"  `address` varchar(128) DEFAULT NULL,\r\n" + 
				"  `contact` varchar(128) DEFAULT NULL,\r\n" + 
				"  `email` varchar(128) DEFAULT NULL,\r\n" + 
				"  `remark` varchar(128) DEFAULT NULL,\r\n" + 
				"  `company_sk` varchar(128) NOT NULL,\r\n" + 
				"  `private_key` varchar(512) DEFAULT NULL,\r\n" + 
				"  `public_key` varchar(512) DEFAULT NULL,\r\n" + 
				"  `device_sk` varchar(2048) DEFAULT NULL,\r\n" + 
				"  `create_stamp` timestamp NULL DEFAULT NULL,\r\n" + 
				"  `update_stamp` timestamp NULL DEFAULT NULL,\r\n" + 
				"  PRIMARY KEY (`company_id`),\r\n" + 
				"  UNIQUE KEY `company_id` (`company_id`)\r\n" + 
				") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8";
        int rows = jdbcTemplateMysql.update(deviceddl);
        log.info("create table iot.device finish. " + rows);
        rows = jdbcTemplateMysql.update(companyddl);
        log.info("create table iot.company finish. " + rows);
	}
}
