package com.lenovo.iot.mqtt.service;

public interface IMqttHttpService {

	public String get_nodes() throws Exception;

	// 查询合并所有节点指标和统计信息
	public String get_metrics() throws Exception;

	public String get_clients(String ip, String queryString) throws Exception;

	public String get_sessions(String ip, String queryString) throws Exception;

	public String get_topics(String ip, String queryString) throws Exception;

	public String get_subscriptions(String ip, String queryString) throws Exception;
}
