package com.lenovo.iot.mqtt.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.lenovo.iot.util.Config;
import com.lenovo.iot.util.MqttHttpServer;

@Service
public class MqttHttpService implements IMqttHttpService {
	private static int PORT = 0;
	private static final String URI_NODES = "/api/nodes";
	private static final String URI_METRICS = "/api/metrics";
	private static final String URI_STATS = "/api/stats";
	private static final String URI_CLIENTS = "/api/clients";
	private static final String URI_SESSIONS = "/api/sessions";
	private static final String URI_TOPICS = "/api/topics";
	private static final String URI_SUBSCRIPTIONS = "/api/subscriptions";
	
//	private static Logger log = Logger.getLogger("mqtt");
    
	@Autowired
	private MqttHttpServer mqttServer;

	@Autowired
	private Config config;
	
	@Override
	public String get_nodes() throws Exception {
		String nodes = mqttServer.Get(config.getMqtt_broker_nodes() + URI_NODES);
		
		List<String> list_nodes = new ArrayList<String>();
		JsonParser parser = new JsonParser();
		JsonArray ja = (JsonArray)parser.parse(nodes);
		for(Object o : ja) {
			JsonObject jo = (JsonObject)o;
			
			String name = jo.get("name").getAsString();
			String ip = name.split("@")[1];
			
			list_nodes.add(ip);
		}
		
		return new Gson().toJson(list_nodes);
	}

	// 查询合并所有节点指标和统计信息
	@Override
	public String get_metrics() throws Exception {
		String nodes = this.get_nodes();
		
		JsonParser parser = new JsonParser();
		JsonArray list_nodes = parser.parse(nodes).getAsJsonArray();
		if(list_nodes.size() == 0) {
			throw new IllegalArgumentException("no nodes in cluster");
		}
		
		CountDownLatch cdl = new CountDownLatch(list_nodes.size());
		
		//多线程分节点查询
		final List<JsonObject> list_result = new ArrayList<JsonObject>(list_nodes.size());
		for(final JsonElement o : list_nodes) {
			final String ip = o.getAsString();

			new Thread(new Runnable() {

				@Override
				public void run() {
					// TODO Auto-generated method stub
					JsonObject result = new JsonObject();
					
					//查询指标
					try {
						JsonObject jo = parser.parse(get_metrics(ip)).getAsJsonObject();
						result.add("messages/received", jo.get("messages/received"));
						result.add("messages/sent", jo.get("messages/sent"));
						result.add("messages/dropped", jo.get("messages/dropped"));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
					}
					
					//查询统计信息
					try {
						JsonObject jo = parser.parse(get_stats(ip)).getAsJsonObject();
						result.add("clients/count", jo.get("clients/count"));
						result.add("sessions/count", jo.get("sessions/count"));
						result.add("subscriptions/count", jo.get("subscriptions/count"));
					} catch (Exception e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
					}
					
					list_result.add(result);
			
					cdl.countDown();
				}
				
			}).start();
		}
		cdl.await();
		
		//合并结果
		long received_tatal = 0, sent_tatal = 0, dropped_total = 0;
		long clients_tatal = 0, sessions_tatal = 0, subscriptions_total = 0; //, topics_total = 0;
		for(JsonObject jo : list_result) {				
			long received = jo.get("messages/received").getAsLong();
			long sent = jo.get("messages/sent").getAsLong();
			long dropped = jo.get("messages/dropped").getAsLong();
			long clients = jo.get("clients/count").getAsLong();
			long sessions = jo.get("sessions/count").getAsLong();
			long subscriptions = jo.get("subscriptions/count").getAsLong();
			//long topics = jo.get("topics/count").getAsLong();
			
			received_tatal += received;
			sent_tatal += sent;
			dropped_total += dropped;
			clients_tatal += clients;
			sessions_tatal += sessions;
			subscriptions_total += subscriptions;
			//topics_total += topics;
		}
		
		JsonObject result = new JsonObject();
		result.addProperty("messages/received", received_tatal);
		result.addProperty("messages/sent", sent_tatal);
		result.addProperty("messages/dropped", dropped_total);
		result.addProperty("clients/count", clients_tatal);
		result.addProperty("sessions/count", sessions_tatal);
		result.addProperty("subscriptions/count", subscriptions_total);
		
		//查询主题个数
		//EMQ的统计信息不准确，因此需要重新查询
		//主题在各个节点中共享，因此查询一个节点即可
		try {
			String r = get_topics(list_nodes.get(0).getAsString(), "curr_page=1&page_size=1");
			JsonObject jo = parser.parse(r).getAsJsonObject();
			result.add("topics/count", jo.get("totalNum"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
		}
		
		return result.toString();
	}

	public String get_metrics(String ip) throws Exception {
		String url = getQueryString(ip, URI_METRICS, null);
		return mqttServer.Get(url);
	}

	public String get_stats(String ip) throws Exception {
		String url = getQueryString(ip, URI_STATS, null);
		return mqttServer.Get(url);
	}

	@Override
	public String get_clients(String ip, String queryString) throws Exception {
		String url = getQueryString(ip, URI_CLIENTS, queryString);
		return mqttServer.Get(url);
	}

	@Override
	public String get_sessions(String ip, String queryString) throws Exception {
		String url = getQueryString(ip, URI_SESSIONS, queryString);
		return mqttServer.Get(url);
	}

	@Override
	public String get_topics(String ip, String queryString) throws Exception {
		String url = getQueryString(ip, URI_TOPICS, queryString);
		return mqttServer.Get(url);
	}

	@Override
	public String get_subscriptions(String ip, String queryString) throws Exception {
		String url = getQueryString(ip, URI_SUBSCRIPTIONS, queryString);
		return mqttServer.Get(url);
	}
	
	
	private String getQueryString(String ip, String uri, String queryString) {
		if(PORT == 0) {
			try {
				PORT = new URL(config.getMqtt_broker_nodes()).getPort();
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				PORT = 18083;
			}
		}
		
		StringBuilder result = new StringBuilder().append("http://").append(ip).append(":").append(PORT).append(uri);
		
		if(queryString != null && !queryString.isEmpty()) {
			result.append("?").append(queryString);
		}
		
		return result.toString();
	}
}
