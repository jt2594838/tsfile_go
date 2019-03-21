package com.lenovo.iot.mqtt.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.lenovo.iot.util.Config;
import com.lenovo.iot.util.MqttHttpServer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class MqttHttpServiceV2 implements IMqttHttpService {
	private static final String URI_NODES =			"/api/v2/management/nodes";
	private static final String URI_METRICS =		"/api/v2/monitoring/metrics";
	private static final String URI_STATS =			"/api/v2/monitoring/stats";

	private static final String URI_BASE =			"/api/v2/nodes/";
	private static final String URI_CLIENTS =		"/clients";
	private static final String URI_SESSIONS =		"/sessions";
	//private static final String URI_TOPICS =		"/topics";
	private static final String URI_SUBSCRIPTIONS =	"/subscriptions";
    
	@Autowired
	private MqttHttpServer mqttServer;

	@Autowired
	private Config config;
	
	@Override
	public String get_nodes() throws Exception {
		String nodes = mqttServer.Get(config.getMqtt_broker_nodes() + URI_NODES);
		
		List<String> list_nodes = new ArrayList<String>();
		JsonParser parser = new JsonParser();
		JsonObject ja = (JsonObject)parser.parse(nodes);
		if(ja.get("code").getAsInt() == 0) {
			for(Object o : ja.get("result").getAsJsonArray()) {
				JsonObject jo = (JsonObject)o;
				list_nodes.add(jo.get("name").getAsString());
			}
		}
		
		return new Gson().toJson(list_nodes);
	}

	// 查询合并所有节点指标和统计信息
	@Override
	public String get_metrics() throws Exception {
		long received_tatal = 0, sent_tatal = 0, dropped_total = 0;
		long clients_tatal = 0, sessions_tatal = 0, subscriptions_total = 0, topics_total = 0;

		JsonParser parser = new JsonParser();

		String metrics = mqttServer.Get(config.getMqtt_broker_nodes() + URI_METRICS);
		JsonObject ja = (JsonObject)parser.parse(metrics);
		if(ja.get("code").getAsInt() == 0) {
			for(Object o : ja.get("result").getAsJsonArray()) {
				JsonObject jo = (JsonObject)o;
				
				Set<Map.Entry<String, JsonElement>> entrySet = jo.entrySet();
				for(Map.Entry<String,JsonElement> entry : entrySet){
					JsonObject jo_metric = entry.getValue().getAsJsonObject();

					long received = jo_metric.get("messages/received").getAsLong();
					long sent = jo_metric.get("messages/sent").getAsLong();
					long dropped = jo_metric.get("messages/dropped").getAsLong();

					received_tatal += received;
					sent_tatal += sent;
					dropped_total += dropped;
				}
			}
		}

		String stats = mqttServer.Get(config.getMqtt_broker_nodes() + URI_STATS);
		ja = (JsonObject)parser.parse(stats);
		if(ja.get("code").getAsInt() == 0) {
			for(Object o : ja.get("result").getAsJsonArray()) {
				JsonObject jo = (JsonObject)o;
				
				Set<Map.Entry<String, JsonElement>> entrySet = jo.entrySet();
				for(Map.Entry<String,JsonElement> entry : entrySet){
					JsonObject jo_metric = entry.getValue().getAsJsonObject();

					long clients = jo_metric.get("clients/count").getAsLong();
					long sessions = jo_metric.get("sessions/count").getAsLong();
					long subscriptions = jo_metric.get("subscriptions/count").getAsLong();
					long topics = jo_metric.get("topics/count").getAsLong();

					clients_tatal += clients;
					sessions_tatal += sessions;
					subscriptions_total += subscriptions;
					topics_total = topics; //主题（路由）在各个节点中共享，因此查询一个节点即可
				}
			}
		}
		
		JsonObject result = new JsonObject();
		result.addProperty("messages/received", received_tatal);
		result.addProperty("messages/sent", sent_tatal);
		result.addProperty("messages/dropped", dropped_total);
		result.addProperty("clients/count", clients_tatal);
		result.addProperty("sessions/count", sessions_tatal);
		result.addProperty("subscriptions/count", subscriptions_total);
		result.addProperty("topics/count", topics_total);
		
		return result.toString();
	}

	@Override
	public String get_clients(String nodeName, String queryString) throws Exception {
		// api/v2/nodes/emq@127.0.0.1/clients
		// api/v2/nodes/emq@127.0.0.1/clients/{client_id}

		String url;
		String client_id = this.extractParam(queryString, "client_key");
		if(client_id != null) {
			url = getQueryString(nodeName, URI_CLIENTS, "/" + client_id);
		} else {
			url = getQueryString(nodeName, URI_CLIENTS, queryString);
		}
	
		String result = mqttServer.Get(url);
		// 结果转换
		return resultV1(result, "clientId");
	}

	@Override
	public String get_sessions(String nodeName, String queryString) throws Exception {
		// api/v2/nodes/emq@127.0.0.1/sessions
		// api/v2/nodes/emq@127.0.0.1/sessions/{client_id}

		String url;
		String client_id = this.extractParam(queryString, "client_key");
		if(client_id != null) {
			url = getQueryString(nodeName, URI_SESSIONS, "/" + client_id);
		} else {
			url = getQueryString(nodeName, URI_SESSIONS, queryString);
		}
	
		String result = mqttServer.Get(url);
		// 结果转换
		return resultV1(result, "clientId");
	}

	@Override
	public String get_topics(String nodeName, String queryString) throws Exception {
		// api/v2/routes
		// api/v2/routes/{topic}
		
		String topic = this.extractParam(queryString, "topics");

		StringBuilder url = new StringBuilder().append(config.getMqtt_broker_nodes()).append("/api/v2/routes");
		if(topic != null) {
			url.append("/").append(topic).toString();
		}
	
		String result = mqttServer.Get(url.toString());
		// 结果转换
		return resultV1(result, "");
	}

	@Override
	public String get_subscriptions(String nodeName, String queryString) throws Exception {
		// api/v2/nodes/emq@127.0.0.1/subscriptions
		// api/v2/subscriptions/{client_id}

		String url;
		String client_id = this.extractParam(queryString, "client_key");
		if(client_id != null) {
			url = new StringBuilder().append(config.getMqtt_broker_nodes()).append("/api/v2/subscriptions/").append(client_id).toString();
		} else {
			url = getQueryString(nodeName, URI_SUBSCRIPTIONS, queryString);
		}
	
		String result = mqttServer.Get(url);
		// 结果转换
		return resultV1(result, "clientid");
	}
	
	private String extractParam(String queryString, String param_key) {
		String param_value = null;

		if(queryString != null && queryString != "") {
			String[] params = queryString.split("&");
			for (String param : params) {
				String [] p = param.split("=");
				if(p[0].equalsIgnoreCase(param_key)) {
					if(p.length > 1) {
						param_value = p[1];
					}
					break;
				}
			}
		}

		return param_value;
	}

	private String getQueryString(String nodeName, String uri, String queryString) {
		StringBuilder result = new StringBuilder().append(config.getMqtt_broker_nodes()).append(URI_BASE).append(nodeName).append(uri);
		if(queryString != null) {
			result.append("?").append(queryString);
		}
		return result.toString();
	}

	private String resultV1(String result, String newClientKey) throws Exception {
		JsonParser parser = new JsonParser();
		JsonObject jo = (JsonObject)parser.parse(result);
		if(jo.get("code").getAsInt() == 0) {
			JsonObject r = jo.get("result").getAsJsonObject();
			r.add("currentPage", r.remove("current_page"));
			r.add("pageSize", r.remove("page_size"));
			r.add("totalNum", r.remove("total_num"));
			r.add("totalPage", r.remove("total_page"));

			JsonArray ja_clients = r.remove("objects").getAsJsonArray();
			for(Object o : ja_clients) {
				JsonObject jo_client = (JsonObject)o;
				if(jo_client.has("client_id")) {
					jo_client.add(newClientKey, jo_client.remove("client_id"));
				}
			}
			r.add("result", ja_clients);

			return r.toString();
		}

		throw new Exception("result code is " + jo.get("code"));
	}
}
