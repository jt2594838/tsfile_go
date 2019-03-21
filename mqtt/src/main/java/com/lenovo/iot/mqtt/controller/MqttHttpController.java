package com.lenovo.iot.mqtt.controller;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.JsonObject;
import com.lenovo.iot.mqtt.service.IMqttHttpService;
import com.lenovo.iot.mqtt.service.MqttHttpService;
import com.lenovo.iot.mqtt.service.MqttHttpServiceV2;
import com.lenovo.iot.util.Config;

@RestController
@RequestMapping(value = "/mqtt")
//@AutoConfigureAfter(Config.class)
//@Import(Config.class)
public class MqttHttpController {
//	private Logger log = Logger.getLogger("mqtt");
	
	@Autowired
	private MqttHttpService mqttHttpServiceV1;

	@Autowired
	private MqttHttpServiceV2 mqttHttpServiceV2;

	@Autowired
	private Config config;

	private IMqttHttpService mqttHttpService;
	private Lock lock = new ReentrantLock();

	private IMqttHttpService getService() {
		if(mqttHttpService == null) {
			lock.lock();
			try {
				if(mqttHttpService == null) {
					if(config.getMqtt_broker_version().equalsIgnoreCase("v2")) {
						mqttHttpService = mqttHttpServiceV2;
					} else {
						mqttHttpService = mqttHttpServiceV1;
					}
					
					System.out.println();
					System.out.println("MQTT Api Version:\t" + config.getMqtt_broker_version());
					System.out.println("MQTT Api Server:\t" + config.getMqtt_broker_nodes());
					System.out.println("MQTT Service address:\t" + config.getMqtt_broker_address());
				}
			} finally {
				lock.unlock();
			}

		}
		
		return mqttHttpService;
	}

	/**
	 * 获取所有节点
	 * @param request
	 * @param response
	 * @return
	 */
	@RequestMapping(value = "/nodes", produces = { "application/json;charset=UTF-8" })
	private String get_nodes(HttpServletRequest request, HttpServletResponse response) {
		try {
			return getService().get_nodes();
		} catch (Exception e) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return getExceptionResult(e, request).toString();
		}
	}

	/**
	 * 获取所有节点的  metric+stats 并汇总
	 */
	@RequestMapping(value = "/metrics", produces = { "application/json;charset=UTF-8" })
	private String get_metrics(HttpServletRequest request, HttpServletResponse response) {
		try {
			return getService().get_metrics();
		} catch (Exception e) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return getExceptionResult(e, request).toString();
		}
	}

	/**
	 * 获取节点的 clients
	 * @param request
	 * @param response
	 * @return
	 */
	@RequestMapping(value = "/nodes/*/clients", produces = { "application/json;charset=UTF-8" })
	private String get_clients(HttpServletRequest request, HttpServletResponse response) {
		try {
			String uri = request.getRequestURI();
			String queryString = request.getQueryString();
			
			String nodeName = uri.split("/")[3];
			return getService().get_clients(nodeName, queryString);
		} catch (Exception e) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return getExceptionResult(e, request).toString();
		}
	}
	
	/**
	 * 获取节点的 sessions
	 * @param request
	 * @param response
	 * @return
	 */
	@RequestMapping(value = "/nodes/*/sessions", produces = { "application/json;charset=UTF-8" })
	private String get_session(HttpServletRequest request, HttpServletResponse response) {
		try {
			String uri = request.getRequestURI();
			String queryString = request.getQueryString();
			
			String nodeName = uri.split("/")[3];
			return getService().get_sessions(nodeName, queryString);
		} catch (Exception e) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return getExceptionResult(e, request).toString();
		}
	}
	
	/**
	 * 获取节点的 topics
	 * @param request
	 * @param response
	 * @return
	 */
	@RequestMapping(value = "/nodes/*/topics", produces = { "application/json;charset=UTF-8" })
	private String get_topic(HttpServletRequest request, HttpServletResponse response) {
		try {
			String uri = request.getRequestURI();
			String queryString = request.getQueryString();
			
			String nodeName = uri.split("/")[3];
			return getService().get_topics(nodeName, queryString);
		} catch (Exception e) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return getExceptionResult(e, request).toString();
		}
	}
	
	/**
	 * 获取节点的 subscription
	 * @param request
	 * @param response
	 * @return
	 */
	@RequestMapping(value = "/nodes/*/subscriptions", produces = { "application/json;charset=UTF-8" })
	private String get_subscription(HttpServletRequest request, HttpServletResponse response) {
		try {
			String uri = request.getRequestURI();
			String queryString = request.getQueryString();
			
			String nodeName = uri.split("/")[3];
			return getService().get_subscriptions(nodeName, queryString);
		} catch (Exception e) {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			return getExceptionResult(e, request).toString();
		}
	}
	
	private JsonObject getExceptionResult(Exception e, HttpServletRequest request) {
		//StringWriter sw = new StringWriter();
		//PrintWriter pw = new PrintWriter(sw);
		//e.printStackTrace(pw);
		
		JsonObject jo = new JsonObject();
		jo.addProperty("error", e.toString());
		jo.addProperty("url", new StringBuilder(request.getRequestURL()).append("?").append(request.getQueryString()).toString());
		//jo.addProperty("stackinfo", sw.toString());
		
		return jo;
	}
}
