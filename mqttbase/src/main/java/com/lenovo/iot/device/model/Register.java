package com.lenovo.iot.device.model;

import java.io.IOException;

public class Register {

	private String client_id;
	private String broker;
	private int port;
	private String device_id;
	private long company_id;
	private String secret_key;
	private String group;
	private int keep_alive;
	private boolean clean_session;
	private long timestamp;

	public String getClient_id() {
		return client_id;
	}

	public void setClient_id(String client_id) {
		this.client_id = client_id;
	}

	public String getBroker() {
		return broker;
	}

	public void setBroker(String broker) {
		this.broker = broker;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDevice_id() {
		return device_id;
	}

	public void setDevice_id(String device_id) {
		this.device_id = device_id;
	}

	public long getCompany_id() {
		return company_id;
	}

	public void setCompany_id(long company_id) {
		this.company_id = company_id;
	}

	public String getSecret_key() {
		return secret_key;
	}

	public void setSecret_key(String secret_key) {
		this.secret_key = secret_key;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public int getKeep_alive() {
		return keep_alive;
	}

	public void setKeep_alive(int keep_alive) {
		this.keep_alive = keep_alive;
	}

	public boolean isClean_session() {
		return clean_session;
	}

	public void setClean_session(boolean clean_session) {
		this.clean_session = clean_session;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public Register() {}
	
	private void writeObject(java.io.ObjectOutputStream s) throws IOException {
		throw new IOException("Serialization not supported");
	}
}
