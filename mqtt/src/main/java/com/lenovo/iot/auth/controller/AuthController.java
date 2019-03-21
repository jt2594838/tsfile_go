package com.lenovo.iot.auth.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.lenovo.iot.auth.service.AuthService;

@RequestMapping("/auth")
@RestController
public class AuthController {

	//private Logger log = Logger.getLogger("register");

	@Autowired
	private AuthService mqttService;

	/* EMQTT Auth */
	@RequestMapping(value = "/auth.do", produces = { "text/plain; charset=UTF-8" })
//	public void Auth(String clientid, String username, String password, HttpServletResponse response) throws Exception {
	public void Auth(HttpServletRequest request, HttpServletResponse response) throws Exception {
		String clientid = request.getParameter("clientid");
		String username = request.getParameter("username");
		String password = request.getParameter("password");
		boolean result = mqttService.auth(clientid, username, password);
		if (result) {
			response.setStatus(HttpServletResponse.SC_OK);
		} else {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
		}
	}

	/* EMQTT Superuser */
	// 如果 emq_auth_http.conf 中配置了 auth.http.super_req，则开启 super user 认证。
	// 该认证访问此 url，如果通过表示为 super user
	// Super user 不进行 acl 认证，可以任意pub和sub
	@RequestMapping(value = "/superuser.do", produces = { "text/plain; charset=UTF-8" })
//	public void Superuser(String clientid, String username, HttpServletResponse response) throws Exception {
	public void Superuser(HttpServletRequest request, HttpServletResponse response) throws Exception {
		String clientid = request.getParameter("clientid");
		String username = request.getParameter("username");
		boolean result = mqttService.superuser(clientid, username);
		if (result) {
			response.setStatus(HttpServletResponse.SC_OK);
		} else {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
		}
	}

	/* EMQTT ACL */
	// access: 1 | 2, 1 = sub, 2 = pub
	@RequestMapping(value = "/acl.do", produces = { "text/plain; charset=UTF-8" })
//	public void ACL(String access, String username, String clientid, String ipaddr, String topic, HttpServletResponse response) throws Exception {
	public void ACL(HttpServletRequest request, HttpServletResponse response) throws Exception {
		String access = request.getParameter("access");
		String username = request.getParameter("username");
		String clientid = request.getParameter("clientid");
		String ipaddr = request.getParameter("ipaddr");
		String topic = request.getParameter("topic");
		boolean result = mqttService.acl(access, username, clientid, ipaddr, topic);
		if (result) {
			response.setStatus(HttpServletResponse.SC_OK);
		} else {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
		}
	}
}
