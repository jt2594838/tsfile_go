package com.lenovo.iot.util;

import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * HTTP请求工具类
 */
public class HttpUtil {
	private static Logger log = Logger.getLogger("mqtt");
	
    /**
     * POST请求
     *
     * @param params
     * @return
     */
    public static String postQuery(String url, String params) {
        String result = null;
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost post = new HttpPost(url);
        CloseableHttpResponse response = null;
        try {
            post.setEntity(new ByteArrayEntity(params.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON));
            response = httpClient.execute(post);
            if (response != null) {
                int status = response.getStatusLine().getStatusCode();
                if (HttpServletResponse.SC_INTERNAL_SERVER_ERROR == status) {
                    throw new IllegalArgumentException(EntityUtils.toString(response.getEntity()));
                }
                if (status == HttpServletResponse.SC_OK) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        result = EntityUtils.toString(entity);
                    }
                }
            }
            return result;
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
            	log.error(e);
                throw (IllegalArgumentException) e;
            }
            log.error(e);
            //e.printStackTrace();
        } finally {
            try {
                response.close();
                httpClient.close();
            } catch (IOException e) {
            	log.error(e);
            	//e.printStackTrace();
            }
        }
        return null;
    }
}
