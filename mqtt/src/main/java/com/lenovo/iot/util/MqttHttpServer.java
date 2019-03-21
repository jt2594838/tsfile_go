package com.lenovo.iot.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.springframework.stereotype.Component;

@Component
public class MqttHttpServer {
	private static final String AK = "admin";
	private static final String SK = "public";
	private static final int BUF_LEN = 1024;

	private void writeObject(java.io.ObjectOutputStream s) throws IOException {
		throw new IOException("Serialization not supported");
	}

	public String Get(final String uri) throws Exception {
		URL url = new URL(uri);

		String authorization = Base64.getEncoder().encodeToString((AK + ":" + SK).getBytes(StandardCharsets.UTF_8));
		// byte[] postData = data.getBytes("UTF-8");
		// int postDataLength = postData.length;
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setDoOutput(true);
		conn.setInstanceFollowRedirects(false);
		conn.setRequestProperty("Authorization", "Basic " + authorization);
		// conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("charset", "utf-8");
		// conn.setRequestProperty("Content-Length", Integer.toString(postDataLength));
		conn.setUseCaches(false);
		conn.setRequestMethod("GET");
		conn.connect();

		// DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
		// wr.write(postData);
		// wr.close();

		String result = "";
		InputStream inStream;
		int status_code = conn.getResponseCode();
		if (status_code == HttpURLConnection.HTTP_OK || status_code == HttpURLConnection.HTTP_CREATED || status_code == HttpURLConnection.HTTP_ACCEPTED) {
			inStream = conn.getInputStream();
		} else {
			inStream = conn.getErrorStream();
		}
		if (inStream != null) {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			byte[] buffer = new byte[BUF_LEN];
			int len = 0;
			int pos = 0;
			// max loops of 20M
			for (int i = 0; i < 20000; i++) {
				len = inStream.read(buffer);
				if (len == -1)
					break;

				outStream.write(buffer, 0, len);
				pos += len;
			}
			// while ((len = inStream.read(buffer)) != -1) {
			// outStream.write(buffer, 0, len);
			// }
			inStream.close();
			result = new String(outStream.toByteArray());
		}

		conn.disconnect();

		return result;
	}
}
