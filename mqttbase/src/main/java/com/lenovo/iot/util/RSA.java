package com.lenovo.iot.util;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import javax.crypto.Cipher;

import org.apache.log4j.Logger;

public class RSA {
	private static Logger log = Logger.getLogger("mqtt");
	
	private static final int KEY_SIZE = 512;
	private static final int BLOCK_SIZE = 53;
	private static final int OUTPUT_BLOCK_SIZE = 64;
	private static final String ALGORITHM = "RSA";
	private static final String ALGORITHM_CIPHER = "RSA/ECB/PKCS1PADDING";
	private static final String ALGORITHMS_SIGNATURE = "SHA256withRSA";

	public static String[] generateRSAKeyPair() throws Exception {
		String[] keypair = new String[2];

		KeyPairGenerator keyPairGen = KeyPairGenerator.getInstance(ALGORITHM);
		keyPairGen.initialize(KEY_SIZE);
		KeyPair keyPair = keyPairGen.generateKeyPair();

		PublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
		PrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();

		String publicKeyString = getKeyString(publicKey);
		keypair[0] = publicKeyString;

		String privateKeyString = getKeyString(privateKey);
		keypair[1] = privateKeyString;

		return keypair;
	}

	public static String decodeSecret(String privateKeyString, String content) throws Exception {
		Cipher rsaCipher = Cipher.getInstance(ALGORITHM_CIPHER);

		byte[] decoded = null;
		decoded = Base64.getDecoder().decode(content);
		Key privateKey = getPrivateKey(privateKeyString);

		rsaCipher.init(Cipher.DECRYPT_MODE, privateKey, new SecureRandom());
		int blocks = decoded.length / OUTPUT_BLOCK_SIZE;
		ByteArrayOutputStream decodedStream = new ByteArrayOutputStream(decoded.length);
		for (int i = 0; i < blocks; i++) {
			decodedStream.write(rsaCipher.doFinal(decoded, i * OUTPUT_BLOCK_SIZE, OUTPUT_BLOCK_SIZE));
		}

		return new String(decodedStream.toByteArray(), StandardCharsets.UTF_8);
	}

	public static String encodeSecret(String publicKeyString, String content) throws Exception {
		Cipher rsaCipher = Cipher.getInstance(ALGORITHM_CIPHER);

		Key publicKey = getPublicKey(publicKeyString);
		rsaCipher.init(Cipher.ENCRYPT_MODE, publicKey, new SecureRandom());
		byte[] data = content.getBytes(StandardCharsets.UTF_8);
		int blocks = data.length / BLOCK_SIZE;
		int lastBlockSize = data.length % BLOCK_SIZE;
		byte[] encryptedData = new byte[(lastBlockSize == 0 ? blocks : blocks + 1) * OUTPUT_BLOCK_SIZE];
		for (int i = 0; i < blocks; i++) {
			rsaCipher.doFinal(data, i * BLOCK_SIZE, BLOCK_SIZE, encryptedData, i * OUTPUT_BLOCK_SIZE);
		}
		if (lastBlockSize != 0) {
			rsaCipher.doFinal(data, blocks * BLOCK_SIZE, lastBlockSize, encryptedData, blocks * OUTPUT_BLOCK_SIZE);
		}

		return new String(Base64.getEncoder().encode(encryptedData));
	}

	private static String getKeyString(Key key) throws Exception {
		byte[] keyBytes = key.getEncoded();
		String s = new String(Base64.getEncoder().encode(keyBytes));
		return s;
	}

	public static PrivateKey getPrivateKey(String key) throws Exception {
		PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(Base64.getDecoder().decode(key));
		KeyFactory keyFactory = KeyFactory.getInstance(ALGORITHM);
		PrivateKey privateKey = keyFactory.generatePrivate(keySpec);
		return privateKey;
	}

	public static PublicKey getPublicKey(String key) throws Exception {
		X509EncodedKeySpec keySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(key));
		KeyFactory keyFactory = KeyFactory.getInstance(ALGORITHM);
		PublicKey publicKey = keyFactory.generatePublic(keySpec);
		return publicKey;
	}

	public static String signWithPrivateKey(String content, String privateKey) throws Exception {
		PrivateKey priKey = getPrivateKey(privateKey);

		Signature signature = Signature.getInstance(ALGORITHMS_SIGNATURE);
		signature.initSign(priKey);
		signature.update(content.getBytes(StandardCharsets.UTF_8));

		byte[] signed = signature.sign();

		return new String(Base64.getEncoder().encode(signed));
	}

	public static boolean verifyWithPublicKey(String content, String sign, String publicKey) throws Exception {
		try {
			PublicKey pubKey = getPublicKey(publicKey);
	
			Signature signature = Signature.getInstance(ALGORITHMS_SIGNATURE);
			signature.initVerify(pubKey);
			signature.update(content.getBytes(StandardCharsets.UTF_8));
	
			return signature.verify(Base64.getDecoder().decode(sign));
		} catch (Exception e) {
			log.error(e);
			//e.printStackTrace();
		}
		
		return false;
	}

/*
	private static void gensk() throws Exception {

		String publicKey = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAJetFlQy7ofOydSG1/Q/hf+hK3zhnJHIJmqFQhlkihR/faLAOAFHyIgoXMTEMudCVjTdAt0rrNPcM8NdVqaVwVkCAwEAAQ==";
		String privateKey = "MIIBVgIBADANBgkqhkiG9w0BAQEFAASCAUAwggE8AgEAAkEAl60WVDLuh87J1IbX9D+F/6ErfOGckcgmaoVCGWSKFH99osA4AUfIiChcxMQy50JWNN0C3Sus09wzw11WppXBWQIDAQABAkAXv/ydFpSPI2wrISozjrbWvk/m8BTqJuMNDuy4uSt95fI6mM0asu9dxQPb6JTfIDqKchwhOQe1mTXjLpDDyYABAiEA5jx0KMoVx7107pMnyBesQxZN4xT+M8oyZ/6JfkOI6AECIQCopiJbQ8Rv1erSGyXzxiEI0xrcC03uBUy1dPzbxVUZWQIhAKLcyrJ72RJU011cHPzuuf0uzuO+Wt8ZvbRQmsQFU9gBAiEAhYKP7EQnvIlSzmYjk2qkOEI3Hz/rv6R+Z6BnlOwRRHECIQC0tay7OPtf3sXykMC5sM817Ud3nbrY0Gzt+zsAgb9tsw==";
		
		JsonObject cer = new JsonObject();
		cer.addProperty("version", "1.0");
		cer.addProperty("auther", "LCIG Big Data BU, Lenovo 2018");
		cer.addProperty("type", 0);
		cer.addProperty("issue", System.currentTimeMillis());
		cer.addProperty("due", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2029-01-01 00:00:00").getTime());
		cer.addProperty("company_id", 100000);
		cer.addProperty("company_sk", "Sky king cover ground tiger, treasure tower shake river monster!");
		
		// 加密
		String cipherText = encodeSecret(publicKey, cer.toString());
		System.out.println("encrypted: " + cipherText);
		
		String plainText = decodeSecret(privateKey, cipherText);
		System.out.println("decrpted: " + plainText);
		
		
		String fileName = "device.sk";
		File file = new File(fileName);
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(file);
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
			
			bw.write("-----BEGIN CERTIFICATE-----");
			bw.newLine();
			
			int pos = 0;
			int len = cipherText.length();
			while (pos < len) {
				String line = cipherText.substring(pos, pos + ((len - pos > 64) ? 64 : len - pos));
				bw.write(line);
				bw.newLine();
				
				pos += 64;
			}
			
			bw.write("-----END CERTIFICATE-----");

			bw.close();
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			log.error(e);
			//e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error(e);
			//e.printStackTrace();
		}
	}
	
	private static void readsk() {
		
	}
	public static void main(String args[]) {
		try {
			// 创建共私密钥对
//			String[] keyPair = generateRSAKeyPair();
//			String publicKey = keyPair[0];
//			String privateKey = keyPair[1];

			String publicKey = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAJetFlQy7ofOydSG1/Q/hf+hK3zhnJHIJmqFQhlkihR/faLAOAFHyIgoXMTEMudCVjTdAt0rrNPcM8NdVqaVwVkCAwEAAQ==";
			String privateKey = "MIIBVgIBADANBgkqhkiG9w0BAQEFAASCAUAwggE8AgEAAkEAl60WVDLuh87J1IbX9D+F/6ErfOGckcgmaoVCGWSKFH99osA4AUfIiChcxMQy50JWNN0C3Sus09wzw11WppXBWQIDAQABAkAXv/ydFpSPI2wrISozjrbWvk/m8BTqJuMNDuy4uSt95fI6mM0asu9dxQPb6JTfIDqKchwhOQe1mTXjLpDDyYABAiEA5jx0KMoVx7107pMnyBesQxZN4xT+M8oyZ/6JfkOI6AECIQCopiJbQ8Rv1erSGyXzxiEI0xrcC03uBUy1dPzbxVUZWQIhAKLcyrJ72RJU011cHPzuuf0uzuO+Wt8ZvbRQmsQFU9gBAiEAhYKP7EQnvIlSzmYjk2qkOEI3Hz/rv6R+Z6BnlOwRRHECIQC0tay7OPtf3sXykMC5sM817Ud3nbrY0Gzt+zsAgb9tsw==";
			
			String cipherText = null;

			String fileName = "device.sk";
			File file = new File(fileName);
			BufferedReader reader = null;
			try {
				InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "UTF-8");
				reader = new BufferedReader(isr);
				String tempString = null;
				while ((tempString = reader.readLine()) != null) {
					if (!tempString.isEmpty()) {
						if (tempString.endsWith("-----BEGIN CERTIFICATE-----")) {
							cipherText = "";
							continue;
						} else if (tempString.endsWith("-----END CERTIFICATE-----")) {
							break;
						}
						
						cipherText += tempString;
					}
				}
				reader.close();
			} catch (IOException e) {
				log.error(e);
				//e.printStackTrace();
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e1) {
					}
				}
			}
			
			String plainText = decodeSecret(privateKey, cipherText);
			
			JsonParser parser = new JsonParser();
			JsonObject cer = parser.parse(plainText).getAsJsonObject();
			System.out.println(cer.get("company_id").getAsLong());
			System.out.println(cer.get("company_sk").getAsString());

			
			
//			// 私钥加密，公钥验签
//			// 签名
//			String signString = signWithPrivateKey(testString, privateKey);
//			// 验签
//			boolean result = verifyWithPublicKey(testString, signString, publicKey);
//			System.out.println("decrpted result: " + result);
//
//			// 公钥加密，私钥解密
//			// 加密
//			String cipherText = encodeSecret(publicKey, testString);
//			System.out.println("encrypted: " + cipherText);
//			// 解密
//			String plainText = decodeSecret(privateKey, cipherText);
//			System.out.println("decrpted: " + plainText);
		} catch (Exception e) {
			log.error(e);
			//e.printStackTrace();
		}
	}
*/
}
