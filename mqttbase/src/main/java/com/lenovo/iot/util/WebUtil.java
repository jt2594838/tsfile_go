package com.lenovo.iot.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.util.CollectionUtils;

import com.google.gson.GsonBuilder;

public class WebUtil {

	public static final String WEB_INF = "/WEB-INF/";
	
	private WebUtil(){}

	public static void redirect(HttpServletRequest req, HttpServletResponse resp, String page) throws IOException {
		if(!page.startsWith("/") && !page.startsWith("http")) {
			page = "/" + page;
		}
		resp.sendRedirect(req.getContextPath() + page);
	}

	//Get parameter
	public static String getPara(final HttpServletRequest req, final String key){
		String para = req.getParameter(key);
		if(para==null){
			para = "";
		}
		//LogUtil.log("key="+key + ", para="+para);
		return para;
	}

	//Get parameter int
	public static int getParaInt(final HttpServletRequest req, final String key, int defaultValue){
		int result = defaultValue;
		String para = req.getParameter(key);
		if(para!=null && para.length()>0){
			result = Integer.parseInt(para);
		}
		return result;
	}
	
	public static long getParaLong(final HttpServletRequest req, final String key, long defaultValue){
		long result = defaultValue;
		String para = req.getParameter(key);
		if(para!=null && para.length()>0){
			result = Long.parseLong(para);
		}
		return result;
	}
	
	public static boolean getParaBoolean(final HttpServletRequest req, final String key, boolean defaultValue){
		boolean result = defaultValue;
		String para = req.getParameter(key);
		if(para!=null && para.length()>0){
			result = Boolean.parseBoolean(para);
		}
		return result;
	}

	//Get host
	public static String getHost(final HttpServletRequest req){
		String url = req.getRequestURL().toString();
		if(url.startsWith("http://moc")){
			url = url.replace("http://", "https://");
		}
		int p = url.indexOf("/",10);
		return url.substring(0, p);
	}

//	public static String md5(byte[] bytes) {
//		try{
//    		MessageDigest md = MessageDigest.getInstance("MD5");
//    	    md.update(bytes);    	    
//    	    StringBuffer buf=new StringBuffer();    	    
//    	    for(byte b:md.digest())
//    	    	buf.append(String.format("%02x", b&0xff) );    	     
//    	    return buf.toString();
//    	}catch( Exception e ){
//    		e.printStackTrace(); 
//    		return null;
//    	}
//	}

	//Get list string
	public static String getString(List<String> list){
        int size = list.size();
        StringBuilder sb = new StringBuilder(size*20);
        for(int i=0;i<size;i++){
        	if(i>0){
        		sb.append(",");
        	}
        	sb.append(list.get(i));
        }
        return sb.toString();
	}
	
//	//获取中文参数
//	public static String newUtf8String(String para) throws UnsupportedEncodingException{
//		return new String(para.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
//	}
	
	//格式化时间，返回yyyy-mm-dd HH:MM:SS
	public static String format(Timestamp time){
		String s = "";
		if(time!=null){
			s = time.toString().substring(0,19);
		}
		return s;
	}
	public static String format(Long time){
	    SimpleDateFormat formatter; 
	    formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
	    String ctime = formatter.format(time); 
	    return ctime; 
	}
	
	public static String listToString(List<String> list, String sign){
		StringBuilder builder = new StringBuilder();
		if(!CollectionUtils.isEmpty(list)){
			for(String str : list){
				builder.append(str).append(sign);
			}
			return builder.substring(0, builder.length()-1);
		}else{
			return builder.toString();
		}
		
	}
	
	public static String dateToString(Date time,String formatString){ 
	    SimpleDateFormat formatter; 
	    formatter = new SimpleDateFormat(formatString); 
	    String ctime = formatter.format(time); 
	    return ctime; 
	} 
	public static String NowString(String formatString){ 
	    return dateToString(new Date(),formatString); 
	}
	
//	//返回jsonp格式数据  
//	public static void response(HttpServletRequest request, HttpServletResponse response, String result) throws Exception {
//		response.setCharacterEncoding("utf-8");
//        response.getWriter().write(result);
//
////		//JSONP
////        PrintWriter out = response.getWriter();
////        String jsonpCallback = request.getParameter("jsonpCallback");
////        out.println(jsonpCallback + "(" + result + ")");
////        out.flush();  
////        out.close();
//	}
	
	/**
	 * 校验邮箱地址是否正确
	 * @param email
	 * @return
	 */
	public static boolean verifyEmail(String email) {
		if(email == null || "".equals(email.trim())) {
			return false;
		}
		String check = "^([a-z0-9A-Z]+[-|\\._]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";  
		Pattern regex = Pattern.compile(check);  
		Matcher matcher = regex.matcher(email);  
		return matcher.matches(); 
	}
}
