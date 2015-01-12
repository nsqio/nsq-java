package ly.bit.nsq.lookupd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DefaultLookup {
	private static final Logger log = LoggerFactory.getLogger(DefaultLookup.class);
	private static final int DEFAULT_CONNECT_TIME_OUT = 5000;
	private static final int DEFAULT_READ_TIME_OUT = 5000;
	private String lookupAddr;

	public DefaultLookup(String lookupAddr) {
		this.lookupAddr = lookupAddr;
	}
	
	public String getLookupAddr() {
		return lookupAddr;
	}

	public List<String> getTcpAddrs(String topic) {
		Map<String, List<String>> allAddr = getLookup(topic);
		if (allAddr == null) return null; 
		return allAddr.get("tcp");
	}
	
	public List<String> getHttpAddrs(String topic) {
		Map<String, List<String>> allAddr = getLookup(topic);
		if (allAddr == null) return null; 
		return allAddr.get("http");
	}
	
	public List<String> getHttpAddrs() {
		Map<String, List<String>> allAddr = getNodes();
		if (allAddr == null) return null; 
		return allAddr.get("http");
	}
	
	public String getAvailableHttpAddr(String topic) {
		List<String> allHttpAddr = getHttpAddrs(topic);
		if (allHttpAddr == null || allHttpAddr.size() < 1) return null;
		return allHttpAddr.get(0);
	}
	public String getAvailableHttpAddr() {
		List<String> allHttpAddr = getHttpAddrs();
		if (allHttpAddr == null || allHttpAddr.size() < 1) return null;
		return allHttpAddr.get(0);
	}
	
	private Map<String, List<String>> getLookup(String topic) {
		String lookupUrl = lookupAddr + "/lookup?topic=" + topic;
		return parse(httpReader(lookupUrl));
	}
	
	private Map<String, List<String>> getNodes() {
		String nodesUrl = lookupAddr + "/nodes";
		return parse(httpReader(nodesUrl));
	}
	
	private Map<String, List<String>> parse(Reader reader) {
		if (reader == null) return null;
		
		Map<String, List<String>> allAvailableAddr = new HashMap<String, List<String>>();
		ObjectMapper mapper = new ObjectMapper();
		try {	 
			 JsonNode rootNode = mapper.readTree(reader);
			 int statusCode = rootNode.path("status_code").getIntValue();
			 if (statusCode != 200) return null;
			 
			 JsonNode producers = rootNode.path("data").path("producers");
			 List<String> tcpProducers = new ArrayList<String>();
			 List<String> httpProducers = new ArrayList<String>();
			 Iterator<JsonNode> prodItr = producers.getElements();
			 while(prodItr.hasNext()){
				 JsonNode producer = prodItr.next();
				 String addr = producer.path("broadcast_address").getTextValue();
				 if (addr == null) { // We're keeping previous field compatibility, just in case
					addr = producer.path("address").getTextValue();
				 }
				 int tcpPort = producer.path("tcp_port").getIntValue();
				 int httpPort = producer.path("http_port").getIntValue();
				 tcpProducers.add(addr + ":" + tcpPort);
				 httpProducers.add("http://" + addr + ":" + httpPort);
			 }
			 allAvailableAddr.put("tcp", tcpProducers);
			 allAvailableAddr.put("http", httpProducers);
			 return allAvailableAddr;
		} catch (JsonParseException e) {
			log.error("Error parsing json from lookupd:", e);
		} catch (JsonMappingException e) {
			log.error("Error mapping json from lookupd:", e);
		} catch (IOException e) {
			log.error("Error reading response from lookupd:", e);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					log.error("lookup close reader error:", e);
				}
			}
		}
		return null;
	}
	
	private Reader httpReader(String uri) {
		try {
			URL url = new URL(uri);
			URLConnection conn = url.openConnection();
			conn.setConnectTimeout(DEFAULT_CONNECT_TIME_OUT);
			conn.setReadTimeout(DEFAULT_READ_TIME_OUT);
			InputStream is = url.openStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			return br;
		} catch (MalformedURLException e) {
			log.error("Malformed Lookupd URL: {}", uri);
		} catch (IOException e) {
			log.error("Problem reading lookupd response: ", e);
		}
		return null;
	}
}
