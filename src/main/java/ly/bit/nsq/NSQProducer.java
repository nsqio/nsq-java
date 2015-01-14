package ly.bit.nsq;

import ly.bit.nsq.exceptions.NSQException;
import ly.bit.nsq.lookupd.DefaultLookup;
import ly.bit.nsq.util.StringUtils;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.client.params.CookiePolicy;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;


public class NSQProducer {
	private static final Logger log = LoggerFactory.getLogger(NSQProducer.class);

	private static final String PUT_URL = "/put?topic=";
	private static final int DEFAULT_SOCKET_TIMEOUT = 2000;
	private static final int DEFAULT_CONNECTION_TIMEOUT = 2000;
	private static final int MAX_RETRY_COUNT = 3;

	private String defaultNsqdAddr;
	private DefaultLookup lookup;
	private ConcurrentHashMap<String, String> hostIndex;
	private ConcurrentHashMap<String, Integer> reTryCountMap;
	protected ExecutorService executor = Executors.newCachedThreadPool();

	protected HttpClient httpclient;
	protected PoolingClientConnectionManager cm;
	// TODO add timeout config / allow setting any httpclient param via getHtttpClient
	
	public NSQProducer(String defaultNsqdAddr, String lookupAddr) {
		this(lookupAddr);
		this.defaultNsqdAddr =  StringUtils.trimRight("/", defaultNsqdAddr);
	}

	public NSQProducer(String lookupAddr) {
		this.lookup = new DefaultLookup(lookupAddr);
		this.hostIndex = new ConcurrentHashMap<String, String>();
		this.reTryCountMap = new ConcurrentHashMap<String, Integer>();

		SchemeRegistry schemeRegistry = new SchemeRegistry();
		schemeRegistry.register(
				new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));

		cm = new PoolingClientConnectionManager(schemeRegistry);

		this.httpclient = new DefaultHttpClient(cm);
		this.setSocketTimeout(DEFAULT_SOCKET_TIMEOUT);
		this.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
		// see https://code.google.com/p/crawler4j/issues/detail?id=136: potentially works around a jvm crash at
		// org.apache.http.impl.cookie.BestMatchSpec.formatCookies(Ljava/util/List;)Ljava/util/List
		this.httpclient.getParams().setParameter(ClientPNames.COOKIE_POLICY, CookiePolicy.IGNORE_COOKIES);

		// register action for shutdown
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run(){
				shutdown();
			}
		});
	}

	/**
	 * Post a message onto NSQ (via the http interface)
	 * @param message
	 * @throws NSQException
	 */
	public void put(String message, String topic) throws NSQException {
		HttpPost post = null;
		try {
			String url = getUrl(topic);
			if (url == null) {
				throw new NSQException("can't get topic:("+topic+") http producer");
			}
			post = new HttpPost(url);
			post.setEntity(new StringEntity(message));
			HttpResponse response = this.httpclient.execute(post);
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new NSQException("POST to " + url + " returned HTTP " + response.getStatusLine().getStatusCode());
			}
			if (response.getEntity() != null) {
				EntityUtils.consume(response.getEntity());
			}
			reTryCountMap.put(topic, 0);
		} catch (UnsupportedEncodingException e) {
			throw new NSQException(e);
		} catch (ClientProtocolException e) {
			throw new NSQException(e);
		} catch (IOException e) {
			Integer reTryCount = reTryCountMap.get(topic);
			if (reTryCount != null && reTryCount.intValue() < MAX_RETRY_COUNT) {
				reTryCountMap.put(topic, MAX_RETRY_COUNT);
				put(message, topic);// retry
			} else {
				throw new NSQException(e);
			}
		} finally {
			if (post != null) {
				post.releaseConnection();
			}
		}
	}

	/**
	 * Post the message in a background executor thread, and log any error that occurs.
	 * If you want, you can call task.get() but then you may as well just call put().
	 * @param message
	 * @return
	 */
	public FutureTask<Void> putAsync(String message, String topic) {
		FutureTask<Void> task = new FutureTask<Void>(new NSQAsyncWriter(message, topic));
		executor.execute(task);
		return task;

	}

	public class NSQAsyncWriter implements Callable<Void> {
		private String message = null;
		private String topic = null;

		NSQAsyncWriter(String message, String topic) {
			this.message = message;
			this.topic = topic;
		}
		public Void call() throws NSQException {
			try {
				NSQProducer.this.put(message, topic);
			} catch (NSQException e) {
				// Log the error here since caller probably won't ever check the future.
				log.error("Error posting NSQ message:", e);
				throw e;
			} catch (Exception e) {
				// Log the error here since caller probably won't ever check the future.
				log.error("Error posting NSQ message:", e);
				throw new NSQException(e);
			}
			return null;
		}
	}

	public void shutdown() {
		if (this.executor != null) {
			this.executor.shutdown();
		}
	}

	public String getUrl(String topic) {
		String url = hostIndex.get(topic);
		if (url == null) {
			if (StringUtils.isBlank(lookup.getLookupAddr()) && !StringUtils.isBlank(defaultNsqdAddr)) {
				url = new StringBuffer(defaultNsqdAddr).append(PUT_URL).append(topic).toString();
				hostIndex.put(topic, url);
			} else {
				String httpAddr = lookup.getAvailableHttpAddr(topic);
				if (httpAddr == null) httpAddr = lookup.getAvailableHttpAddr();
				if (httpAddr == null) return null;
				url = new StringBuffer(httpAddr).append(PUT_URL).append(topic).toString();
				hostIndex.put(topic, url);
				reTryCountMap.put(topic, 0);
			}
		}
		
		return url;
	}

	/**
	 * This setter is probably only useful in a unit test / mocking context.
	 * @param client
	 */
	public void setHttpClient(HttpClient client) {
		this.httpclient = client;
	}

	public HttpClient getHttpclient() {
		return this.httpclient;
	}

	public void setSocketTimeout(int timeout) {
		this.httpclient.getParams().setIntParameter(CoreConnectionPNames.SO_TIMEOUT, timeout);
	}

	public void setConnectionTimeout(int timeout) {
		this.httpclient.getParams().setIntParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, timeout);
	}
	
	/**
	 * default 2
	 * @param max
	 */
	public void setDefaultMaxPerRoute(int max) {
		this.cm.setDefaultMaxPerRoute(max);
	}
	
	/**
	 * default 20
	 * @param max
	 */
	public void setMaxTotal(int max) {
		this.cm.setMaxTotal(max);
	}

}
