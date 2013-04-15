package ly.bit.nsq;

import ly.bit.nsq.exceptions.NSQException;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;


public class NSQProducer {
	private static final Logger log = LoggerFactory.getLogger(NSQProducer.class);

	private static final String PUT_URL = "/put?topic=";
	private static final int DEFAULT_SOCKET_TIMEOUT = 2000;
	private static final int DEFAULT_CONNECTION_TIMEOUT = 2000;

	private String url;
	private String topic;
	protected ExecutorService executor = Executors.newCachedThreadPool();


	protected HttpClient httpclient;
	// TODO add timeout config / allow setting any httpclient param via getHtttpClient

	// Convenience  constructor assuming local nsqd on standard port
	public NSQProducer(String topic) {
		this("http://127.0.0.1:4151", topic);
	}

	public NSQProducer(String url, String topic) {
		this.topic = topic;
		this.url = url + PUT_URL + topic;

		SchemeRegistry schemeRegistry = new SchemeRegistry();
		schemeRegistry.register(
				new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));

		ClientConnectionManager cm = new PoolingClientConnectionManager(schemeRegistry);

		this.httpclient = new DefaultHttpClient(cm);
		this.setSocketTimeout(DEFAULT_SOCKET_TIMEOUT);
		this.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);

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
	public void put(String message) throws NSQException {
		HttpPost post = null;
		try {
			post = new HttpPost(url);
			post.setEntity(new StringEntity(message));
			HttpResponse response = this.httpclient.execute(post);
			if (response.getStatusLine().getStatusCode() != 200) {
				throw new NSQException("POST to " + url + " returned HTTP " + response.getStatusLine().getStatusCode());
			}
			if (response != null && response.getEntity() != null) {
				EntityUtils.consume(response.getEntity());
			}
		} catch (UnsupportedEncodingException e) {
			throw new NSQException(e);
		} catch (ClientProtocolException e) {
			throw new NSQException(e);
		} catch (IOException e) {
			throw new NSQException(e);
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
	public FutureTask<Void> putAsync(String message) {
		FutureTask task = new FutureTask<Void>(new NSQAsyncWriter(message));
		executor.execute(task);
		return task;

	}

	public class NSQAsyncWriter implements Callable<Void> {
		private String message = null;

		NSQAsyncWriter(String message) {
			this.message = message;
		}
		public Void call() throws NSQException {
			try {
				NSQProducer.this.put(message);
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

	public String toString(){
		return "Writer<" + this.url + ">";
	}
	public String getUrl() {
		return url;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
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

}
