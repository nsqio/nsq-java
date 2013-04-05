package ly.bit.nsq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;


import ly.bit.nsq.exceptions.NSQException;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicStatusLine;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.FutureTask;

/**
 * User: oneill
 * Date: 4/4/13
 */
public class NSQProducerTest {
	private Logger log = LoggerFactory.getLogger(NSQProducerTest.class);
	NSQProducer producer;
	String topic = "andy_wuz_ere";
	HttpClient mockClient;

	@Before
	public void setUp() {
		mockClient = mock(HttpClient.class);
		producer = new NSQProducer(topic);
		producer.httpclient = mockClient;
		assertEquals("http://127.0.0.1:4151/put?topic=" + topic, producer.getUrl());
	}

	@After
	public void tearDown() {
		producer.shutdown();
	}

	@Test
	public void testPut_success() throws Exception {
		log.debug("Attempting successful put");
		HttpResponse successResponse = mock(HttpResponse.class);
		StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), HttpStatus.SC_OK, "OK");
		when(successResponse.getStatusLine()).thenReturn(statusLine);
		when(mockClient.execute(any(HttpPost.class))).thenReturn(successResponse);
		producer.put("{foo:\"bar\"}");
	}

	@Test(expected = NSQException.class)
	public void testPut_error() throws Exception {
		log.debug("Attempting error put");
		HttpResponse errorResponse = mock(HttpResponse.class);
		StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), HttpStatus.SC_BAD_GATEWAY,
				"OH NOES, NSQD CRASHED");
		when(errorResponse.getStatusLine()).thenReturn(statusLine);
		when(mockClient.execute(any(HttpPost.class))).thenReturn(errorResponse);
		producer.put("{foo:\"bar\"}");
	}

	@Test
	public void testPutAsync_success() throws Exception {
		log.debug("Attempting successful async put");
		HttpResponse successResponse = mock(HttpResponse.class);
		StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), HttpStatus.SC_OK, "OK");
		when(successResponse.getStatusLine()).thenReturn(statusLine);
		when(mockClient.execute(any(HttpPost.class))).thenReturn(successResponse);
		FutureTask<Void> future = producer.putAsync("{foo:\"bar\"}");
		future.get();
	}

	@Test(expected = java.util.concurrent.ExecutionException.class)
	public void testPutAsync_error() throws Exception {
		log.debug("Attempting error async put, you will see an exception now:");
		HttpResponse errorResponse = mock(HttpResponse.class);
		StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), HttpStatus.SC_BAD_GATEWAY,
				"OH NOES, NSQD CRASHED");
		when(errorResponse.getStatusLine()).thenReturn(statusLine);
		when(mockClient.execute(any(HttpPost.class))).thenReturn(errorResponse);
		FutureTask<Void> future = producer.putAsync("{foo:\"bar\"}");
		future.get();
	}

	@Test
	public void testShutdown() throws Exception {
		log.debug("Attempting async put with shutdown");
		HttpResponse successResponse = mock(HttpResponse.class);
		StatusLine statusLine = new BasicStatusLine(new ProtocolVersion("HTTP", 1, 1), HttpStatus.SC_OK, "OK");
		when(successResponse.getStatusLine()).thenReturn(statusLine);
		when(mockClient.execute(any(HttpPost.class))).thenReturn(successResponse);
		FutureTask<Void> future = producer.putAsync("{foo:\"bar\"}");
		producer.shutdown();
		assertTrue(producer.executor.isShutdown());

		future.get();

	}
}
