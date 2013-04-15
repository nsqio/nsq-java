package ly.bit.nsq.lookupd;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

public class BasicLookupd extends AbstractLookupd {
	private static final Logger log = LoggerFactory.getLogger(AbstractLookupd.class);

	@Override
	public List<String> query(String topic) {
		String urlString = this.addr + "/lookup?topic=" + topic;
		URL url = null;
		try {
			url = new URL(urlString);
		} catch (MalformedURLException e) {
			log.error("Malformed Lookupd URL: {}", urlString);
		}
		try {
			InputStream is = url.openStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			return parseResponseForProducers(br);
		} catch (IOException e) {
			log.error("Problem reading lookupd response: ", e);
		}
		return new LinkedList<String>();
	}

	public BasicLookupd(String addr){
		this.addr = addr;
	}

}
