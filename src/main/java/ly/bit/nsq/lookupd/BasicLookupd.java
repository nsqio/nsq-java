package ly.bit.nsq.lookupd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;

public class BasicLookupd extends AbstractLookupd {

	@Override
	public List<String> query(String topic) {
		URL url = null;
		try {
			url = new URL(this.addr + "/lookup?topic=" + topic);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
		try {
			InputStream is = url.openStream();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			return parseResponseForProducers(br);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new LinkedList<String>();
	}

	public BasicLookupd(String addr){
		this.addr = addr;
	}

}
