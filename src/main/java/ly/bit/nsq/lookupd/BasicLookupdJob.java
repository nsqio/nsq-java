package ly.bit.nsq.lookupd;

import java.util.List;
import java.util.Map;

import ly.bit.nsq.NSQReader;
import ly.bit.nsq.exceptions.NSQException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BasicLookupdJob implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(BasicLookupdJob.class);
    final private String lookupdAddress;
    final private NSQReader reader;

    public BasicLookupdJob(String lookupdAddress, NSQReader reader) {
        this.lookupdAddress = lookupdAddress;
        this.reader = reader;
    }

	@Override
    public void run() {
        Map<String, AbstractLookupd> lookupdConnections = reader.getLookupdConnections();
        AbstractLookupd lookupd = lookupdConnections.get(lookupdAddress);
        List<String> producers = lookupd.query(reader.getTopic());
        for(String producer : producers) {
            String[] components = producer.split(":");
            String nsqdAddress = components[0];
            int nsqdPort = Integer.parseInt(components[1]);
            try {
                reader.connectToNsqd(nsqdAddress, nsqdPort);
            } catch (NSQException e) {
                log.error("Error reading response from lookupd", e);
            }
        }
    }
}
