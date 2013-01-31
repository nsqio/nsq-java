package ly.bit.nsq.lookupd;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import ly.bit.nsq.NSQReader;
import ly.bit.nsq.exceptions.NSQException;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class BasicLookupdJob implements Job {

	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		String addr = context.getMergedJobDataMap().getString("lookupdAddress");
		NSQReader reader = NSQReader.readerIndex.get(context.getMergedJobDataMap().getString("reader"));
		Map<String, AbstractLookupd> lookupdConnections = reader.getLookupdConnections();
		AbstractLookupd lookupd = lookupdConnections.get(addr);
		List<String> producers = lookupd.query(reader.getTopic());
		for(String producer : producers) {
			String[] components = producer.split(":");
			String nsqdAddress = components[0];
			int nsqdPort = Integer.parseInt(components[1]);
			try {
				reader.connectToNsqd(nsqdAddress, nsqdPort);
			} catch (NSQException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
}
