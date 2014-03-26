package org.dcache.srm.qos.lambdastation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Properties;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.qos.QOSPlugin;
import org.dcache.srm.qos.QOSTicket;
import org.dcache.srm.util.Configuration;

public class LambdaStationPlugin implements QOSPlugin {
    private static final Logger logger =
            LoggerFactory.getLogger(LambdaStationPlugin.class);
	private LambdaStationMap lambdaStationMap;
	private String lambdaStationConf;
	private String lambdaStationScript;
	private AbstractStorageElement storage;
	private Collection<LambdaStationTicket> tickets = new ArrayList();

	public LambdaStationPlugin(){}

	@Override
    public void setSrm(SRM srm) {
        lambdaStationConf = srm.getConfiguration().getQosConfigFile();
        storage = srm.getStorage();
		Properties properties = new Properties();

        FileInputStream fis;
		try {
            fis = new FileInputStream(lambdaStationConf);
        }
        catch(FileNotFoundException ex) {
                logger.error(ex.toString());
                return;
        }
        try {
            properties.load(fis);
        } catch(IOException ex) {
                logger.error("failed to load configuration ",ex);
                return;
        } finally {
            try {
                fis.close();
            } catch (IOException ex) {
                logger.error("failed to close "+lambdaStationConf, ex);
            }
        }
        this.lambdaStationScript = properties.getProperty("l_station_script","/opt/d-cache/conf/l_station_script.sh");
		this.lambdaStationMap = new LambdaStationMap(properties.getProperty("l_station_map","/opt/d-cache/conf/l_station_map.xml"));
	}

	@Override
        public QOSTicket createTicket(
			String credential,
			Long bytes,
			String srcURL,
			int srcPortMin,
			int srcPortMax,
			String srcProtocol,
			String dstURL,
			int dstPortMin,
			int dstPortMax,
			String dstProtocol) {
		LambdaStationTicket ticket = new LambdaStationTicket(
				credential,
				dstURL,
				null, // don't make it hardcoded
				srcURL,
				null, // don't make it hardcoded
				600, // hardcoded travel time so far
				lambdaStationMap);
		ticket.bytes = bytes;
		return ticket;
	}

	@Override
        public void addTicket(QOSTicket qosTicket) {
           if (qosTicket instanceof LambdaStationTicket) {
                LambdaStationTicket ls_ticket =
                    (LambdaStationTicket)qosTicket;
		tickets.add(ls_ticket);
           }
	}

	@Override
        public boolean submit() {
		boolean result = true;
		for (LambdaStationTicket ls_ticket:tickets) {
                    ls_ticket.OpenTicket(lambdaStationScript);
                    result = !result ? isTicketEnabled(ls_ticket) : result;
		}
		return result;
	}

	private boolean isTicketEnabled(QOSTicket qosTicket) {
		if (qosTicket instanceof LambdaStationTicket) {
			LambdaStationTicket	ls_ticket = (LambdaStationTicket)qosTicket;
			boolean sEnabled = ls_ticket.srcEnabled();
			boolean dEnabled = ls_ticket.dstEnabled();
			logger.debug("src enabled="+sEnabled+" dst enabled="+dEnabled);
			return sEnabled && dEnabled && ls_ticket.getLocalTicketID() != 0;
		}
		else {
                    return false;
                }
	}

	@Override
        public void sayStatus(QOSTicket qosTicket) {
		if (qosTicket instanceof LambdaStationTicket) {
			LambdaStationTicket	ls_ticket = (LambdaStationTicket)qosTicket;
			if (isTicketEnabled(qosTicket)) {
				Date now = new Date();
				long t = now.getTime();
				long time_left = ls_ticket.getActualEndTime() - t/1000;
				logger.debug("End time="+ls_ticket.getActualEndTime()+" Travel Time="+ls_ticket.TravelTime+" now="+t+" expires in "+time_left);
				// try to calculate transfer time assuming 1Gb local connection
				long transfer_time = 0l;
				long rate_MB = 100000000l; // 1Gb means 100MB
				if (ls_ticket.bytes != null) {
					transfer_time = ls_ticket.bytes /rate_MB;
				}
				long extend_time = Math.max(transfer_time, 600l);
				if (time_left - extend_time < 0) {
					logger.debug("AM: will extend end time by "+extend_time);
				}
				else {
					logger.debug("AM: no need to extend end time");
				}
			}
		}
	}
}
