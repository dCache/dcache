package org.dcache.srm.qos.terapaths;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.qos.*;
import java.util.*;
import java.io.*;
import org.dcache.srm.util.Configuration;
import terapathsexamplejavaclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TerapathsPlugin implements QOSPlugin {
    private static final Logger logger =
            LoggerFactory.getLogger(TerapathsPlugin.class);


	AbstractStorageElement storage;
	private ArrayList tickets = new ArrayList();
	private static TpsAPISEI tpsAPISEIPort = null;
	Properties properties = new Properties();
	String propFile;
	Date lastRetrieval = null;
	String username;
	String password;
	String[] bandwidths;

	public TerapathsPlugin(){}

	public void setSrmConfiguration(Configuration configuration) {
		this.propFile = configuration.getQosConfigFile();
		this.storage = configuration.getStorage();		
	}

	public QOSTicket createTicket(
			String credential, 
			Long bytes,
			String srcURL, 
			int srcPortMin, 
			int srcPortMax,
			String srcProtocol,
			String dstURL, 
			int destPortMin,
			int dstPortMax,
			String dstProtocol) {
		return new TerapathsTicket(
				credential,
				bytes,
				srcURL,
				srcPortMin,
				srcPortMax,
				srcProtocol,
				dstURL,
				destPortMin,
				dstPortMax,
				dstProtocol);
	}

	public void addTicket(QOSTicket qosTicket) {
		assert(qosTicket instanceof TerapathsTicket);
		tickets.add(qosTicket);
	}

	public boolean submit() {
		boolean result = true;
		Bandwidths[] bws = null;
		ScheduleSlots[] ss = null;
		long startTime = new Date().getTime();

		logger.debug("Submitting qos request...");
		
		if (lastRetrieval==null || lastRetrieval.before(lastModification())) {
			try {
				properties.load(new FileInputStream(propFile));
			}
			catch(FileNotFoundException ex) {
				logger.error(ex.toString());
				return false;
			}
			catch(IOException ex) {
				logger.error(ex.toString());
				return false;
			}

			try {
				terapathsexamplejavaclient.TpsAPI tpsAPI = new terapathsexamplejavaclient.TpsAPI_Impl();
				tpsAPISEIPort = tpsAPI.getTpsAPISEIPort();
				((javax.xml.rpc.Stub) tpsAPISEIPort)._setProperty(javax.xml.rpc.Stub.ENDPOINT_ADDRESS_PROPERTY, properties.getProperty("serviceUrl", "http://198.124.220.9:8080/terapathsAPI/tpsAPI?wsdl"));
			} catch(javax.xml.rpc.ServiceException ex) {
				logger.error(ex.toString());
				return false;
			} catch(Exception ex) {
				logger.error(ex.toString());
				return false;
			}

			// SSL security stuff
			try {
				System.setProperty("javax.net.ssl.keyStoreType", "JKS");
				if (properties.getProperty("keyStore")!=null)
					System.setProperty("javax.net.ssl.keyStore", properties.getProperty("keyStore", "/usr/java/jdk1.5.0_14/jre/lib/security/keystore"));
				System.setProperty("javax.net.ssl.keyStorePassword", properties.getProperty("keyStorePassword","secret"));            
				System.setProperty("javax.net.ssl.trustStoreType", "JKS");
				if (properties.getProperty("trustStore")!=null)
					System.setProperty("javax.net.ssl.trustStore", properties.getProperty("trustStore","/usr/java/jdk1.5.0_14/jre/lib/security/cacerts2"));
				System.setProperty("javax.net.ssl.trustStorePassword", properties.getProperty("trustStorePassword","secret"));    

			} catch (Exception e) {
				logger.error(e.toString());
			} 	

			username = properties.getProperty("username", "terapaths");
			password = properties.getProperty("password", "terapaths");
			String bandwidthStr = properties.getProperty("bandwidthClasses");
			if (bandwidthStr != null)
				bandwidths = bandwidthStr.split(",");
			if (bandwidths==null || bandwidths.length==0) {
				bandwidths = new String[1];
				bandwidths[0] = "CS1_1";
			}
		}

		Iterator it = tickets.iterator();
		while (it.hasNext()) {
			TerapathsTicket tpTicket = (TerapathsTicket)it.next();
			// Convert names to ip address
			if (!(tpTicket.srcIP.toLowerCase().equals(tpTicket.srcIP.toUpperCase()))) {
				try {
					tpTicket.srcIP = java.net.InetAddress.getByName(tpTicket.srcIP).getHostAddress();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			if (!(tpTicket.dstIP.toLowerCase().equals(tpTicket.dstIP.toUpperCase()))) {
				try {
					tpTicket.dstIP = java.net.InetAddress.getByName(tpTicket.dstIP).getHostAddress();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}

			try {
				bws = tpsAPISEIPort.tpsAPI_getBandwidths(username, password, tpTicket.srcIP, tpTicket.dstIP);
				if (bws!=null) {
					boolean successFlag = false;
					for (int i = 0; i < bws.length; i++) {
						if (bws[i] == null) 
							continue; // bws[0] = local site, bws[1] = always null, bws[2] = remote site
						int j = 0;
						for (; j < bws[i].getBw().length; j++) {
							Bandwidth bw = bws[i].getBw()[j];

							// Make sure bandwidth class name is in bandwidths list from properties file
							int k=0;
							for (; k<bandwidths.length; k++) {
								if (bandwidths[k].equals(bw.getClassName()))
									break;
							}
							if (k==bandwidths.length)
								continue;

							// Get schedule for bandwidth and time range
							long endTime = startTime + (long)(Double.parseDouble(properties.getProperty("extraTimePerc", "1.1")) * (((double)tpTicket.bytes)/(bw.getBandwidth()*8)) * 1000);
							try {
								ss = tpsAPISEIPort.tpsAPI_getSchedule(username, password, "unidirectional", tpTicket.srcIP, tpTicket.dstIP, startTime, endTime, bw);
							} catch(java.rmi.RemoteException ex) {
								ex.printStackTrace();
							} catch(Exception ex) {
								ex.printStackTrace();
							}

							if (ss!=null && ss.length>0) {
								ReservationData rdSnd = new ReservationData();
								rdSnd.setBandwidth(bw);
								rdSnd.setDestIp(tpTicket.dstIP);
								rdSnd.setDestPortMax(tpTicket.dstPortMax);
								rdSnd.setDestPortMin(tpTicket.dstPortMin); 
								rdSnd.setDirection("unidirectional");
								rdSnd.setDuration( java.lang.Math.max((endTime - startTime)/1000, Long.parseLong(properties.getProperty("minReservationSec", "60"))) ); // in seconds
								rdSnd.setProtocol(tpTicket.srcProtocol);
								rdSnd.setSrcIp(tpTicket.srcIP);
								rdSnd.setSrcPortMax(tpTicket.srcPortMax);
								rdSnd.setSrcPortMin(tpTicket.srcPortMin);
								rdSnd.setStartTime(startTime);
								rdSnd.setUserName(username);
								rdSnd.setWho(new Who(password));
								
								// Convert names to ip address
								if (!(rdSnd.getDestIp().toLowerCase().equals(rdSnd.getDestIp().toUpperCase()))) {
									try {
										rdSnd.setDestIp( java.net.InetAddress.getByName(rdSnd.getDestIp()).getHostAddress() );
									} catch (Exception ex) {
										ex.printStackTrace();
									}
								}
								if (!(rdSnd.getSrcIp().toLowerCase().equals(rdSnd.getSrcIp().toUpperCase()))) {
									try {
										rdSnd.setSrcIp( java.net.InetAddress.getByName(rdSnd.getSrcIp()).getHostAddress() );
									} catch (Exception ex) {
										ex.printStackTrace();
									}
								}
								ReservationData rdRcv = tpsAPISEIPort.tpsAPI_reserve(rdSnd);
								if (rdRcv==null || (rdRcv.getStartTime()==rdSnd.getStartTime() && rdRcv.getDuration()==rdSnd.getDuration()))
									continue;

								tpTicket.id = rdRcv.getId();
								tpTicket.startTime = startTime;
								tpTicket.endTime = endTime;
								tpTicket.bandwidth = rdRcv.getBandwidth().getBandwidth();

								logger.debug("Submitted qos request "+tpTicket.id);
								successFlag = true;
								break;
							}
							if (successFlag)
								break;
						}
						if (j == bws[i].getBw().length)
							result = false;
					}
				}
				else
					result = false;
			}
			catch(java.rmi.RemoteException ex) {
				logger.error(ex.toString());
				return false;
			} catch(Exception ex) {
				logger.error(ex.toString());
				return false;
			}
		}

		return result;
	}

	public void sayStatus(QOSTicket qosTicket) {
		assert(qosTicket instanceof TerapathsTicket);
		//TerapathsTicket tpTicket = (TerapathsTicket)qosTicket;
		//if (tpTicket.id != -1) {
		//	Date now = new Date();
		//	long now = now.getTime();
		//	long timeLeft = tpTicket.endTime - now;
		//	logger.debug("End time="+(tpTicket.endTime/1000)+"s now="+(now/1000)+"s expires in "+(timeLeft/1000)+"s");
		//	long transferTimeLeft = 0;
		//	if (tpTicket.bytes!=-1 && tpTicket.bandwidth!=-1)
		//		transferTimeLeft = tpTicket.bytes/(tpTicket.bandwidth*8); // TODO: change bytes to remaining bytes
		//	if (timeLeft - transferTime < 0)
		//		logger.debug("AM: will extend end time by "+extendTime);
		//	else
		//		logger.debug("AM: no need to extend end time");
		//	logger.debug("Ticket "+tpTicket.id+" enabled");
		//}
	}

	private Date lastModification() {
		try {
			File file = new File(propFile);
			return new Date(file.lastModified());
		} catch (Exception e) {
			return null;
		}
	}

}
