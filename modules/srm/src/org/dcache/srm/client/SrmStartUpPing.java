//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 10/07 by Dmitry Litvintsev (litvinse@fnal.gov)
// 
// A simple utility class that pings SRM server so we can see SRM cell
// as "on-line" right away. It is supposed to be run from the same host the srm is run at
//______________________________________________________________________________

package org.dcache.srm.client;

import org.globus.util.GlobusURL;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.srm.Logger;
import org.dcache.srm.v2_2.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class SrmStartUpPing {

    //
    // logger is used by SRMClientV2. elog is implemented to print nothing on purpose
    //
    private static class SrmLogger implements Logger {
	private boolean debug;
	public SrmLogger(boolean debug) {
	    this.debug = debug;
	}
	public void elog(String s) {
	}
	public void elog(Throwable t) {
	}
	public void log(String s) {
	    if (debug) {
		System.out.println(s);
	    }
	}
    }

    public static void main(String agv[]) { 
 	org.ietf.jgss.GSSCredential user_cred = null;
 	org.dcache.srm.Logger logger = new SrmLogger(false);
	int port=8443;
	String x509cert = System.getenv("X509_CERT");
	if (x509cert==null) { 
	    x509cert="/etc/grid-security/hostcert.pem";
	    System.err.println("environment variable X509_CERT is not defined, using default \""+x509cert+"\"");
	}
	String x509key = System.getenv("X509_KEY");
	if (x509key==null) { 
	    x509key="/etc/grid-security/hostkey.pem";
	    System.err.println("environment variable X509_KEY is not defined, using default \""+x509key+"\"");
	}
	String sport=System.getenv("SRM_V2_PORT");
	if (sport==null) { 
	    System.err.println("environment variable SRM_V2_PORT is not defined, using default \""+port+"\"");
	}
	else { 
	    try { 
		port = Integer.parseInt(sport);
	    }
	    catch (NumberFormatException npe){ 
		npe.printStackTrace();
		System.exit(1);
	    }
	}
 	try {
 	    user_cred =  org.dcache.srm.security.SslGsiSocketFactory.createUserCredential(
		null,
		x509cert,x509key);
// 	    SRMClientV2 client = new SRMClientV2(new GlobusURL("srm://fapl110.fnal.gov:8443/srm/managerv2?SFN="),
	    SRMClientV2 client = new SRMClientV2(new GlobusURL("srm://localhost:"+port+"/srm/managerv2?SFN="),
 						 user_cred,
 						 10000,
 						 0,
 						 logger,
 						 false,
 						 false,
 						 null,
 						 null);
	    
 	    SrmPingRequest request = new SrmPingRequest();
 	    SrmPingResponse response = client.srmPing(request);
 	    if(response == null) {
		throw new IOException(" null response");
	    }
 	    StringBuffer sb = new StringBuffer();
 	    sb.append("VersionInfo : "+response.getVersionInfo()+"\n");
 	    if (response.getOtherInfo()!=null) { 
 		ArrayOfTExtraInfo info = response.getOtherInfo();
 		if (info.getExtraInfoArray()!=null) {
 		    for (int i=0;i<info.getExtraInfoArray().length;i++) { 
 			TExtraInfo extraInfo = info.getExtraInfoArray()[i];
 			sb.append(extraInfo.getKey() +":"+(extraInfo.getValue())+"\n");
 		    }
 		}
 	    }
 	    System.out.println(sb.toString());
 	}
 	catch (Exception e) { 
 	    //e.printStackTrace();
	    // logger.elog(e.getMessage());
 	}
	finally {
	    System.exit(0);
	}
	System.exit(0);
    }
}
	    