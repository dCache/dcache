//______________________________________________________________________________
//
// $Id: SrmCheckPermission.java,v 1.5 2006/12/21 17:39:41 litvinse Exp $
// $Author: litvinse $
//
// created 10/07 by Dmitry Litvintsev (litvinse@fnal.gov)
// 
// A simple utility class that pings SRM server so we can see SRM cell
// as "on-line" right away
//______________________________________________________________________________

package org.dcache.srm.client;

import org.globus.util.GlobusURL;
import org.dcache.srm.client.SRMClientV2;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.srm.Logger;
import org.dcache.srm.v2_2.*;
import java.io.IOException;

import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.TrustedCertificates;

import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;

import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.GlobusCredentialException;

import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSCredential;

import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;


import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;


import org.dcache.srm.security.SslGsiSocketFactory;
import org.dcache.srm.SRMUser;


import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class SrmStartUpPing {

    private static class SrmLogger implements Logger {
	private boolean debug;
	public SrmLogger(boolean debug) {
	    this.debug = debug;
	}
	public void elog(String s) {
	    System.err.println(s);
	}
	public void elog(Throwable t) {
	    t.printStackTrace(System.err);
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
 	try {
 	    user_cred =  org.dcache.srm.security.SslGsiSocketFactory.createUserCredential(
		null,
		"/etc/grid-security/hostcert.pem",
		"/etc/grid-security/hostkey.pem");
 	    SRMClientV2 client = new SRMClientV2(new GlobusURL("srm://fapl110.fnal.gov:8443/srm/managerv2?SFN="),
 						 user_cred,
 						 10000,
 						 1,
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
 	    e.printStackTrace();
 	}
    }
}
	    