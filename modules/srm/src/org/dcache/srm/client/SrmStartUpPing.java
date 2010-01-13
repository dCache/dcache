//______________________________________________________________________________
//
// created 10/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
// A simple utility class that pings SRM server so we can see SRM cell
// as "on-line" right away. It is supposed to be run from the same host the srm is run at
//______________________________________________________________________________

package org.dcache.srm.client;

import org.globus.gsi.GlobusCredentialException;
import org.globus.util.GlobusURL;
import org.ietf.jgss.GSSException;
import org.dcache.srm.Logger;
import org.dcache.srm.v2_2.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.RemoteException;

import javax.xml.rpc.ServiceException;

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

    private static org.dcache.srm.Logger _logger = new SrmLogger(false);
    private static org.ietf.jgss.GSSCredential _user_cred = null;
    private static SRMClientV2 _client;
    private static GlobusURL _url;

    public static void main(String agv[]) {
        String x509cert = getEnvironmentVariableWithDefault( "X509_CERT", "/etc/grid-security/hostcert.pem");
        String x509key = getEnvironmentVariableWithDefault( "X509_KEY", "/etc/grid-security/hostkey.pem");

        tryToBuildUrl();
        tryToObtainUserCredential( x509cert, x509key);
        tryToOtainSrmClient();
        SrmPingResponse response = tryToPing();
        buildOutput( response);
    }

    private static String getEnvironmentVariableWithDefault( String variableName, String defaultValue) {
        String value = System.getenv( variableName);
        if( value==null) {
            value = defaultValue;
            System.err.println("environment variable "+variableName+" is not defined, using default \""+defaultValue+"\"");
        }
        return value;
    }

    private static void tryToBuildUrl() {
        String portStr = getEnvironmentVariableWithDefault( "SRM_V2_PORT", "8443");

        try {
            Integer.parseInt( portStr);
        } catch (NumberFormatException npe) {
            System.err.println("Port number must be an integer: " + portStr);
            System.exit(1);
        }

        String url = "srm://localhost:"+portStr+"/srm/managerv2?SFN=";

        try {
            _url = new GlobusURL( url);
        } catch (MalformedURLException e) {
            System.err.println( "URL is malformed: " + url);
            System.exit( 1);
        }
    }

    private static void tryToObtainUserCredential( String x509Cert, String x509Key) {
        try {
            _user_cred =  org.dcache.srm.security.SslGsiSocketFactory.createUserCredential(
                    null,
                    x509Cert,x509Key);
        } catch (GlobusCredentialException gce) {
            System.err.println( "Unable to create user credential: " + gce.getMessage());
            System.exit( 1);
        } catch (GSSException ge) {
            System.err.println( "Unable to create user credential: " + ge.getMessage());
            System.exit( 1);
        }
    }

    private static void tryToOtainSrmClient() {
        try {
            _client = new SRMClientV2(_url,
                    _user_cred,
                    10000,
                    0,
                    _logger,
                    false,
                    false,
                    null,
                    null);
        } catch (IOException e) {
            System.err.println( "Unable to instantiate SRMClientV2: " + e.getMessage());
        } catch (InterruptedException e) {
            System.err.println( "Interrupted while creating SRMClientV2");
        } catch (ServiceException e) {
            System.err.println( "ServiceException while creating SRMClientV2: " + e.getMessage());
        }

        if( _client == null)
            System.exit( 1);
    }


    private static SrmPingResponse tryToPing() {
        SrmPingRequest request = new SrmPingRequest();
        SrmPingResponse response = null;
        try {
            response = _client.srmPing(request);
        } catch (RemoteException e) {
            System.err.println( "Received remote exception from server:");
            e.getCause().printStackTrace();
            System.exit( 1);
        }

        if(response == null) {
            System.err.println( "Obtained a null response from srmPing");
            System.exit( 1);
        }

        return response;
    }


    private static void buildOutput( SrmPingResponse response) {
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
}
	    
