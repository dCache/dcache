/*
 * DelegationTestMiddleServer.java
 *
 * Created on December 17, 2004, 3:53 PM
 */

package org.dcache.srm.security;

import org.globus.gsi.CredentialException;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.AuthorizationException;
import org.globus.gsi.gssapi.auth.GSSAuthorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.gridforum.jgss.ExtendedGSSContext;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

/**
 *
 * @author  timur
 */
public class DelegationTestMiddleServer {

    private String x509ServiceCert;
    private String x509ServiceKey;
    private String x509TrastedCACerts;

    /** Creates a new instance of DelegationTestMiddleServer */
    public DelegationTestMiddleServer(String x509ServiceCert,
    String x509ServiceKey,
    String x509TrastedCACerts,
    int port) throws IOException {
            this.x509ServiceCert =  x509ServiceCert;
        this.x509ServiceKey = x509ServiceKey;
        this.x509TrastedCACerts = x509TrastedCACerts;
        ServerSocket ss = new ServerSocket(port);
        while (true)
        {
           final Socket s = ss.accept();

            new Thread( new Runnable()
            {
                @Override
                public void run(){
                   handle(s);
                }
            }
            ).start();
        }
    }

    public static void  delegateCredential(InetAddress inetAddress,
    int port,GSSCredential credential,boolean fulldelegation)
    throws Exception {
        // say("createSocket("+inetAddress+","+port+")");
        Socket s =null;
        try {
            //   say("delegateCredentials() user credential is "+credential);
            GSSManager manager = ExtendedGSSManager.getInstance();
            GSSAuthorization gssAuth =
            HostAuthorization.getInstance();
            GSSName targetName = gssAuth.getExpectedName(null, inetAddress.getCanonicalHostName());
            ExtendedGSSContext context =
            (ExtendedGSSContext) manager.createContext(targetName,
                GSSConstants.MECH_OID,
                credential,
                GSSContext.DEFAULT_LIFETIME);
            context.setOption(GSSConstants.GSS_MODE,
            GSIConstants.MODE_GSI);
            context.requestCredDeleg(true);
            if(fulldelegation) {
                context.setOption(GSSConstants.DELEGATION_TYPE, GSIConstants.DELEGATION_TYPE_FULL);
            }
            else {
                context.setOption(GSSConstants.DELEGATION_TYPE, GSIConstants.DELEGATION_TYPE_LIMITED);
            }
            //context.setOption(
            s = new Socket(inetAddress,port);
            GSIGssSocket gsiSocket = new GSIGssSocket(s, context);
            gsiSocket.setUseClientMode(true);
            gsiSocket.setAuthorization(
            //org.globus.ogsa.impl.security.authorization.HostAuthorization.
            gssAuth
            );
            gsiSocket.setWrapMode(GssSocket.SSL_MODE);
            gsiSocket.startHandshake();
        }
        catch(Exception e) {
            if(s!=null) {
                try {
                    s.close();
                }
                catch(Exception e1) {
                }
            }
            throw e;
        }
    }


    public void handle(Socket s) {
        try {
            GSSContext context = getServiceContext();
            GSIGssSocket gsis = new GSIGssSocket(s, context);
            gsis.setAuthorization(
                        new Authorization() {
                            @Override
                            public void authorize(GSSContext context, String host)
                            throws AuthorizationException {
                                say("authorized");
                            }
            }
            );
            gsis.setUseClientMode(false);
            gsis.setWrapMode(GssSocket.SSL_MODE);
            DataInputStream inStream =
                    new DataInputStream(gsis.getInputStream());
            String str = inStream.readUTF();
            inStream.close();
            say("read utf: "+str);
            InetAddress  finaldesthost =
                InetAddress.getByName(str.substring(0, str.indexOf(' ')));
            int finaldestport =
                Integer.parseInt(str.substring( str.indexOf(' ')+1));
            GSSCredential cred = gsis.getContext().getDelegCred();
            if(cred != null) {
                say("received deleg cred "+cred.getName());
                say("delegatig to host ="+finaldesthost+" port="+finaldestport);
                delegateCredential(finaldesthost, finaldestport, cred, false);           }


        } catch (Exception e)        {
            esay(e);
        } finally {
            try {
                s.close();
            }catch (Exception e1) {}

        }

    }
    public void say(String s){
        System.out.println(s);

    }

    public void esay(String s){
        System.err.println(s);

    }

    public void esay(Throwable t){
        t.printStackTrace();

    }

    private static X509Credential service_cred;
    private static TrustedCertificates trusted_certs;

    public static GSSCredential getServiceCredential(
    String x509ServiceCert,
    String x509ServiceKey,int usage) throws GSSException {

        try {
            if(service_cred != null) {
                service_cred.verify();
            }
        }
        catch(CredentialException gce) {
            service_cred = null;

        }


        if(service_cred == null) {
            try {
                service_cred =new X509Credential(
                x509ServiceCert,
                x509ServiceKey
                );
            }
            catch(CredentialException gce) {
                throw new GSSException(GSSException.NO_CRED ,
                0,
                "could not load host globus credentials "+gce.toString());
            } catch(IOException ioe) {
                throw new GSSException(GSSException.NO_CRED, 0,
                                       "could not load host globus credentials "+ioe.toString());
            }

        }

        GSSCredential cred = new GlobusGSSCredentialImpl(service_cred, usage);

        return cred;
    }

    public static GSSContext getServiceContext(
    String x509ServiceCert,
    String x509ServiceKey,
    String x509TrastedCACerts) throws GSSException {
        GSSCredential cred = getServiceCredential(x509ServiceCert, x509ServiceKey,
        GSSCredential.ACCEPT_ONLY);

        if(trusted_certs == null) {
            trusted_certs =
            TrustedCertificates.load(x509TrastedCACerts);
        }

        GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSContext context =
        (ExtendedGSSContext) manager.createContext(cred);

        context.setOption(GSSConstants.GSS_MODE,
        GSIConstants.MODE_GSI);
        context.setOption(GSSConstants.TRUSTED_CERTIFICATES,
        trusted_certs);
        return context;
    }

    private GSSContext getServiceContext() throws GSSException {
        try {
            return getServiceContext(x509ServiceCert, x509ServiceKey,  x509TrastedCACerts);
        }
        catch(GSSException gsse) {
            esay(gsse);
            throw gsse;
        }
    }
   public static final void main(String args[]) throws IOException{
        String x509ServiceCert = args[0];
        String x509ServiceKey = args[1];
        String x509TrastedCACerts = args[2];
        int port = Integer.parseInt(args[3]);
        new DelegationTestMiddleServer(x509ServiceCert,
        x509ServiceKey,
        x509TrastedCACerts,
        port);
   }

}
