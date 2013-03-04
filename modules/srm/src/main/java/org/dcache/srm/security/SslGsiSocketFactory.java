/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

package org.dcache.srm.security;

import org.globus.gsi.CredentialException;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/**
 *
 * @author  timur
 */
public class SslGsiSocketFactory {

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
        return context;
    }


    public static GSSCredential createUserCredential(
    String x509UserProxy)  throws CredentialException, GSSException {
        if(x509UserProxy != null) {
            X509Credential gcred = new X509Credential(x509UserProxy);
            GSSCredential cred =
            new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
            return cred;
        }
        X509Credential gcred = X509Credential.getDefaultCredential();
        GSSCredential cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        return  cred;

    }

    public static GSSCredential createUserCredential(String x509ServiceCert, String x509ServiceKey)
    throws CredentialException, GSSException {
        if(x509ServiceCert != null && x509ServiceKey != null) {
            try {
                X509Credential gcred =new X509Credential(
                                                         x509ServiceCert,
                                                         x509ServiceKey
                                                         );
                GSSCredential cred =
                    new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
                return cred;
            }
            catch(IOException ioe) {
                throw new GSSException(GSSException.NO_CRED, 0,
                                       "could not create globus credentials "+ioe.toString());
            }
        }

        X509Credential gcred = X509Credential.getDefaultCredential();
        GSSCredential cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        return  cred;
    }

    public static GSSCredential createUserCredential(String x509UserProxy, String x509ServiceCert, String x509ServiceKey)
    throws CredentialException, GSSException {

        if(x509UserProxy != null) {
            return createUserCredential(x509UserProxy);
        }
        else if(x509ServiceCert != null && x509ServiceKey != null) {
            return createUserCredential(x509ServiceCert,x509ServiceKey);
        }

        X509Credential gcred = X509Credential.getDefaultCredential();
        GSSCredential cred = new GlobusGSSCredentialImpl(gcred, GSSCredential.INITIATE_ONLY);
        return  cred;
    }




    public static GSIGssSocket delegateCredential(InetAddress inetAddress,
    int port,GSSCredential credential,boolean fulldelegation)
    throws Exception {
        // say("createSocket("+inetAddress+","+port+")");
        Socket s =null;
      GSIGssSocket gsiSocket;
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
            gsiSocket = new GSIGssSocket(s, context);
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

      return gsiSocket;
    }
}





