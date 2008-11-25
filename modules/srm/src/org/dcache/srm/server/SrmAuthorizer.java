/**
 * SrmAuthorizer.java
 *
 * Authors:  LH - Leo Heska
 *
 * History:
 *    2005/07/22 LH Extracted from SrmSoapBindingImpl.java
 */

/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract
No. DE-AC02-76CH03000. Therefore, the U.S. Government retains a
world-wide non-exclusive, royalty-free license to publish or reproduce
these documents and software for U.S. Government purposes.  All
documents and software available from this server are protected under
the U.S. and Foreign Copyright Laws, and FNAL reserves all rights.
 
 
 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.
 
 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).
 
 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.
 
 
Neither the name of Fermilab, the  URA, nor the names of the
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.
 
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
cost, or damages arising out of or resulting from the use of the
software available from this server.
 
 
Export Control:
 
All documents and software available from this server are subject to
U.S. export control laws.  Anyone downloading information from this
server is obligated to secure any necessary Government licenses before
exporting documents or software obtained from this server.
 */

package org.dcache.srm.server;

import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.globus.axis.gsi.GSIConstants;
import org.dcache.srm.request.RequestCredential;
import org.ietf.jgss.GSSContext;

import java.net.Inet4Address;

import java.util.*;
import java.io.IOException;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.globus.gsi.gssapi.net.impl.GSIGssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.auth.NoAuthorization;
import org.globus.gsi.GlobusCredential;
import org.globus.gsi.GlobusCredentialException;
import org.globus.gsi.TrustedCertificates;
import org.globus.gsi.bc.BouncyCastleUtil;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;
import org.glite.security.voms.VOMSValidator;
import org.glite.security.voms.VOMSAttribute;
import org.glite.security.voms.BasicVOMSTrustStore;
import org.glite.security.util.DirectoryList;
import java.security.cert.X509Certificate;
import org.dcache.srm.SRMAuthorizationException;

// The following imports are needed to extract the user's credential
// from the servlet context
// from the servlet context


public class SrmAuthorizer {
   String storageName;
   String pathToConfigurationXml;
   Object syncObject = new Object();
   public static final String REMOTE_ADDR = "REMOTE_ADDR";
   private org.dcache.srm.SRMAuthorization authorization;
   private org.dcache.srm.request.RequestCredentialStorage credential_storage;
   private org.dcache.srm.request.sql.RequestsPropertyStorage propertyStorage;
   public org.apache.log4j.Logger log;
   private static boolean initialized = false;
   private String logConfigFile;
   private static String vomsdir = "/etc/grid-security/vomsdir";
   private static String service_trusted_certs = "/etc/grid-security/certificates";
    
   static {
        try {
            new DirectoryList(vomsdir).getListing();
        } catch (IOException e) {
            vomsdir = service_trusted_certs;
        }
        VOMSValidator.setTrustStore(new BasicVOMSTrustStore(vomsdir, 12*3600*1000));
    }
   
   public SrmAuthorizer(SrmDCacheConnector srmConn) {
      initialize(srmConn);
   }
   
   
   private synchronized void initialize(SrmDCacheConnector srmConn) {
      try {
         log = org.apache.log4j.Logger.getLogger(
             "logger.org.dcache.authorization."+
             this.getClass().getName());
         logConfigFile = srmConn.getLogFile();
         org.apache.log4j.xml.DOMConfigurator.configure(logConfigFile);
         // Below re-checks config file periodically; default 60 seconds
         // DOMConfigurator.configureAndWatch(logConfigFile);
         
         authorization =
            srmConn.configuration.getAuthorization();
         credential_storage =
            srmConn.getSrm().getRequestCredentialStorage();
         propertyStorage = RequestsPropertyStorage.getPropertyStorage(
            srmConn.configuration.getJdbcUrl(),
            srmConn.configuration.getJdbcClass(),
            srmConn.configuration.getJdbcUser(),
            srmConn.configuration.getJdbcPass(),
            srmConn.configuration.getNextRequestIdStorageTable(),
            srmConn.configuration.getStorage()
            );
            
            initialized = true;
            log.debug("Successfully initialized");
      } catch (Exception e) {
         e.printStackTrace();
         log.fatal("Failed to initialize: exception is " + e);
         throw new RuntimeException(e);
      }
   }
   
      
   public UserCredential getUserCredentials() throws SRMAuthorizationException {
      try {
         org.apache.axis.MessageContext mctx = 
         org.apache.axis.MessageContext.getCurrentContext();
         setUpEnv(mctx);
         
         org.ietf.jgss.GSSContext gsscontext  =
            (org.ietf.jgss.GSSContext)mctx.getProperty(GSIConstants.GSI_CONTEXT);
         if(gsscontext == null) {
             throw new SRMAuthorizationException(
             "cant extract gsscontext from MessageContext, gsscontext is null");
         }
         String secureId = gsscontext.getSrcName().toString();
         log.debug("User ID (secureId) is: " + secureId);
         org.ietf.jgss.GSSCredential delegcred = gsscontext.getDelegCred();
         if(delegcred != null) {
             try {
                log.debug("User credential (delegcred) is: " +
                   delegcred.getName());
             } catch (Exception e) {
                log.debug("Caught occasional (usually harmless) exception" +
                   " when calling " + "delegcred.getName()): ", e);
             }
          }
         
         UserCredential userCredential = new UserCredential();
         userCredential.secureId = secureId;
         userCredential.context = gsscontext;
         userCredential.credential = delegcred;
         String remote_addr = (String) mctx.getProperty(REMOTE_ADDR);
         userCredential.clientHost =
             Inet4Address.getByName(remote_addr).getCanonicalHostName();

         return userCredential;
      }catch (SRMAuthorizationException srme){
          throw srme;
      } catch (Exception e) {
          log.error("getUserCredentials failed with exception",e);
         throw new SRMAuthorizationException(e.toString());
      }
   }
   
   
   public org.dcache.srm.SRMUser getRequestUser(
       RequestCredential requestCredential,
       String role,
       GSSContext context) throws SRMAuthorizationException {

      org.dcache.srm.SRMUser requestUser =
         authorization.authorize(requestCredential.getId(),
         requestCredential.getCredentialName(),
         role,
         context);
      
      return requestUser;
   }

   public org.dcache.srm.request.RequestCredential
       getRequestCredential(UserCredential userCredential, String role)  {
      try {
         log.debug(
            "About to call RequestCredential.getRequestCredential(" +
            userCredential.secureId + "," + role + ")");
         org.dcache.srm.request.RequestCredential rc =
            org.dcache.srm.request.RequestCredential.getRequestCredential(
            userCredential.secureId,role);
         log.debug("Received RequestCredential: " + rc);
         // log.debug("rc.getRole(): " + rc.getRole());
         if(rc != null) {
            rc.keepBestDelegatedCredential(userCredential.credential);
         } else {
            log.debug("About to create new RequestCredential");
            rc = new org.dcache.srm.request.RequestCredential(userCredential.secureId, role,
               userCredential.credential,
               credential_storage,propertyStorage );
         }
         rc.saveCredential();
         log.debug("About to return RequestCredential = " + rc);
         return rc;
      } catch(Exception e) {
         log.fatal(e);
         RuntimeException re = new RuntimeException(e.toString());
         log.fatal("About to throw runtime " +
            "exception" + re + "generated from " + e);
         throw re;
      }
   }
   
   
   
   private void setUpEnv(org.apache.axis.MessageContext msgContext) {
      Object tmp =
         msgContext.getProperty(org.apache.axis.transport.http.HTTPConstants.MC_HTTP_SERVLETREQUEST);

      if ((tmp == null) || !(tmp instanceof javax.servlet.http.HttpServletRequest)) {
         return;
      }

      javax.servlet.http.HttpServletRequest req = (javax.servlet.http.HttpServletRequest)tmp;

      tmp = req.getAttribute(GSIConstants.GSI_CONTEXT);

      if (tmp != null) {
         msgContext.setProperty(GSIConstants.GSI_CONTEXT, tmp);
      }

      tmp = req.getRemoteAddr();
      if (tmp != null) {
         msgContext.setProperty(REMOTE_ADDR, tmp);
      }
   }
   
  public static Collection <String> getFQANsFromContext(ExtendedGSSContext gssContext, boolean validate) throws Exception {
        X509Certificate[] chain = (X509Certificate[]) gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        return getFQANsFromX509Chain(chain, validate);
    }
    
    public static Collection <String> getFQANsFromContext(ExtendedGSSContext gssContext) throws SRMAuthorizationException {
      try {
            X509Certificate[] chain = (X509Certificate[]) gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
            return getFQANsFromX509Chain(chain, false);
      } catch (Exception e) {
         throw new SRMAuthorizationException("getFQANsFromContext failed", e);
      }
    }
    
    public static Collection <String> getValidatedFQANsFromX509Chain(X509Certificate[] chain) throws Exception {
        return getFQANsFromX509Chain(chain, true);
    }
    
    public static Collection <String> getFQANsFromX509Chain(X509Certificate[] chain) throws Exception {
        return getFQANsFromX509Chain(chain, false);
    }
    
    public static Collection <String> getFQANsFromX509Chain(X509Certificate[] chain, boolean validate) throws Exception {
        VOMSValidator validator = new VOMSValidator(chain);
        return getFQANsFromX509Chain(validator, validate);
    }
    
    public static Collection <String> getFQANsFromX509Chain(VOMSValidator validator, boolean validate) throws Exception {
        
        if(!validate) return getFQANs(validator);
        Collection <String> validatedroles;
        
        try {
            validatedroles = getValidatedFQANs(validator);
            //if(!role.equals(validatedrole))
            //hrow new AuthorizationServiceException("role "  + role + " did not match validated role " + validatedrole);
        } catch(org.ietf.jgss.GSSException gsse ) {
            throw new SRMAuthorizationException(gsse.toString());
        } catch(Exception e) {
            throw new SRMAuthorizationException("Could not validate role.");
        }
        
        return validatedroles;
    }
    
    public static Collection <String> getFQANs(X509Certificate[] chain) throws GSSException {
        VOMSValidator validator = new VOMSValidator(chain);
        return getFQANs(validator);
    }
    
    public static Collection <String> getFQANs(VOMSValidator validator) throws GSSException {
        LinkedHashSet <String> fqans = new LinkedHashSet <String> ();
        validator.parse();
        List listOfAttributes = validator.getVOMSAttributes();
        
        Iterator i = listOfAttributes.iterator();
        while (i.hasNext()) {
            VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
            List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
            Iterator j = listOfFqans.iterator();
            if (j.hasNext()) {
                fqans.add((String) j.next());
            }
        }
        
        return fqans;
    }
    
    public static Collection <String> getValidatedFQANArray(X509Certificate[] chain) throws GSSException {
        VOMSValidator validator = new VOMSValidator(chain);
        return getValidatedFQANs(validator);
    }
    
    public static Collection <String> getValidatedFQANs(VOMSValidator validator) throws GSSException {
        LinkedHashSet <String> fqans = new LinkedHashSet <String> ();
        validator.validate();
        List listOfAttributes = validator.getVOMSAttributes();
        
        Iterator i = listOfAttributes.iterator();
        while (i.hasNext()) {
            VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
            List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
            Iterator j = listOfFqans.iterator();
            if (j.hasNext()) {
                fqans.add((String) j.next());
            }
        }
        
        return fqans;
    }
    
    public static class NullIterator<String> implements Iterator {
        private boolean hasnext = true;
        public boolean hasNext() { return hasnext; }
        public String next()
        throws java.util.NoSuchElementException {
            if(hasnext) {
                hasnext = false;
                return null;
            }
            throw new java.util.NoSuchElementException("no more nulls");
        }
        
        public void remove() {}
    }
    
    public static String getFormattedAuthRequestID(long id) {
        String idstr;
        idstr = String.valueOf(id);
        while (idstr.length()<10) idstr = " " + idstr;
        return idstr;
    }

}
