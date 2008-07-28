// $Id: AuthorizationService.java,v 1.38 2007-10-19 20:50:02 tdh Exp $
// $Log: not supported by cvs2svn $
// Revision 1.37  2007/08/03 20:20:01  timur
// implementing some of the findbug bugs and recommendations, avoid selfassignment, possible nullpointer exceptions, syncronization issues, etc
//
// Revision 1.36  2007/08/03 15:46:02  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes etc, per findbug recommendations
//
// Revision 1.35  2007/08/02 16:00:28  timur
// reuse instance of Random, per findbugs recommendation
//
// Revision 1.34  2007/06/08 18:23:06  tdh
// Provisional fix for DN extraction from certificate chain.
//
// Revision 1.33  2007/05/31 19:28:59  tdh
// Use getIssuer instead of getSubject for DN.
//
// Revision 1.32  2007/05/31 18:13:09  tdh
// Remove excision of CN=\d\d\d\d\d\d from SubjectDN.
//
// Revision 1.31  2007/05/01 19:19:50  tdh
// Clip "CN=limited proxy" string from end of DN.
//
// Revision 1.30  2007/04/17 21:48:31  tdh
// Added print of line number when catching generic throwable from plugin.
//
// Revision 1.29  2007/03/27 19:20:28  tdh
// Merge of support for multiple attributes from 1.7.1.
//
// Revision 1.28  2007/03/16 22:36:16  tdh
// Propagate requested username.
//
// Revision 1.27  2007/03/16 21:59:49  tdh
// Added ability to request username.
// Config files are read only if they have changed.
//
// Revision 1.26  2007/02/08 20:27:45  tdh
// Added code for dynamic mapping of uid, gid, etc.
//
// Revision 1.25  2007/01/31 17:56:45  tdh
// Added field and methods for vomsValidation to AuthorizationConfig.
// Added option to validate attributes in AuthorizationService.
// Made static BasicTrustStore instantiation in AuthorizationService use /etc/grid-security/vomsdir, or, if empty, certificates directory.
//
// Revision 1.24  2007/01/12 19:28:45  tdh
// Will throw exception REVOCATION_MESSAGE is caught.
// Fixed DN field ordering.
// Fixed authorization being done twice since SAZ was added.
//
// Revision 1.23  2007/01/04 17:45:03  tdh
// Forwarding log level to SAZ plugin.
// Using full chain to authorize.
// Fixed order of DN fields.
//
// Revision 1.22  2006/12/21 22:16:48  tdh
// Service context used to set up socket with SAZ server.
// Convert bouncycastle DN to globus form.
// Improved error reporting, exception handling.
// Setting DN in results.
// Moved some functions from GPLAZMA to AuthorizationService.
//
// Revision 1.21  2006/12/15 15:56:30  tdh
// Added support for SAZ authorization.
//
// Revision 1.20  2006/12/07 20:28:49  tdh
// Added static method to extract FQAN from context. Authorization using context calls
// this method, then queries gPlazma using group and role; SRM and gridftp no longer
// delegate credentials to gPlamza. Static method replaced code in plugins.
//
// Revision 1.19  2006/11/29 19:11:35  tdh
// Added ability to set log level and propagate to plugins.
//
// Revision 1.18  2006/11/28 21:14:55  tdh
// Check that caller is not null.
//
// Revision 1.17  2006/11/28 21:13:20  tdh
// Set log4j log level in plugins according to printout level of calling cell.
//
// Revision 1.16  2006/11/13 16:33:41  tigran
// fixed CLOSE_WAIT:
// delegated socked was no closed
//
// Revision 1.15  2006/09/07 20:12:52  tdh
// Propagate fix of Revision 1.11.4.4 to development branch.
//
// Revision 1.14  2006/08/23 16:40:05  tdh
// Propagate authorization request ID to plugins so log entries can be tagged with it.
//
// Revision 1.13  2006/08/07 16:38:03  tdh
// Merger of changes from branch, exception handling and ignore blank config file lines.
//
// Revision 1.11.4.3  2006/08/07 16:23:41  tdh
// Added exception handling lines to other authorization methods.
//
// Revision 1.11.4.2  2006/08/07 15:55:23  tdh
// Checks that authorization is null before throwing AuthorizationServiceException.
//
// Revision 1.11.4.1  2006/07/26 18:41:59  tdh
// Backport of recent changes to development branch.
//
// Revision 1.12  2006/07/25 14:58:45  tdh
// Merged DN/Role authentication. Added logging and authRequestID code.
//
// Revision 1.11.2.2  2006/07/12 19:45:57  tdh
// Uncommented GPLAZMALiteVORoleAuthzPlugin and added CVS line.
//

package diskCacheV111.services.authorization;

import java.util.*;
import java.io.*;
import java.lang.*;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.security.cert.X509Certificate;

import diskCacheV111.util.*;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpTransferProtocolInfo;
import diskCacheV111.vehicles.transferManager.RemoteGsiftpDelegateUserCredentialsMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.DNInfo;
import diskCacheV111.vehicles.AuthenticationMessage;
import diskCacheV111.vehicles.X509Info;
import org.dcache.auth.UserAuthRecord;

//import org.apache.log4j.Logger;
//import org.apache.log4j.Level;

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
import org.globus.gsi.GSIConstants;
import org.globus.gsi.bc.BouncyCastleUtil;
import org.globus.gsi.bc.X509NameHelper;
import org.dcache.srm.security.SslGsiSocketFactory;
import org.gridforum.jgss.ExtendedGSSManager;
import org.gridforum.jgss.ExtendedGSSContext;
import org.glite.security.voms.VOMSValidator;
import org.glite.security.voms.VOMSAttribute;
import org.glite.security.voms.BasicVOMSTrustStore;
import org.glite.security.util.DirectoryList;
import org.bouncycastle.asn1.x509.TBSCertificateStructure;
import org.bouncycastle.asn1.x509.X509Name;
import org.bouncycastle.asn1.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import dmg.cells.nucleus.*;

/**
 *
 * @author Abhishek Singh Rana, Timur Perelmutov
 */

public class AuthorizationService {
    
    public static final String GPLAZMA_SRMDCACHE_RELEASE_VERSION="0.1-1";
    private static String service_key           = "/etc/grid-security/hostkey.pem";
    private static String service_cert          = "/etc/grid-security/hostcert.pem";
    private static String service_trusted_certs = "/etc/grid-security/certificates";
    private static String vomsdir = "/etc/grid-security/vomsdir";
    private List l = null;
    AuthorizationServicePlugin kPlug;
    AuthorizationServicePlugin VOPlug;
    AuthorizationServicePlugin gPlug;
    AuthorizationServicePlugin liteVORolePlug;
    private String authConfigFilePath;
    private long authRequestID;
    private String authRequestID_str;
    CellAdapter caller;
    private String loglevel=null;
    private boolean delegate_to_gplazma=false;
    //public static final Pattern pattern1 = Pattern.compile("/CN=proxy");
    //public static final Pattern pattern2 = Pattern.compile("/CN=\\d{6,}");
    //public static final Pattern pattern3 = Pattern.compile("/CN=limited proxy");
    /** Whether to use SAZ callout if gsscontext is available. **/
    private boolean use_saz=false;
    private boolean omitEmail=false;
    private AuthorizationConfig authConfig=null;
    private Vector pluginPriorityConfig;
    private DateFormat _df   = new SimpleDateFormat("MM/dd HH:mm:ss" );
    public static final String capnull = "/Capability=NULL";
    public static final int capnulllen = capnull.length();
    public static final String rolenull ="/Role=NULL";
    public static final int rolenulllen = rolenull.length();
    
    static {
        Logger.getLogger(org.glite.security.trustmanager.CRLFileTrustManager.class.getName()).setLevel(Level.ERROR);
        Logger.getLogger("org.glite.security.trustmanager.axis.AXISSocketFactory").setLevel(Level.OFF);
        Logger.getLogger("org.glite.security.util.DirectoryList").setLevel(Level.OFF);
        try {
            new DirectoryList(vomsdir).getListing();
        } catch (IOException e) {
            vomsdir = service_trusted_certs;
        }
        VOMSValidator.setTrustStore(new BasicVOMSTrustStore(vomsdir, 12*3600*1000));
    }
    
    private static final Random random = new Random();
    public AuthorizationService()
    throws AuthorizationServiceException {
        this(null, random.nextInt(Integer.MAX_VALUE), null);
    }
    
    public AuthorizationService(String authservConfigFilePath)
    throws AuthorizationServiceException {
        this(authservConfigFilePath, random.nextInt(Integer.MAX_VALUE), null);
    }
    
    public AuthorizationService(long authRequestID)
    throws AuthorizationServiceException {
        this(null, authRequestID, null);
    }
    
    public AuthorizationService(CellAdapter caller)
    throws AuthorizationServiceException {
        this(null, random.nextInt(Integer.MAX_VALUE), caller);
    }
    
    public AuthorizationService(String authservConfigFilePath, long authRequestID)
    throws AuthorizationServiceException {
        this(authservConfigFilePath, authRequestID, null);
    }
    
    public AuthorizationService(String authservConfigFilePath, CellAdapter caller)
    throws AuthorizationServiceException {
        this(authservConfigFilePath, random.nextInt(Integer.MAX_VALUE), caller);
    }
    
    public AuthorizationService(String authservConfigFilePath, long authRequestID, CellAdapter caller)
    throws AuthorizationServiceException {
        this.authConfigFilePath = authservConfigFilePath;
        this.authRequestID=authRequestID;
        this.caller = caller;
        authRequestID_str = getFormattedAuthRequestID(authRequestID);
    }
    
    public long getAuthRequestID() {
        return authRequestID;
    }
    
    private void say(String s) {
        if(caller!=null) {
            caller.say("authRequestID " + authRequestID + " " + s);
        } else {
            System.out.println(_df.format(new Date()) + " authRequestID " + authRequestID + " " + s);
        }
    }
    
    private void esay(String s) {
        if(caller!=null) {
            caller.esay("authRequestID " + authRequestID_str + " " + s);
        } else {
            System.err.println(_df.format(new Date()) + " authRequestID " + authRequestID_str + " " + s);
        }
    }
    
    private void addPlugin(AuthorizationServicePlugin plugin)
    throws AuthorizationServiceException {
        try {
            if(plugin == null) {
                esay("Plugin is null and cannot be added.");
            } else {
                forwardLogLevel(plugin);
                l.add(plugin);
            }
        } catch (Exception e ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " Exception adding Plugin: " +e);
        }
    }
    
    
    private void forwardLogLevel(AuthorizationServicePlugin plugin) {
        if(loglevel==null) {
            if(caller!=null && (caller.getNucleus().getPrintoutLevel() & CellNucleus.PRINT_CELL ) > 0 )
                plugin.setLogLevel("INFO");
            else
                plugin.setLogLevel("ERROR");
        } else {
            plugin.setLogLevel(loglevel);
        }
    }
    
    private void buildDCacheAuthzPolicy()
    throws AuthorizationServiceException {
        
        String kpwdPath;
        String VOMapUrl;
        String gridmapfilePath;
        String storageAuthzDbPath;
        String gPLAZMALiteVORoleMapPath;
        String gPLAZMALiteStorageAuthzDbPath;
        
        if(authConfig!=null) return;
        
        try {
            authConfig = new AuthorizationConfig(authConfigFilePath);
        } catch(java.io.IOException ioe) {
            esay("Exception in AuthorizationConfig instantiation :" + ioe);
            throw new AuthorizationServiceException(ioe.toString());
        }
        try {
            pluginPriorityConfig = authConfig.getpluginPriorityConfig();
            ListIterator iter = pluginPriorityConfig.listIterator(0);
            while (iter.hasNext()) {
                String thisSignal = (String)iter.next();
                if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getKpwdSignal())) ) {
                    try {
                        try {
                            kpwdPath = authConfig.getKpwdPath();
                        } catch(Exception e) {
                            esay("Exception getting Kpwd Path from configuration :" +e);
                            throw new AuthorizationServiceException(e.toString());
                        }
                        if (kpwdPath != null && !kpwdPath.equals("")) {
                            AuthorizationServicePlugin kPlug = new KPWDAuthorizationPlugin(kpwdPath, authRequestID);
                            addPlugin(kPlug);
                        } else {
                            esay("Kpwd Path not well-formed in configuration.");
                        }
                    } catch (AuthorizationServiceException ae) {
                        esay("Exception: " +ae);
                    }
                }//end of kpwd-if
                else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getVOMappingSignal())) ) {
                    try {
                        try {
                            VOMapUrl = authConfig.getMappingServiceUrl();
                        } catch(Exception e) {
                            esay("Exception getting VO Map Url from configuration : " +e);
                            throw new AuthorizationServiceException(e.toString());
                        }
                        if (VOMapUrl != null && !VOMapUrl.equals("")) {
                            AuthorizationServicePlugin VOPlug = new VOAuthorizationPlugin(VOMapUrl, authRequestID);
                            ((VOAuthorizationPlugin) VOPlug).setCacheLifetime(authConfig.getMappingServiceCacheLifetime());
                            addPlugin(VOPlug);
                        } else {
                            esay("VO Map Url not well-formed in configuration.");
                        }
                    } catch (AuthorizationServiceException ae) {
                        esay("Exception : " +ae);
                    }
                }//end of saml-based-vo-mapping-if
                else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getGPLiteVORoleMappingSignal())) ) {
                    try {
                        try {
                            gPLAZMALiteVORoleMapPath = authConfig.getGridVORoleMapPath();
                            gPLAZMALiteStorageAuthzDbPath = authConfig.getGridVORoleStorageAuthzPath();
                        } catch(Exception e) {
                            esay("Exception getting Grid VO Role Map or Storage Authzdb paths from configuration :" +e);
                            throw new AuthorizationServiceException(e.toString());
                        }
                        if (gPLAZMALiteVORoleMapPath != null && gPLAZMALiteStorageAuthzDbPath != null &&
                                !gPLAZMALiteVORoleMapPath.equals("") && !gPLAZMALiteStorageAuthzDbPath.equals("")) {
                            AuthorizationServicePlugin liteVORolePlug = new GPLAZMALiteVORoleAuthzPlugin(gPLAZMALiteVORoleMapPath, gPLAZMALiteStorageAuthzDbPath, authRequestID);
                            addPlugin(liteVORolePlug);
                        } else {
                            esay("Grid VO Role Map or Storage Authzdb paths not well-formed in configuration");
                        }
                    } catch (AuthorizationServiceException ae) {
                        esay("Exception : " +ae);
                    }
                }//end of gplazmalite-vorole-mapping-if
                else if ( (thisSignal != null) && (thisSignal.equals((String)authConfig.getGridMapFileSignal())) ) {
                    try {
                        try {
                            gridmapfilePath = authConfig.getGridMapFilePath();
                            storageAuthzDbPath = authConfig.getStorageAuthzPath();
                        } catch(Exception e) {
                            esay("Exception getting GridMap or Storage Authzdb path from configuration :" +e);
                            throw new AuthorizationServiceException(e.toString());
                        }
                        if (gridmapfilePath != null && storageAuthzDbPath != null &&
                                !gridmapfilePath.equals("") && !storageAuthzDbPath.equals("")) {
                            AuthorizationServicePlugin gPlug  = new GridMapFileAuthzPlugin(gridmapfilePath, storageAuthzDbPath, authRequestID);
                            addPlugin(gPlug);
                        } else {
                            esay("GridMap or Storage Authzdb paths not well-formed in configuration");
                        }
                    } catch (AuthorizationServiceException ae) {
                        esay("Exception : " +ae);
                    }
                }//end of gplazmalite-gridmapfile-if
            }//end of while
        } catch(Exception cpe) {
            esay("Exception processing Choice|Priority Configuration :" + cpe);
            throw new AuthorizationServiceException(cpe.toString());
        }
        
    }
    
    public LinkedList <UserAuthRecord> authorize(GSSContext context, String desiredUserName, String serviceUrl, Socket socket)
    throws AuthorizationServiceException {
        AuthorizationServiceException authexceptions=null;
        
        X509Certificate[] chain;
        ExtendedGSSContext extendedcontext;
        try {
            if (context instanceof ExtendedGSSContext) {
                extendedcontext = (ExtendedGSSContext) context;
            } else {
                esay("Received context not instance of ExtendedGSSContext, AuthorizationService exiting ...");
                authexceptions = new  AuthorizationServiceException("\nException thrown by " + this.getClass().getName() +
                        ": Received context not instance of ExtendedGSSContext");
                throw authexceptions;
            }
            
            chain=(X509Certificate[])extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        } catch(Exception e) {
            esay("AuthorizationService - Exception: " + e);
            authexceptions = new  AuthorizationServiceException("\nException thrown by " + this.getClass().getName() + ": " + e.getMessage());
            throw authexceptions;
        }
        
        return authorize(chain, desiredUserName, serviceUrl, socket);
    }
    
    public LinkedList <UserAuthRecord> authorize(X509Certificate[] chain, String desiredUserName, String serviceUrl, Socket socket)
    throws AuthorizationServiceException {
        AuthorizationServiceException authexceptions=null;
        
        Iterator plugins = getPlugins();
        if (!plugins.hasNext()) return new LinkedList <UserAuthRecord> ();
        
        try {
            setUseSAZ(authConfig.getSazClientOn());
        } catch(Exception e) {
            authexceptions = new  AuthorizationServiceException("\nException thrown by " + authConfig.getClass().getName() + ": " + e.getMessage());
            throw authexceptions;
        }
        
        if (getUseSAZ()) {
            SAZAuthorizationPlugin sazclient;
            GSSContext context=null;
            Socket sazsocket=null;
            try {
                sazclient = new SAZAuthorizationPlugin(null, authRequestID);
                forwardLogLevel(sazclient);
                context=AuthorizationService.getServiceContext();
                //context=getUserContext("/tmp/x509up_u500");
                sazsocket = AuthorizationService.getGsiClientSocket(authConfig.getSazServerHost(),
                        Integer.parseInt(authConfig.getSazServerPort()), (ExtendedGSSContext) context);
                ((GssSocket)sazsocket).setUseClientMode(true);
                //UserAuthRecord sazauth = sazclient.authorize(new X509Certificate[]{chain[i]}, desiredUserName, serviceUrl, sazsocket);
                UserAuthRecord sazauth = sazclient.authorize(chain, desiredUserName, serviceUrl, sazsocket);
                if(sazauth==null) {
                    authexceptions = new  AuthorizationServiceException("Authorization denied by SAZ");
                    throw authexceptions;
                }
            } catch(Exception e) {
                if(e instanceof AuthorizationServiceException)
                    authexceptions = new  AuthorizationServiceException(" from SAZAuthorizationPlugin : " + e.getMessage());
                else
                    authexceptions = new  AuthorizationServiceException(" from SAZAuthorizationPlugin : " + e);
                try {
                    if(context!=null) context.dispose();
                    if(sazsocket!=null) sazsocket.close();
                } catch (Exception de) {
                    String error = authexceptions==null?"":authexceptions.getMessage() + 
                            ": Exception thrown by SAZAuthorizationPlugin: " + de.getMessage();
                    authexceptions = new  AuthorizationServiceException(error);
                }
                throw authexceptions;
            }
            try {
                if(context!=null) context.dispose();
                if(sazsocket!=null) sazsocket.close();
            } catch (Exception de) {
                String error = authexceptions==null?"":authexceptions.getMessage() + 
                        ": Exception thrown by SAZAuthorizationPlugin: " + de.getMessage();
                authexceptions = new  AuthorizationServiceException(error);
            }
        }
        
        String subjectDN;
        Collection <String> roles;
        
        try {
            subjectDN = getSubjectFromX509Chain(chain);
        } catch(Exception e) {
            String msg = e.getMessage();
            if(msg==null)
                throw new AuthorizationServiceException("Could not extract subject DN from certificate chain");
            else throw new AuthorizationServiceException("Could not extract subject DN from certificate chain: "  + msg);
        }
        
        try {
            roles = getFQANsFromX509Chain(chain, authConfig.getVOMSValidation());
        } catch(Exception e) {
            String msg = e.getMessage();
            if(msg==null)
                throw new AuthorizationServiceException("for subject DN " + subjectDN);
            else throw new AuthorizationServiceException("for subject DN " + subjectDN + " " + msg);
        }
        
        return authorize(subjectDN, roles, desiredUserName, serviceUrl, socket);
        
    }
    
    public LinkedList <UserAuthRecord> authorize(String subjectDN, Iterable <String> roles, String desiredUserName, String serviceUrl, Socket socket)
    throws AuthorizationServiceException {
        LinkedList <UserAuthRecord> records = new LinkedList <UserAuthRecord> ();
        
        AuthorizationServiceException authexceptions=null;
        
        Iterator <String> roleIter = roles.iterator();
        if(!roleIter.hasNext()) roleIter = new NullIterator<String>();
        while (roleIter.hasNext()) {
            String role = roleIter.next();
            UserAuthRecord r=null;
            try {
                r = authorize(subjectDN, role, desiredUserName, serviceUrl, socket);
            } catch (AuthorizationServiceException ase) {
                if(authexceptions==null)
                    authexceptions = ase;
                else
                    authexceptions = new  AuthorizationServiceException(authexceptions.getMessage() + "\n" + ase.getMessage());
            }
            if(r!=null && !records.contains(r)) {
                records.add(r);
                if(desiredUserName!=null) break;
            }
        }
        
        if(authexceptions!=null && records.isEmpty()) throw authexceptions;
        return records;
    }
    
    
    public UserAuthRecord authorize(String subjectDN, String role, String desiredUserName, String serviceUrl, Socket socket)
    throws AuthorizationServiceException {
        UserAuthRecord r = null;
        AuthorizationServiceException authexceptions=null;
        
        AuthorizationServicePlugin p;
        Iterator plugins = getPlugins();
        while (r==null && plugins.hasNext()) {
            p = (AuthorizationServicePlugin) plugins.next();
            try {
                r = p.authorize(subjectDN, role, desiredUserName, serviceUrl, socket);
            } catch(AuthorizationServiceException ae) {
                if(authexceptions==null)
                    authexceptions = new  AuthorizationServiceException("\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
                else
                    authexceptions = new  AuthorizationServiceException(authexceptions.getMessage() + "\nException thrown by " + p.getClass().getName() + ": " + ae.getMessage());
                //if(ae.getMessage().equals(AuthorizationServicePlugin.REVOCATION_MESSAGE)) throw authexceptions;
            }
        }
        
        if(authexceptions!=null && r==null) throw authexceptions;
        
        return r;
    }
    
    public Iterator getPlugins() throws AuthorizationServiceException {
        
        if(l==null) {
            l = new LinkedList();
            
            try {
                buildDCacheAuthzPolicy();
                //System.out.println("- - -               Deactivating     g P L A Z M A    Policy   Loader                 - - - ");
            }	catch(AuthorizationServiceException aue) {
                esay("Exception in building DCache Authz Policy: " + aue);
                throw new AuthorizationServiceException(aue.toString());
            }
            
            //System.out.println("Number of authorization mechanisms being used: " +maxm);
            if (l.size() == 0) {
                say("All Authorization OFF!  System Quasi-firewalled!");
                //return null;
            }
        }
        
        return l.listIterator(0);
    }
    
    // Called on requesting cel
    //
    public AuthenticationMessage authenticate(GSSContext serviceContext, String user, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        
        RemoteGsiftpTransferProtocolInfo protocolInfo =
                new RemoteGsiftpTransferProtocolInfo(
                "RemoteGsiftpTransfer",
                1,1,null,0,
                null,
                caller.getNucleus().getCellName(),
                caller.getNucleus().getCellDomainName(),
                authRequestID,
                0,
                0,
                0,
                new Long(0),
                user);
        //long authRequestID = protocolInfo.getId();
        
        return  authenticate(serviceContext, protocolInfo, cellpath, caller);
    }
    
    public AuthenticationMessage authenticate(GSSContext serviceContext, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        
        RemoteGsiftpTransferProtocolInfo protocolInfo =
                new RemoteGsiftpTransferProtocolInfo(
                "RemoteGsiftpTransfer",
                1,1,null,0,
                null,
                caller.getNucleus().getCellName(),
                caller.getNucleus().getCellDomainName(),
                authRequestID,
                0,
                0,
                0,
                new Long(0));
        //long authRequestID = protocolInfo.getId();
        
        return  authenticate(serviceContext, protocolInfo, cellpath, caller);
    }
    
    public AuthenticationMessage authenticate(GSSContext serviceContext, RemoteGsiftpTransferProtocolInfo protocolInfo, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        
        if(!delegate_to_gplazma) {
            //if (do_saz) {
            X509Certificate[] chain;
            
            ExtendedGSSContext extendedcontext;
            
            if (serviceContext instanceof ExtendedGSSContext) {
                extendedcontext = (ExtendedGSSContext) serviceContext;
            } else {
                esay("Received context not instance of ExtendedGSSContext, AuthorizationService exiting ...");
                return null;
            }
            
            try {
                chain = (X509Certificate[]) extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
            } catch(org.ietf.jgss.GSSException gsse ) {
                esay(" Error extracting X509 chain from GSSContext: " +gsse);
                throw new AuthorizationServiceException(gsse.toString());
            }
            
            return authenticate(chain, protocolInfo.getUser(), cellpath, caller); //todo add getSrcName to args
            //}
            
            //return
        }
        
        AuthenticationMessage authmessage = null;
        
        String authcellname = cellpath.getCellName();
        try {
            GSSName GSSIdentity = serviceContext.getSrcName();
            CellMessage m = new CellMessage(cellpath, protocolInfo);
            m = caller.getNucleus().sendAndWait(m, 40000L) ;
            if(m==null) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " Message to " + authcellname + " timed out for authentification of " + GSSIdentity);
            }
            
            
            Object obj = m.getMessageObject();
            if(obj instanceof RemoteGsiftpDelegateUserCredentialsMessage) {
                if(((Message) obj).getId()!=authRequestID) {
                    throw new AuthorizationServiceException("authRequestID " + authRequestID + " delegation failed: mismatch with returned authRequestID " + ((Message) obj).getId());
                }
                GSIGssSocket authsock=null;
                
                try {
                    
                    authsock = SslGsiSocketFactory.delegateCredential(
                            InetAddress.getByName(((RemoteGsiftpDelegateUserCredentialsMessage) obj).getHost()),
                            ((RemoteGsiftpDelegateUserCredentialsMessage) obj).getPort(),
                            //ExtendedGSSManager.getInstance().createCredential(GSSCredential.INITIATE_ONLY),
                            serviceContext.getDelegCred(),
                            false);
                    //say(this.toString() + " delegation appears to have succeeded");
                } catch ( UnknownHostException uhe ) {
                    throw new AuthorizationServiceException("authRequestID " + authRequestID + " unknown host exception in delegation " + uhe);
                } catch(Exception e) {
                    throw new AuthorizationServiceException("authRequestID " + authRequestID + " delegation failed for authentification of " + GSSIdentity + " " + e);
                }
                
                Object authobj=null;
                if(authsock!=null) {
                    try {
                        authsock.setSoTimeout(30000);
                        InputStream authin = authsock.getInputStream();
                        ObjectInputStream authstrm = new ObjectInputStream(authin);
                        authobj = authstrm.readObject();
                        if(authobj==null) {
                            throw new AuthorizationServiceException("authRequestID " + authRequestID + " authorization object was null for " + GSSIdentity);
                        } else {
                            if( authobj instanceof Exception ) throw (Exception) authobj;
                            authmessage = (AuthenticationMessage) authobj;
                            Iterator <UserAuthRecord> recordsIter = authmessage.getUserAuthRecords().iterator();
                            while (recordsIter.hasNext()) {
                                UserAuthRecord rec = recordsIter.next();
                                String GIDS_str = Arrays.toString(rec.GIDs);
                                caller.say("authRequestID " + authRequestID + " received " + rec.Username + " " + rec.UID + " " + GIDS_str + " " + rec.Root);
                            }
                        }
                    } catch (IOException ioe) {
                        throw new AuthorizationServiceException("authRequestID " + authRequestID + " could not receive authorization object for " + GSSIdentity + " " + ioe);
                    } catch (ClassCastException cce) {
                        throw new AuthorizationServiceException("authRequestID " + authRequestID + " incorrect class for authorization object for " + GSSIdentity + " " + cce);
                    } finally {
                        authsock.close();
                    }
                    
                } else {
                    throw new AuthorizationServiceException("authRequestID " + authRequestID + " socket to receive authorization object was null for " + GSSIdentity);
                }
                
            } else {
                if( obj instanceof NoRouteToCellException )
                    throw (NoRouteToCellException) obj;
                if( obj instanceof Throwable )
                    throw (Throwable) obj;
            }
        } catch ( AuthorizationServiceException ase ) {
            throw ase;
        } catch ( GSSException gsse ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " error getting source from context " + gsse);
        } catch ( NoRouteToCellException nre ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + nre);
        } catch ( InterruptedException ie ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " message thread was interrupted " + ie);
        } catch ( ClassNotFoundException cnfe ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " class of object returned from " + authcellname + " not found " + cnfe);
        } catch ( Throwable t ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " received exception \"" + t.getMessage() + "\" at " + t.getStackTrace()[0]);
        }
        
        return authmessage;
    }
    
    public AuthenticationMessage authenticate(String subjectDN, Collection<String> roles, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        DNInfo dnInfo = new DNInfo(subjectDN, roles, authRequestID);
        return authenticate(dnInfo, cellpath, caller);
    }
    
    public AuthenticationMessage authenticate(String subjectDN, String role, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        DNInfo dnInfo = new DNInfo(subjectDN, role, authRequestID);
        return authenticate(dnInfo, cellpath, caller);
    }
    
    public AuthenticationMessage authenticate(String subjectDN, Collection<String> roles, String user, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        DNInfo dnInfo = new DNInfo(subjectDN, roles, user, authRequestID);
        return authenticate(dnInfo, cellpath, caller);
    }
    
    public AuthenticationMessage authenticate(String subjectDN, String role, String user, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        DNInfo dnInfo = new DNInfo(subjectDN, role, user, authRequestID);
        return authenticate(dnInfo, cellpath, caller);
    }
    
    public AuthenticationMessage authenticate(DNInfo dnInfo, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        
        AuthenticationMessage authmessage = null;
        
        String authcellname = cellpath.getCellName();
        try {
            CellMessage m = new CellMessage(cellpath, dnInfo);
            m = caller.getNucleus().sendAndWait(m, 40000L) ;
            if(m==null) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " Message to " + authcellname + " timed out for authentification of " + dnInfo.getDN() + " and roles " + dnInfo.getFQANs());
            }
            
            Object authobj = m.getMessageObject();
            if (authobj==null) {
                throw new AuthorizationServiceException(
                        "authRequestID " + authRequestID + 
                        " authorization object was null for " + 
                        dnInfo.getDN() + " and roles " + dnInfo.getFQANs());
            }
            else if(authobj instanceof AuthenticationMessage) {
                if(((AuthenticationMessage) authobj).getAuthRequestID()!=authRequestID) {
                    throw new AuthorizationServiceException("authRequestID " + authRequestID + " delegation failed: mismatch with returned authRequestID " + ((Message) authobj).getId());
                }
                authmessage = (AuthenticationMessage) authobj;
                Iterator <UserAuthRecord> recordsIter = authmessage.getUserAuthRecords().iterator();
                while (recordsIter.hasNext()) {
                    UserAuthRecord rec = recordsIter.next();
                    String GIDS_str = Arrays.toString(rec.GIDs);
                    caller.say("authRequestID " + authRequestID + " received " + rec.Username + " " + rec.UID + " " + GIDS_str + " " + rec.Root);
                }
            } else {
                if( authobj instanceof NoRouteToCellException ) {
                    throw (NoRouteToCellException) authobj;
                }
                if( authobj instanceof Throwable ) {
                    throw (Throwable) authobj;
                }
                throw new AuthorizationServiceException("unknown type of authobj");
            }
        } catch ( AuthorizationServiceException ase ) {
            throw ase;
        } catch ( GSSException gsse ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " error getting source from context " + gsse);
        } catch ( NoRouteToCellException nre ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + nre);
        } catch ( InterruptedException ie ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " message thread was interrupted " + ie);
        } catch ( ClassNotFoundException cnfe ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " class of object returned from " + authcellname + " not found " + cnfe);
        } catch ( Throwable t ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " received exception \"" + t.getMessage() + "\" at " + t.getStackTrace()[0]);
        }
        
        return authmessage;
    }
    
    public AuthenticationMessage authenticate(X509Certificate[] chain, String user, CellPath cellpath, CellAdapter caller) throws AuthorizationServiceException {
        
        AuthenticationMessage authmessage = null;
        X509Info x509info = new X509Info(chain, user, authRequestID);
        
        String authcellname = cellpath.getCellName();
        try {
            CellMessage m = new CellMessage(cellpath, x509info);
            m = caller.getNucleus().sendAndWait(m, 3600000L);
            if(m==null) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " Message to " + authcellname + " timed out for authentification of " + getSubjectFromX509Chain(chain));
            }
            
            Object authobj = m.getMessageObject();
            if(authobj==null) {
                throw new AuthorizationServiceException("authRequestID " + authRequestID + " authorization object was null for " + getSubjectFromX509Chain(chain));
            }
            if(authobj instanceof AuthenticationMessage) {
                authmessage = (AuthenticationMessage) authobj;
                if(authmessage.getAuthRequestID()!=authRequestID) {
                    throw new AuthorizationServiceException("authRequestID " + authRequestID + " authentication failed: mismatch with returned authRequestID " + ((Message) authobj).getId());
                }
                Iterator <UserAuthRecord> recordsIter = authmessage.getUserAuthRecords().iterator();
                while (recordsIter.hasNext()) {
                    UserAuthRecord rec = recordsIter.next();
                    String GIDS_str = Arrays.toString(rec.GIDs);
                    caller.say("authRequestID " + authRequestID + " received " + rec.Username + " " + rec.UID + " " + GIDS_str + " " + rec.Root);
                }
            } else {
                if( authobj instanceof Throwable )
                    throw (Throwable) authobj;
                else
                    throw new AuthorizationServiceException("authRequestID " + authRequestID + " incorrect class for authorization object");
            }
        } catch ( AuthorizationServiceException ase ) {
            throw ase;
        } catch ( GSSException gsse ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " error getting source from context " + gsse);
        } catch ( NoRouteToCellException nre ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + nre);
        } catch ( InterruptedException ie ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " message thread was interrupted " + ie);
        } catch ( ClassNotFoundException cnfe ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " class of object returned from " + authcellname + " not found " + cnfe);
        } catch ( Throwable t ) {
            throw new AuthorizationServiceException("authRequestID " + authRequestID + " received exception \"" + t.getMessage() + "\" at " + t.getStackTrace()[0]);
        }
        
        return authmessage;
    }
    
    public static GSSContext getUserContext(String proxy_cert) throws GSSException {
        
        GlobusCredential userCredential;
        try {
            userCredential =new GlobusCredential(proxy_cert, proxy_cert);
        } catch(GlobusCredentialException gce) {
            throw new GSSException(GSSException.NO_CRED , 0,
                    "could not load host globus credentials "+gce.toString());
        }
        
        GSSCredential cred = new GlobusGSSCredentialImpl(
                userCredential,
                GSSCredential.INITIATE_AND_ACCEPT);
        TrustedCertificates trusted_certs =
                TrustedCertificates.load(service_trusted_certs);
        GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSContext context =
                (ExtendedGSSContext) manager.createContext(cred);
        
        context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
        context.setOption(GSSConstants.TRUSTED_CERTIFICATES, trusted_certs);
        
        return context;
    }
    
    public static GSSContext getServiceContext() throws GSSException {
        
        GlobusCredential serviceCredential;
        try {
            serviceCredential =new GlobusCredential(
                    service_cert,
                    service_key
                    );
        } catch(GlobusCredentialException gce) {
            throw new GSSException(GSSException.NO_CRED ,
                    0,
                    "could not load host globus credentials "+gce.toString());
        }
        
        
        GSSCredential cred = new GlobusGSSCredentialImpl(serviceCredential,
                GSSCredential.INITIATE_AND_ACCEPT);
        TrustedCertificates trusted_certs =
                TrustedCertificates.load(service_trusted_certs);
        GSSManager manager = ExtendedGSSManager.getInstance();
        ExtendedGSSContext context =
                (ExtendedGSSContext) manager.createContext(cred);
        
        context.setOption(GSSConstants.GSS_MODE,
                GSIConstants.MODE_GSI);
        context.setOption(GSSConstants.TRUSTED_CERTIFICATES,
                trusted_certs);
        
        return context;
    }
    
    public static Socket getGsiClientSocket(String host, int port, ExtendedGSSContext context) throws Exception {
        Socket clientSocket = GssSocketFactory.getDefault().createSocket(host, port, context);
        ((GssSocket)clientSocket).setWrapMode(GssSocket.GSI_MODE);
        ((GssSocket)clientSocket).setAuthorization(NoAuthorization.getInstance());
        return(clientSocket);
    }
    /**
     * Returns the Globus formatted representation of the
     * subject DN of the specified DN.
     *
     * @param dn the DN
     * @return the Globus formatted representation of the
     *         subject DN.
     */
    public static String toGlobusID(Vector dn) {
        
        int len = dn.size();
        StringBuffer buf = new StringBuffer();
        for (int i=0;i<len;i++) {
            Vector rdn = (Vector)dn.elementAt(i);
            // checks only first ava entry
            String [] ava = (String[])rdn.elementAt(0);
            buf.append('/').append(ava[0]).append('=').append(ava[1]);
        }
        return buf.toString();
    }
    
    /**
     * Converts the certificate dn into globus dn representation:
     * 'cn=proxy, o=globus' into '/o=globus/cn=proxy'
     *
     * @param  certDN regural dn
     * @return globus dn representation
     */
    public static String toGlobusDN(String certDN) {
        StringTokenizer tokens = new StringTokenizer(certDN, ",");
        StringBuffer buf = new StringBuffer();
        String token;
        
        while(tokens.hasMoreTokens()) {
            token = tokens.nextToken().trim();
            buf.insert(0, token);
            buf.insert(0, "/");
        }
        
        return buf.toString();
    }
    
    public String getSubjectFromX509Chain(X509Certificate[] chain) throws Exception {
        String subjectDN;

        TBSCertificateStructure tbsCert=null;
        X509Certificate	clientcert=null;
        //int clientcertindex = CertUtil.findClientCert(chain);
        for (int i=0; i<chain.length; i++) {
            X509Certificate	testcert = chain[i];
    //DERObject obj = BouncyCastleUtil.toDERObject(testcert.getTBSCertificate());
	//tbsCert  =  TBSCertificateStructure.getInstance(obj);
            tbsCert  = BouncyCastleUtil.getTBSCertificateStructure(testcert);
            int certType = BouncyCastleUtil.getCertificateType(tbsCert);
            if (!org.globus.gsi.CertUtil.isImpersonationProxy(certType)) {
                clientcert = chain[i];
                break;
            }
        }
        
        if(clientcert == null) {
            throw new AuthorizationServiceException("could not find clientcert");
        }
        //subjectDN = clientcert.getSubjectX500Principal().toString();
        //subjectDN = clientcert.getSubjectDN().toString();
        //subjectDN = X509NameHelper.toString((X509Name)clientcert.getSubjectDN());
        //subjectDN = toGlobusDN(subjectDN);
        subjectDN = X509NameHelper.toString(tbsCert.getSubject());


        //ASN1Sequence seq = (ASN1Sequence)BouncyCastleUtil.duplicate(tbsCert.getSubject().getDERObject());
        subjectDN = toGlobusString((ASN1Sequence)tbsCert.getSubject().getDERObject());
        
        // Find End-Entity Certificate, e.g. user certificate
        
        //byte[] encoded = chain[clientcertindex].getEncoded();
        //X509Cert cert = new X509Cert(encoded);
        //TBSCertificateStructure issuerTbsCert  = BouncyCastleUtil.getTBSCertificateStructure(chain[clientcertindex]);
        //X509Certificate	testcert = chain[1];
        //TBSCertificateStructure tbsCert  = BouncyCastleUtil.getTBSCertificateStructure(testcert);
        //int certType = BouncyCastleUtil.getCertificateType(tbsCert, trustedCerts);
        //BouncyCastleUtil.getIdentity(this.identityCert);
        
        //if (org.globus.gsi.CertUtil.isImpersonationProxy(certType)) {
        // throw exception
        //}
        //String identity = X509NameHelper.toString((X509Name)chain[clientcertindex].getSubjectDN());
        //String identity = BouncyCastleUtil.getIdentity(chain[clientcertindex]);
        
        //GlobusGSSContextImpl.GSSProxyPathValidator validator = new GSSProxyPathValidator();
        //ProxyPathValidator validator = new ProxyPathValidator();
        
        //try {
        //  validator.validate(chain, null, null);
        //} catch (Exception e) {throw e;}
        //subjectDN = new GlobusGSSName(identity).toString();
        //subjectDN = validator.getIdentity();
        
        //Vector userCerts = PureTLSUtil.certificateChainToVector(chain);
    /*
    X509Cert cert = new X509Cert(clientcert.getEncoded());
                ByteArrayInputStream in = new ByteArrayInputStream(cert.getDER());
                X509Certificate clientX509cert = org.globus.gsi.CertUtil.loadCertificate(in);
    subjectDN = BouncyCastleUtil.getIdentity(clientX509cert);
     */
        
    /*
    if( subjectDN.startsWith("CN=") ||
        subjectDN.startsWith("E=")  ||
        subjectDN.substring(0,6).toLowerCase().startsWith("email="))
      subjectDN = toGlobusDN(subjectDN);
    else
      subjectDN = "/" + subjectDN.replace(',', '/');
     */
        
    /*Matcher m1 = pattern1.matcher(subjectDN);
    subjectDN = m1.replaceAll("");
    //Matcher m2 = pattern2.matcher(subjectDN);
    //subjectDN = m2.replaceAll("");
    Matcher m3 = pattern3.matcher(subjectDN);
    subjectDN = m3.replaceAll("");
     */
        
        return subjectDN;
    }

    private String toGlobusString(ASN1Sequence seq) {
	  if (seq == null) {
	    return null;
	  }

	  Enumeration e = seq.getObjects();
	  StringBuffer buf = new StringBuffer();
        while (e.hasMoreElements()) {
            ASN1Set set = (ASN1Set)e.nextElement();
	    Enumeration ee = set.getObjects();
	    boolean didappend = false;
	    while (ee.hasMoreElements()) {
		ASN1Sequence s = (ASN1Sequence)ee.nextElement();
		DERObjectIdentifier oid = (DERObjectIdentifier)s.getObjectAt(0);
		String sym = (String)X509Name.OIDLookUp.get(oid);
        if (oid.equals(X509Name.EmailAddress) && omitEmail) {
            say("Omitting email field from DN: " + sym + "=" + ((DERString)s.getObjectAt(1)).getString());
            continue;
        }
        if(!didappend) { buf.append('/'); didappend = true; }
        if (sym == null) {
		    buf.append(oid.getId());
		} else {
		    buf.append(sym);
		}
		buf.append('=');
		buf.append( ((DERString)s.getObjectAt(1)).getString());
		if (ee.hasMoreElements()) {
		    buf.append('+');
		}
	    }
	  }

	  return buf.toString();
    }
    
    public static Collection <String> getFQANsFromContext(ExtendedGSSContext gssContext, boolean validate) throws Exception {
        X509Certificate[] chain = (X509Certificate[]) gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        return getFQANsFromX509Chain(chain, validate);
    }
    
    public static Collection <String> getFQANsFromContext(ExtendedGSSContext gssContext) throws Exception {
        X509Certificate[] chain = (X509Certificate[]) gssContext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        return getFQANsFromX509Chain(chain, false);
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
            throw new AuthorizationServiceException(gsse.toString());
        } catch(Exception e) {
            throw new AuthorizationServiceException("Could not validate role.");
        }
        
        return validatedroles;
    }
    
    public static Collection <String> getFQANs(X509Certificate[] chain) throws GSSException {
        VOMSValidator validator = new VOMSValidator(chain);
        return getFQANs(validator);
    }

  /**
   *  We want to keep different roles but discard subroles. For example,
attribute : /cms/uscms/Role=cmssoft/Capability=NULL
attribute : /cms/uscms/Role=NULL/Capability=NULL
attribute : /cms/Role=NULL/Capability=NULL
attribute : /cms/uscms/Role=cmsprod/Capability=NULL

   should yield the roles

   /cms/uscms/Role=cmssoft/Capability=NULL
   /cms/uscms/Role=cmsprod/Capability=NULL

   * @param validator
   * @return
   * @throws GSSException
   */
    public static Collection <String> getFQANs(VOMSValidator validator) throws GSSException {
        LinkedHashSet <String> fqans = new LinkedHashSet <String> ();
        validator.parse();
        List listOfAttributes = validator.getVOMSAttributes();

        boolean usingroles=false;
        Iterator i = listOfAttributes.iterator();
        while (i.hasNext()) {
            VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
            List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
            Iterator j = listOfFqans.iterator();
            while (j.hasNext()) {
                String attr = (String) j.next();
                String attrtmp=attr;
                if(attrtmp.endsWith(capnull))
                attrtmp = attrtmp.substring(0, attrtmp.length() - capnulllen);
                if(attrtmp.endsWith(rolenull))
                attrtmp = attrtmp.substring(0, attrtmp.length() - rolenulllen);
                Iterator k = fqans.iterator();
                boolean issubrole=false;
                while (k.hasNext()) {
                  String fqanattr=(String) k.next();
                  if (fqanattr.startsWith(attrtmp)) {issubrole=true; break;}
                }
                if(!issubrole) fqans.add(attr);
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
    
    public void setLogLevel(String level) {
        if (level==null) {
            esay("setLogLevel called with null argument. Log level not changed.");
            return;
        }
        
        String newlevel = level.toUpperCase();
        if( newlevel.equals("DEBUG") ||
                newlevel.equals("INFO")  ||
                newlevel.equals("WARN")  ||
                newlevel.equals("ERROR")  )
            loglevel = newlevel;
        else
            esay("Log level not set. Allowed values are DEBUG, INFO, WARN, ERROR.");
    }
    
    public void setDelegateToGplazma(boolean boolarg) {
        delegate_to_gplazma = boolarg;
    }
    
    public void setUseSAZ(boolean boolarg) {
        use_saz = boolarg;
    }
    
    public boolean getUseSAZ() {
        return use_saz;
    }
    
} //end of class AuthorizationService
