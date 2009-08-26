// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.4  2007/08/03 15:47:57  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes, not comparing objects using == or !=,  etc, per findbug recommendations
//
// Revision 1.3  2007/01/06 00:23:54  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
// Revision 1.2.12.1  2007/01/04 02:58:54  timur
// changes to database layer to improve performance and reduce number of updates
//
// Revision 1.2  2005/03/24 19:16:18  timur
// made built in client always delegate credentials, which is required by LBL's DRM
//
// Revision 1.1  2005/03/13 21:56:28  timur
// more changes to restore compatibility
//
// Revision 1.3  2005/03/11 21:16:26  timur
// making srm compatible with cern tools again
//
// Revision 1.2  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.26  2004/12/14 19:37:56  timur
// fixed advisory delete permissions and client issues
//
// Revision 1.25  2004/10/30 04:19:07  timur
// Fixed a problem related to the restoration of the job from database
//
// Revision 1.24  2004/10/28 02:41:32  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.23  2004/09/07 04:14:28  timur
// fixed a memory leak
//
// Revision 1.22  2004/08/06 19:35:26  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.21.2.11  2004/08/02 22:06:13  timur
// added script for building standalone file system srm
//
// Revision 1.21.2.10  2004/07/02 20:10:25  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.21.2.9  2004/06/23 21:56:02  timur
// Get Requests are now stored in database, Request Credentials are now stored in database too
//
// Revision 1.21.2.8  2004/06/22 01:38:07  timur
// working on the database part, created persistent storage for getFileRequests, for the next requestId
//
// Revision 1.21.2.7  2004/06/18 22:20:53  timur
// adding sql database storage for requests
//
// Revision 1.21.2.6  2004/06/16 22:14:32  timur
// copy works for mulfile request
//
// Revision 1.21.2.5  2004/06/16 19:44:34  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//

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

// generated by GLUE/wsdl2java on Mon Jun 17 15:27:13 CDT 2002
package diskCacheV111.srm.server;

import diskCacheV111.srm.ISRM;
import org.dcache.srm.SRM;
import diskCacheV111.srm.RequestStatus;
import diskCacheV111.srm.FileMetaData;
import org.dcache.srm.security.SslGsiSocketFactory;


import java.io.IOException;
import java.net.InetAddress;

import electric.registry.Registry;
import electric.server.http.HTTP;
import electric.net.socket.SocketFactories;
import electric.util.Context;
import electric.net.http.HTTPContext;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.RequestCredentialStorage;
import org.dcache.srm.util.Configuration;
import diskCacheV111.srm.IInformationProvider;
import diskCacheV111.srm.StorageElementInfo;
import org.dcache.srm.SRMException;
import java.sql.SQLException;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSContext;

public class SRMServerV1 implements ISRM, IInformationProvider {
    private SRM srm;
    private SRMAuthorization authorization;
    private int port;
    private static HTTPContext srmcontext = null;
    private InetAddress localhost;
    private static String localhostname;
    private static SslGsiSocketFactory socket_factory;
    private Configuration configuration;
    private RequestCredentialStorage credential_storage;

    public SRMServerV1(SRM srm,int port,
    SRMAuthorization authorization,
    Configuration configuration,
    RequestCredentialStorage credential_storage)
    throws IOException, SQLException, electric.xml.ParseException{
        if(srm == null) {
            throw new java.lang.NullPointerException("srm is null!!!");
        }
        
        this.srm = srm;
        this.port = port;
        this.authorization = authorization;
        this.configuration = configuration;
        this.credential_storage = credential_storage;
        
        System.setProperty("electric.soap.optimize","no");
        System.setProperty("electric.wsdl.targetNamespace","http://srm.1.0.ns");
        if(configuration.getGlue_mapfile() != null) {
            electric.xml.io.Mappings.readMappings (configuration.getGlue_mapfile());
        }
        String glueProtocol = "http";
        if(configuration.isGsissl() && authorization != null) {
            socket_factory = new SslGsiSocketFactory(configuration);
            socket_factory.setDoDelegation(true);
            socket_factory.setFullDelegation(true);
            
            SocketFactories.addFactory("ssl", socket_factory);
            glueProtocol = "https";
        }
        if(srmcontext == null) {
            localhost = InetAddress.getLocalHost();
            localhostname = localhost.getHostName();
            
            srmcontext = HTTP.startup( glueProtocol+"://"+
            localhostname+":"+port+"/srm" );
            
        }
        // publish an instance of ManagerImpl
        try {
            Registry.publish( "managerv1", this,ISRM.class );
            Registry.publish( "infoProvider1_0", this,IInformationProvider.class );
        }
        catch(electric.registry.RegistryException re) {
            throw new IOException(" can not publish the SRM server: "+re);
        }
    }
    
    public void say(String s) {
        configuration.getStorage().log("SRMServerV1: "+s);
    }
    
    public void esay(String s) {
        configuration.getStorage().elog("SRMServerV1: "+s);
    }
    
    public void esay(Exception e) {
        configuration.getStorage().elog(e);
    }
    
    public static SslGsiSocketFactory getSocketFactory() {
        return socket_factory;
    }
    public InetAddress getHost() {
        return localhost;
    }
    
    public int getPort() {
        return port;
    }
    
    
    public RequestStatus put( String[] sources,
    String[] dests,
    long[] sizes,
    boolean[] wantPerm,
    String[] protocols ) {
        StringBuffer sb = new StringBuffer();
        sb.append("put sources = ");
        for(int i = 0 ; i < sources.length; ++i) {
            sb.append(sources[i]).append(',');
        }
        sb.append("    dests = ");
        for(int i = 0 ; i < dests.length; ++i) {
            sb.append(dests[i]).append(',');
        }
        sb.append("    sizes = ");
        for(int i = 0 ; i < sizes.length; ++i) {
            sb.append(sizes[i]).append(',');
        }
        sb.append("    wantPerm = ");
        for(int i = 0 ; i < wantPerm.length; ++i) {
            sb.append(wantPerm[i]).append(',');
        }
        sb.append("    protocols = ");
        for(int i = 0 ; i < protocols.length; ++i) {
            sb.append(protocols[i]).append(',');
        }
        say(sb.toString());
        UserCredential userCredential;
        SRMUser user ;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return srm.put(user,credential,sources,dests,sizes,wantPerm,protocols,
        userCredential.clientHost);
    }
    
    public RequestStatus get( String[] surls,
    String[] protocols ) {
        try {
            if(surls == null) {
                throw new NullPointerException("surls is null!!!");
            }
            if(protocols == null) {
                throw new NullPointerException("protocols is null!!!");
            }
                
            StringBuffer sb = new StringBuffer();
            sb.append("get surls = ");
            for(int i = 0 ; i < surls.length; ++i) {
                sb.append(surls[i]).append(',');
            }
            sb.append("     protocols = ");
            for(int i = 0 ; i < protocols.length; ++i) {
                sb.append(protocols[i]).append(',');
            }
            say(sb.toString());
            UserCredential userCredential;
            SRMUser user ;
            try {
                userCredential = getUserCredentials();
                user = getRequestUser(userCredential, null);
            }
            catch(SRMAuthorizationException srmae) {
                throw new RuntimeException(srmae);
            }
            RequestCredential credential = getRequestCredentilal(userCredential,null);
            RequestStatus rs = srm.get(user,credential,surls,protocols,
                userCredential.clientHost);
            say("returning rs = "+rs);
            return rs;
        }
        catch(Throwable t) {
            t.printStackTrace();
            return null;
        }
    }
    public RequestStatus copy( String[] srcSURLS,
    String[] destSURLS,
    boolean[] wantPerm ) {
        UserCredential userCredential ;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return  srm.copy(user,credential,srcSURLS,destSURLS,wantPerm,
            userCredential.clientHost);
    }
    
    public RequestStatus getRequestStatus( int requestId ) {
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        RequestStatus rs = srm.getRequestStatus(user,credential,requestId);
        //say("returning rs = "+rs);
        return rs;
    }
    
    public boolean ping() {
        say("ping");
        return true;
    }
    
    public RequestStatus mkPermanent( String[] SURLS ) {
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return  null;
    }
    
    public RequestStatus pin( String[] TURLS ) {
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return srm.pin(user,credential,TURLS);
    }
    
    public RequestStatus unPin( String[] TURLS ,int requestID) {
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return srm.unPin(user,credential,TURLS, requestID);
    }
    
    public RequestStatus getEstGetTime( String[] SURLS ,String[] protocols) {
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return  null;
    }
    
    public RequestStatus getEstPutTime( String[] src_names,
    String[] dest_names,
    long[] sizes,
    boolean[] wantPermanent,
    String[] protocols) {
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return  null;
    }
    
    public FileMetaData[] getFileMetaData( String[] SURLS ) {
        
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return srm.getFileMetaData(user,credential,SURLS);
    }
    
    public RequestStatus setFileStatus( int requestId,
    int fileId,
    String state ) {
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return srm.setFileStatus(user,credential,requestId,fileId,state);
    }
    
    public void advisoryDelete( String[] SURLS) {
        for(int i=0; i <SURLS.length;++i) {
            say("SRMServerV1.advisoryDelete: SURL #"+i+" = "+SURLS[i] );
        }
        
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        srm.advisoryDelete(user,credential,SURLS);
    }
    
    public String[] getProtocols() {
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential = getRequestCredentilal(userCredential,null);
        return srm.getProtocols(user,credential);
    }
    
    private class UserCredential {
        String secureId;
        GSSCredential credential;
        GSSContext context;
        String clientHost;
    }
    
    private UserCredential getUserCredentials() throws SRMAuthorizationException {
        Context threadContext = Context.thread();
        if(threadContext == null) {
            throw new SRMAuthorizationException("threadContext == null");
        }
        Object o = threadContext.getProperty("httpRequest");
        if(o == null || !(o instanceof electric.net.http.HTTPRequest)) {
            throw new SRMAuthorizationException("httpRequest contex == null");
        }
        electric.net.http.HTTPRequest httprequest = (electric.net.http.HTTPRequest)o;
        
        electric.net.channel.IChannel channel = httprequest.getChannel();
        if(channel == null) {
            throw new SRMAuthorizationException("electric.net.channel.IChannel == null");
        }
        electric.util.XURL loc = channel.getLocalXURL();
        electric.util.XURL rmt = channel.getRemoteXURL();
        if(loc == null || rmt == null) {
            throw new SRMAuthorizationException("LocalXURL == null OR RemoteXURL== null");
        }
        
        final String key = loc.getHost()+loc.getPort()+rmt.getHost()+rmt.getPort();
        final String idkey = key+SslGsiSocketFactory.ID_POSTFIX;
        final String delegatedkey = key+SslGsiSocketFactory.DELEG_CRED_POSTFIX;
        final String gsscontextkey =key+SslGsiSocketFactory.GSSCONTEXT;
        
        Context appContext = Context.application();
        if(appContext == null) {
            throw new SRMAuthorizationException("appContext == null");
        }
        
        o = appContext.getProperty(idkey);
        if(o == null || !(o instanceof String)) {
            esay(idkey +" property is null or isn't string");
            throw new SRMAuthorizationException(idkey +" property is null or isn't string");
        }
        say(" userId is "+o);
        UserCredential userCredential = new UserCredential();
        userCredential.secureId = (String) o;
        
        
        o = appContext.getProperty(gsscontextkey);
        if(o == null || !(o instanceof GSSContext)) {
            esay(gsscontextkey +" property is null or isn't GSSContext");
            throw new SRMAuthorizationException(gsscontextkey +
                " property is null or isn't GSSContext");
        }
        userCredential.context = (GSSContext) o;
        
        o = appContext.getProperty(delegatedkey);
        if(o != null &&  o instanceof GSSCredential) {
            userCredential.credential = (GSSCredential)o;
            try {
                say(" user credential is "+userCredential.credential.getName());
            }catch(Exception e) {
                esay(e);
            }
            
        }
        
        userCredential.clientHost = rmt.getHostAddress().getHostName();
        
        return userCredential;
    }
    
    public SRMUser getRequestUser(UserCredential userCredential,String role)
    throws SRMAuthorizationException {
        
        SRMUser requestUser = authorization.authorize(null, userCredential.secureId, role,userCredential.context);
        
        return requestUser;
    }
    
    public  RequestCredential getRequestCredentilal(UserCredential userCredential,String role)  {
        try {
            say("calling RequestCredential.getRequestCredential("+userCredential.secureId+","+role+")");
            RequestCredential rc = RequestCredential.getRequestCredential(userCredential.secureId,role);
            say("received RequestCredential = "+rc);
            if(rc != null) {
                rc.keepBestDelegatedCredential(userCredential.credential);
            }
            else {
                say("creating new RequestCredential");
                rc = new RequestCredential(userCredential.secureId, role,
                userCredential.credential,
                credential_storage);
            }
            say("returning RequestCredential = "+rc);
            rc.saveCredential();
            return rc;
        }
        catch(Exception e) {
            esay(e);
            esay("to implement interface, cannot throw Exception, so I throw runtime exception");
            throw new RuntimeException(e.toString());
        }
    }
    
    
    public diskCacheV111.srm.StorageElementInfo getStorageElementInfo() {
        
        UserCredential userCredential;
        SRMUser user = null;
        try {
            userCredential = getUserCredentials();
            user = getRequestUser(userCredential, null);
        }
        catch(SRMAuthorizationException srmae) {
            throw new RuntimeException(srmae);
        }
        RequestCredential credential =getRequestCredentilal(userCredential,null);
        try {
            return srm.getStorageElementInfo(user,credential);
        }
        catch(SRMException srme) {
            //return zeros
            return new StorageElementInfo();
        }
    }
    
}
