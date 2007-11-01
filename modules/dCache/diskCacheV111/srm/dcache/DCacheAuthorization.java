// $Id: DCacheAuthorization.java,v 1.8.2.8 2007-10-11 23:11:59 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.8.2.7  2007/10/11 02:35:04  timur
// make DCacheAuthorization use log4j instead of System.out
//
// Revision 1.8.2.6  2006/11/30 21:51:55  tdh
// Pass cell reference to AuthorizationService for logging purposes.
//
// Revision 1.8.2.5  2006/10/04 21:22:11  tdh
// Corrected previous catch attempt.
//
// Revision 1.8.2.4  2006/10/04 18:35:43  tdh
// Ignore exception thrown when using java 1.4, so Tomcat won't quit.
//
// Revision 1.8.2.3  2006/09/15 16:10:24  tdh
// Query gPlazma for authorization with DN and role (Subject and FQAN) directly, instead of delegating the context.
//
// Revision 1.8.2.2  2006/08/07 21:30:31  tdh
// Do not throw exception if gPlazma cell failes and direct module calls yet to be tried.
//
// Revision 1.8.2.1  2006/07/26 18:42:00  tdh
// Backport of recent changes to development branch.
//
// Revision 1.9  2006/07/25 16:07:50  tdh
// Make message to gPlazma cell independent of gPlazma domain.
//
// Revision 1.8  2006/07/03 19:56:50  tdh
// Added code to throw and/or catch AuthenticationServiceExceptions from GPLAZMA cell.
//
// Revision 1.7  2006/06/29 20:27:43  tdh
// Changed hard-coded path of gplazma cell to gPlazma@gPlazmaDomain.
//
// Revision 1.6  2006/06/09 15:17:17  tdh
// Added fields to constructor for using gplazma cell for authentfication. Added logic to use gplazma cell.
//
// Revision 1.5  2005/05/20 16:50:39  timur
// adding optional usage of vo authorization module
//
// Revision 1.4  2005/03/01 23:12:09  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.3  2005/01/25 05:17:31  timur
// moving general srm stuff into srm repository
//
// Revision 1.2  2004/08/06 19:35:22  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.4  2004/06/18 22:20:51  timur
// adding sql database storage for requests
//
// Revision 1.1.2.3  2004/06/16 22:14:31  timur
// copy works for mulfile request
//
// Revision 1.1.2.2  2004/06/15 22:15:41  timur
// added cvs logging tags and fermi copyright headers at the top
//
// Revision 1.1.2.1  2004/05/18 21:40:30  timur
// incorporation of the new scheduler into srm, repackaging of all the srm classes
//
// Revision 1.11  2003/10/22 21:00:26  cvs
// adding staff for srm v2.1
//
// Revision 1.10  2003/06/18 01:33:23  cvs
// upgrading Globus Java CoG Kit to version 1.1 Alpha
//
// Revision 1.9  2003/05/09 22:00:05  cvs
// new better implementation of srm copy functionality, that workes with groups of files
//
// Revision 1.8  2003/04/10 22:05:31  cvs
// dcache authorization changes
//
// Revision 1.7  2003/03/28 17:47:47  cvs
// remove credential caching, so that changes in kpwd passwd file take effect immediately
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


/*
 * SRMAuthorization.java
 *
 * Created on October 4, 2002, 12:11 PM
 */

package diskCacheV111.srm.dcache;

import diskCacheV111.util.KAuthFile;
import diskCacheV111.util.UserAuthRecord;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMAuthorizationException;
import java.io.FileReader;
import java.io.BufferedReader;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.List;
import java.util.Iterator;
import java.net.Socket;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;

import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.scheduler.JobCreatorStorage;
import org.dcache.srm.scheduler.JobCreator;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.opensciencegrid.authz.common.LocalId;
import org.gridforum.jgss.ExtendedGSSContext;
import org.globus.gsi.gssapi.GSSConstants;
import org.glite.security.voms.VOMSValidator;
import org.glite.security.voms.VOMSAttribute;
import org.glite.security.voms.BasicVOMSTrustStore;
import org.glite.security.voms.ac.AttributeCertificate;
import org.glite.security.util.FileCertReader;
import diskCacheV111.services.authorization.AuthorizationService;
import diskCacheV111.services.authorization.AuthorizationServiceException;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellAdapter;
import org.apache.log4j.Logger;
import java.util.Map;
import java.util.HashMap;

/**
 *
 * @author  timur
 */
public final class DCacheAuthorization implements SRMAuthorization {
    
    private static  org.apache.log4j.Logger _logAuth = 
             org.apache.log4j.Logger.getLogger(
            "logger.org.dcache.authorization"+
            SRMAuthorization.class.getName());
    // private Hashtable users = new Hashtable ();
    private static DCacheAuthorization srmauthorization;
    private String kAuthFileName;
    private String gplazmaPolicyFilePath; 
    private boolean use_gplazmaAuthzModule=false;
	  private boolean use_gplazmaAuthzCell=false;
    private CellAdapter parentcell=null;
    private static Map UsernameMap = new HashMap();
    private static long cache_lifetime=180*1000L;


    //Only create the trust store once, refresh every 12 hours
    static {
      try {
        FileCertReader dummy = new FileCertReader();
        VOMSValidator.setTrustStore(new BasicVOMSTrustStore("/etc/grid-security/certificates", 12*3600*1000));
      } catch (CertificateException ce) {
          _logAuth.error(" Certificate Exception.", ce);
      } catch (UnsupportedClassVersionError ucve) {
        _logAuth.error(" VOMSValidator cannot find correct version. gPlazma authorization not supported.",ucve);
      }
    }

    /** Creates a new instance of SRMAuthorization */
   
    /** 
    private DCacheAuthorization(String kAuthFileName) {
       this.kAuthFileName = kAuthFileName;
    }

    private DCacheAuthorization(String gplazmaPolicyFilePath, String dummyVariable) {
       this.gplazmaPolicyFilePath = gplazmaPolicyFilePath;
    }
    */	

    //constructor being used
    private DCacheAuthorization(boolean use_gplazmaAuthzCell, boolean use_gplazmaAuthzModule, String kAuthFileName, String gplazmaPolicyFilePath, CellAdapter parentcell) {
       this.use_gplazmaAuthzCell=use_gplazmaAuthzCell;
       this.use_gplazmaAuthzModule=use_gplazmaAuthzModule;
       this.kAuthFileName = kAuthFileName;
       this.gplazmaPolicyFilePath = gplazmaPolicyFilePath;
       this.parentcell = parentcell;
    }
	
    /** Performes authorization checks. Throws
     * <code>SRMAuthorizationException</code> if the authorization fails.
     * Otherwise, the function completes normally.
     *
     * @exception <code>SRMAuthorizationException</code> if the peer is
     *            not authorized to access/use the resource.
     */
    public RequestUser authorize(Long requestCredentialId, 
        String secureId,String name, GSSContext gsscontext)
    throws SRMAuthorizationException {

        UserAuthRecord user_rec = null;
        if( _logAuth.isDebugEnabled() ) {
                _logAuth.debug("authorize " + requestCredentialId + ":"+
                    secureId+":"+
                    name+":" );
        }

        if(cache_lifetime>0) {
            if( _logAuth.isDebugEnabled() ) {
                    _logAuth.debug("getting user mapping");
            }


          TimedUserAuthRecord tUserRec = getUsernameMapping(requestCredentialId);
            if( _logAuth.isDebugEnabled() ) {
                    _logAuth.debug("tUserRec = "+tUserRec);
            }
          
          if( tUserRec!=null && tUserRec.age() < cache_lifetime &&
            tUserRec.sameDesiredUserName(name)) {
              user_rec = tUserRec.getUserAuthRecord();
          }
        }
          
        if(user_rec  == null){
          if (!use_gplazmaAuthzCell && !use_gplazmaAuthzModule) {
            user_rec = authorize(secureId,name);
          }
          else {
            user_rec = authorize(gsscontext,name);
          }

          if(cache_lifetime>0) {
              putUsernameMapping(requestCredentialId, new TimedUserAuthRecord(user_rec, name));
          }
        }


        if(user_rec==null) {
          throw new SRMAuthorizationException("username is null");
        }
        String username = user_rec.Username;
        String root = user_rec.Root;
        int uid = user_rec.UID;
        int gid = user_rec.GID;
        JobCreator creator = JobCreator.getJobCreator(username);
        if(creator != null) {
            if( ! (creator instanceof DCacheUser) ) {
                throw new SRMAuthorizationException(" job creator with name "+username+
                " exists and is not an instance of dcache user!!!");
            }
            DCacheUser user = (DCacheUser) creator;
            if ( username.equals( user.getName()) &&
                 root.equals(user.getRoot()) &&
                 uid == user.getUid() &&
                 gid == user.getGid() ){
                return user;
            }
           _logAuth.error(" Warning: user parameters for user "+ user+ " have changed ");
        }
        DCacheUser user = new DCacheUser(username,root,uid,gid);
        user.saveCreator();
        return user;
        
    }
    
    private UserAuthRecord authorize(String secureId,String name)
    throws SRMAuthorizationException {
        
        if(name==null) {
            name = getUserNameByGlobusId(secureId);
        }
        UserAuthRecord userRecord = getUserRecord(name,secureId);
        return userRecord;
    }
	
    private UserAuthRecord authorize(GSSContext serviceContext,String name)
    throws SRMAuthorizationException {
        AuthorizationService authServ = null;
        UserAuthRecord authRecord = null;
        try {
             authServ = new AuthorizationService(gplazmaPolicyFilePath, parentcell);
        }
        catch(Exception e) {
             e.printStackTrace();
             throw new SRMAuthorizationException(e.toString());
        }

        String gssIdentity = null;
        String requestedServiceName;
        String fqanValue = null;

        ExtendedGSSContext extendedcontext;
        if (serviceContext instanceof ExtendedGSSContext) {
			    extendedcontext = (ExtendedGSSContext) serviceContext;
		    } else {
			    _logAuth.error("Received context not instance of ExtendedGSSContext, Plugin exiting...");
			    return null;
		    }

        X509Certificate[] chain;
        try {
          chain = (X509Certificate[]) extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);
        } catch (GSSException gsse) {
          _logAuth.error("Caught GSSException in getting cerficate chain " ,gsse);
			    return null;
        }

        try {
          gssIdentity = extendedcontext.getSrcName().toString();
        } catch (GSSException gsse) {
         _logAuth.error("Caught GSSException in getting DN " ,gsse);
			    return null;
        }

        try {
          requestedServiceName = extendedcontext.getTargName().toString();
        } catch (GSSException gsse) {
          _logAuth.error("Caught GSSException in getting service name " ,gsse);
			    return null;
        }

        VOMSValidator validator = new VOMSValidator(chain);
        validator.parse();
        List listOfAttributes = validator.getVOMSAttributes();

            // Getting first FQAN only
        Iterator i = listOfAttributes.iterator();
        if (i.hasNext()) {
          VOMSAttribute vomsAttribute = (VOMSAttribute) i.next();
          AttributeCertificate ac = vomsAttribute.getAC();
          String issuer = ac.getIssuer().toString();
          if( _logAuth.isDebugEnabled() ) {
                _logAuth.debug("VOMS Server is '" + issuer + "'");
          }
          List listOfFqans = vomsAttribute.getFullyQualifiedAttributes();
          Iterator j = listOfFqans.iterator();
          if (j.hasNext()) {
            fqanValue = (String) j.next();
            if( _logAuth.isDebugEnabled() ) {
                _logAuth.debug("FQAN is '" + fqanValue + "'");
            }
          } else {
             if( _logAuth.isDebugEnabled() ) {
                _logAuth.debug("No FQAN found");
             }
          }
        } else {
             if( _logAuth.isDebugEnabled() ) {
                _logAuth.debug("No attribute found");
             }
        }


        if (use_gplazmaAuthzCell) {
          try {
            //authRecord = (UserAuthRecord) authServ.authenticate(serviceContext, new CellPath("gPlazma"), parentcell);
            authRecord = (UserAuthRecord) authServ.authenticate(gssIdentity, fqanValue, new CellPath("gPlazma"), parentcell);
          } catch (AuthorizationServiceException ase) {
            if(!use_gplazmaAuthzModule) {
              throw new SRMAuthorizationException(ase.toString());
            }
            ase.printStackTrace();
            _logAuth.error("Authorization through gPlazma cell failed " + ase);
          }
        }

        if (authRecord==null && use_gplazmaAuthzModule) {
          try {
            //authRecord = authServ.authorize(serviceContext, name, null, null);
            authRecord = (UserAuthRecord) authServ.authorize(gssIdentity, fqanValue, null, null, null);
          }
          catch(Exception ee) {
             ee.printStackTrace();
             throw new SRMAuthorizationException(ee.toString());
          }
          if(authRecord == null) {
             throw new SRMAuthorizationException("srm authorization failed, permission denied");
          }
        }
        return authRecord;
    }
    
    private String getUserNameByGlobusId(String globusId)
    throws SRMAuthorizationException {
        KAuthFile authf = null;
        try {
            authf = new KAuthFile(kAuthFileName);
        }
        catch(java.io.IOException ioe) {
            ioe.printStackTrace();
            throw new SRMAuthorizationException(ioe.toString());
        }
        String username =authf.getIdMapping(globusId);
        if(username == null) {
            throw new SRMAuthorizationException(
            " can not determine username from GlobusId="+globusId);
        }
        return username;
    }
    
    private UserAuthRecord getUserRecord(String username,String globusId)
    throws SRMAuthorizationException {
        KAuthFile authf = null;
        try {
            authf = new KAuthFile(kAuthFileName);
        }
        catch(java.io.IOException ioe) {
            ioe.printStackTrace();
            throw new SRMAuthorizationException(ioe.toString());
        }
        UserAuthRecord userRecord = authf.getUserRecord(username);
        
        if(userRecord == null) {
            throw new SRMAuthorizationException(
            "user "+username+" is not found");
        }
        
        if(!userRecord.hasSecureIdentity(globusId)) {
            throw new SRMAuthorizationException(
            "srm authorization failed for user "+username+"with GlobusId="+globusId);
        }
        //users.put(username,userRecord);
        return userRecord;
    }
   
    /** 
    public static SRMAuthorization getDCacheAuthorization(String kAuthFileName) {
        if(srmauthorization == null) {
            srmauthorization = new DCacheAuthorization(kAuthFileName);
        }
        
        if(!srmauthorization.kAuthFileName.equals(kAuthFileName)) {
            srmauthorization = new DCacheAuthorization(kAuthFileName);
        }
        return srmauthorization;
    }
	
    public static SRMAuthorization getDCacheAuthorization(String gplazmaPolicyFilePath, String dummyVariable) {
        if(srmauthorization == null) {
            srmauthorization = new DCacheAuthorization(gplazmaPolicyFilePath, "dummy");
        }
        if(!srmauthorization.gplazmaPolicyFilePath.equals(gplazmaPolicyFilePath)) {
            srmauthorization = new DCacheAuthorization(gplazmaPolicyFilePath, "dummy");
        }
        return srmauthorization;
    }	
    */
	
    public static SRMAuthorization getDCacheAuthorization(boolean use_gplazmaAuthzCell, boolean use_gplazmaAuthzModule, String kAuthFileName, String gplazmaPolicyFilePath, CellAdapter parentcell) {
        if(srmauthorization == null) {
            srmauthorization = new DCacheAuthorization(use_gplazmaAuthzCell, use_gplazmaAuthzModule, kAuthFileName, gplazmaPolicyFilePath, parentcell);
        }
        if(!use_gplazmaAuthzModule) {
            if(!srmauthorization.kAuthFileName.equals(kAuthFileName)) {
                srmauthorization = new DCacheAuthorization(use_gplazmaAuthzCell, use_gplazmaAuthzModule, kAuthFileName, gplazmaPolicyFilePath, parentcell);
            } 
        }
        else {
            if(!srmauthorization.gplazmaPolicyFilePath.equals(gplazmaPolicyFilePath)) {
                srmauthorization = new DCacheAuthorization(use_gplazmaAuthzCell, use_gplazmaAuthzModule, kAuthFileName, gplazmaPolicyFilePath, parentcell);
            }
        }
        return srmauthorization;
    }

  private synchronized void putUsernameMapping(Object key, TimedUserAuthRecord tUserRec) {
    UsernameMap.put(key, tUserRec);
  }

  private synchronized TimedUserAuthRecord getUsernameMapping(Object key) {
    return (TimedUserAuthRecord) UsernameMap.get(key);
  }

  private class TimedUserAuthRecord extends UserAuthRecord {
    UserAuthRecord user_rec;
    long timestamp;
    String desiredUserName=null;

    TimedUserAuthRecord(UserAuthRecord user_rec) {
      this.user_rec=user_rec;
      this.timestamp=System.currentTimeMillis();
    }

    TimedUserAuthRecord(UserAuthRecord user_rec, String desiredUserName) {
      this(user_rec);
      this.desiredUserName=desiredUserName;
    }

    private UserAuthRecord getUserAuthRecord() {
      return user_rec;
    }

    private long age() {
      return System.currentTimeMillis() - timestamp;
    }

    private boolean sameDesiredUserName(String requestDesiredUserName) {
      if(desiredUserName==null && requestDesiredUserName==null) return true;
      return (desiredUserName.equals(requestDesiredUserName));
    }
  }
}
