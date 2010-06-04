// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.13  2007/03/27 19:20:28  tdh
// Merge of support for multiple attributes from 1.7.1.
//
// Revision 1.12  2006/12/15 16:08:44  tdh
// Added code to make delegation from cell to gPlazma optional, through the batch file parameter "delegate-to-gplazma". Default is to not delegate.
//
// Revision 1.11  2006/08/07 21:31:29  tdh
// Do not throw exception if gPlazma cell failes and direct module calls yet to be tried.
//
// Revision 1.10  2006/08/02 22:09:54  timur
// more work for srm space reservation, included voGroup and voRole support
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

import org.dcache.auth.*;
import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.persistence.AuthRecordPersistenceManager;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;
import diskCacheV111.util.CacheException;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.gridforum.jgss.ExtendedGSSContext;

import java.security.cert.X509Certificate;
import javax.security.auth.Subject;

import java.util.Map;
import java.util.HashMap;
import java.util.Random;
import java.util.List;
import java.util.LinkedList;

/**
 *
 * @author  timur
 */
public final class DCacheAuthorization implements SRMAuthorization {

      static Random random;
      static {
        random = new Random();
      }

    private static  org.slf4j.Logger _logAuth =
             org.slf4j.LoggerFactory.getLogger(DCacheAuthorization.class);
    private static Map UsernameMap = new HashMap();
    private long cache_lifetime=0L;
    private AuthRecordPersistenceManager authRecordPersistenceManager;
    private final LoginStrategy loginStrategy;

    public DCacheAuthorization(LoginStrategy loginStrategy,
            AuthRecordPersistenceManager authRecordPersistenceManager)
    {
       this.loginStrategy = loginStrategy;
       this.authRecordPersistenceManager = authRecordPersistenceManager;
    }

    /** Performs authorization checks. Throws
     * <code>SRMAuthorizationException</code> if the authorization fails.
     * Otherwise, the function completes normally.
     *
     * @param requestCredentialId
     * @param secureId
     * @param name
     * @param gsscontext
     * @exception <code>SRMAuthorizationException</code> if the peer is
     *            not authorized to access/use the resource.
     */
    public SRMUser authorize(Long requestCredentialId,
        String secureId,String name, GSSContext gsscontext)
    throws SRMAuthorizationException {

        AuthorizationRecord user_rec = null;
        if( _logAuth.isDebugEnabled() ) {
                _logAuth.debug("authorize " + requestCredentialId + ":"+
                    secureId+":"+
                    name+":" );
        }

        if(cache_lifetime>0) {
            if( _logAuth.isDebugEnabled() ) {
                    _logAuth.debug("getting user mapping");
            }


          TimedAuthorizationRecord tUserRec = getUsernameMapping(requestCredentialId);
            if( _logAuth.isDebugEnabled() ) {
              if(tUserRec != null ) {
                  AuthorizationRecord userRec = tUserRec.user_rec;
                  if(userRec != null ) {
                     _logAuth.debug("cached tUserRec = "+userRec);
                  } else {
                      _logAuth.debug("cached tUserRec = null ");
                  }
              } else {
                  _logAuth.debug("cached tUserRec = null ");
              }
          }

          if( tUserRec!=null && tUserRec.age() < cache_lifetime &&
            tUserRec.sameDesiredUserName(name)) {
              user_rec = tUserRec.getAuthorizationRecord();
          }
        }

        if(user_rec  == null){
            user_rec = authorize(secureId, gsscontext, name);
            user_rec = authRecordPersistenceManager.persist(user_rec);

            if(cache_lifetime>0) {
                putUsernameMapping(requestCredentialId, new TimedAuthorizationRecord(user_rec, name));
            }
        }


        if(user_rec==null) {
          throw new SRMAuthorizationException("username is null");
        }
        return user_rec;

    }

    private AuthorizationRecord authorize(String secureId,
                                          GSSContext serviceContext,
                                          String name)
        throws SRMAuthorizationException
    {
        try {
            if (!(serviceContext instanceof ExtendedGSSContext)) {
                throw new RuntimeException("GSSContext not instance of ExtendedGSSContext");
            }

            ExtendedGSSContext extendedcontext =
                (ExtendedGSSContext) serviceContext;
            X509Certificate[] chain =
                (X509Certificate[]) extendedcontext.inquireByOid(GSSConstants.X509_CERT_CHAIN);

            Subject subject = new Subject();
            if (name != null && !name.isEmpty()) {
                subject.getPrincipals().add(new UserNamePrincipal(name));
            }
            if (secureId != null && !secureId.isEmpty()) {
                /* Technically, the secureId could be a
                 * KerberosPrincipal too. At this point we cannot tell
                 * the difference anymore.
                 */
                subject.getPrincipals().add(new GlobusPrincipal(secureId));
            }
            subject.getPublicCredentials().add(chain);

            LoginReply login = loginStrategy.login(subject);
            AuthorizationRecord authRecord =
                Subjects.getAuthorizationRecord(login.getSubject());

            for (LoginAttribute attribute: login.getLoginAttributes()) {
                if (attribute instanceof RootDirectory) {
                    authRecord.setRoot(((RootDirectory) attribute).getRoot());
                } else if (attribute instanceof HomeDirectory) {
                    authRecord.setHome(((HomeDirectory) attribute).getHome());
                } else if (attribute instanceof ReadOnly) {
                    authRecord.setReadOnly(((ReadOnly) attribute).isReadOnly());
                }
            }
            return authRecord;
        } catch (GSSException e) {
            throw new SRMAuthorizationException(e.getMessage(), e);
        } catch (CacheException e) {
            throw new SRMAuthorizationException(e.getMessage(), e);
        }
    }

  public void setCacheLifetime(String lifetime_str) {
    if(lifetime_str==null || lifetime_str.length()==0) return;
    try {
      setCacheLifetime(Long.decode(lifetime_str).longValue()*1000);
    } catch (NumberFormatException nfe) {
      _logAuth.error("Could not format cache lifetime=" + lifetime_str + " as long integer.");
    }
  }

  public void setCacheLifetime(long lifetime) {
    cache_lifetime = lifetime;
  }

  private synchronized void putUsernameMapping(Object key, TimedAuthorizationRecord tUserRec) {
    UsernameMap.put(key, tUserRec);
}

  private synchronized TimedAuthorizationRecord getUsernameMapping(Object key) {
    return (TimedAuthorizationRecord) UsernameMap.get(key);
  }

  private class TimedAuthorizationRecord  {
    AuthorizationRecord user_rec;
    long timestamp;
    String desiredUserName=null;

    TimedAuthorizationRecord(AuthorizationRecord user_rec) {
      this.user_rec=user_rec;
      this.timestamp=System.currentTimeMillis();
    }

    TimedAuthorizationRecord(AuthorizationRecord user_rec, String desiredUserName) {
      this(user_rec);
      this.desiredUserName=desiredUserName;
    }

    private AuthorizationRecord getAuthorizationRecord() {
      return user_rec;
    }

    private long age() {
      return System.currentTimeMillis() - timestamp;
    }

    private boolean sameDesiredUserName(String requestDesiredUserName) {
      if(desiredUserName==null && requestDesiredUserName==null) return true;
      if(desiredUserName == null) return false;
      return (desiredUserName.equals(requestDesiredUserName));
    }
  }


  public static AuthorizationRecord convertLegacyToNewAuthRec(
      UserAuthRecord legacyAuthRec) {
        AuthorizationRecord authRec = new AuthorizationRecord();

        authRec.setId(random.nextLong());
        authRec.setIdentity(legacyAuthRec.Username);
        authRec.setName(legacyAuthRec.DN);
        authRec.setUid(legacyAuthRec.UID);
        authRec.setPriority(legacyAuthRec.priority);
        authRec.setHome(legacyAuthRec.Home);
        authRec.setRoot(legacyAuthRec.Root);
        authRec.setReadOnly(legacyAuthRec.ReadOnly);

        List grplistcoll = new LinkedList<GroupList>();
        GroupList grplist = new GroupList();
        List grpcoll = new LinkedList<Group>();
        int[] GIDs = ((UserAuthRecord) legacyAuthRec).GIDs;
        for (int gid : GIDs) {
            Group grp = new Group();
            grp.setGroupList(grplist);
            grp.setGid(gid);
            grpcoll.add(grp);
        }
        grplist.setGroups(grpcoll);
        grplist.setAttribute(legacyAuthRec.getFqan().toString());
        grplist.setAuthRecord(authRec);
        grplistcoll.add(grplist);
        authRec.setGroupLists(grplistcoll);

        return authRec;

  }
}
