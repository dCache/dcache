// $Id$


/*
 * UnixfsAuthorization.java
 *
 * modified for DiskSE: AIK July 12, 2004
 */

/**
 *
 * @author  timur
 */

package org.dcache.srm.unixfs;

import org.ietf.jgss.GSSContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMUser;

public final class UnixfsAuthorization implements SRMAuthorization {

    private static UnixfsAuthorization srmauthorization;
    private static Logger logger =
        LoggerFactory.getLogger(UnixfsAuthorization.class);

    private String kAuthFileName;

    /** Creates a new instance of SRMAuthorization */
    private UnixfsAuthorization(String kAuthFileName) {
        this.kAuthFileName = kAuthFileName;
     }

    /** Performs authorization checks. Throws
     * <code>SRMAuthorizationException</code> if the authorization fails.
     * Otherwise, the function completes normally.
     *
     * @param chain(???) the peer certificate chain.
     * @param socket the socket connected to the peer.
     * @exception <code>SRMAuthorizationException</code> if the peer is
     *            not authorized to access/use the resource.
     */
    @Override
    public SRMUser authorize(Long requestCredentialId, String secureId,String name, GSSContext context,
                             String remoteIP)
    throws SRMAuthorizationException {
  /** @todo -- javadoc, there is no such arg 'chain' */
        UserAuthRecord user_rec = authorize(secureId,name);
        logger.debug("Received authorization request from remote IP {}", remoteIP);
        String username = user_rec.Username;
        String root = user_rec.Root;
        int uid = user_rec.UID;
        int gid = user_rec.GID;

        UnixfsUser user = new UnixfsUser(username,root,uid,gid);
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

    private String getUserNameByGlobusId(String globusId)
    throws SRMAuthorizationException {
        KAuthFile authf;
        try {
            authf = new KAuthFile(kAuthFileName);
        }
        catch(IOException ioe) {
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
        KAuthFile authf;
        try {
            authf = new KAuthFile(kAuthFileName);
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
            throw new SRMAuthorizationException(ioe.toString());
        }
        UserAuthRecord userRecord = authf.getUserRecord(username);

        if(userRecord == null) {
            throw new SRMAuthorizationException(
            "user "+username+" not found");
        }

        if(!userRecord.hasSecureIdentity(globusId)) {
            throw new SRMAuthorizationException(
            "srm authorization failed for user "+username+" with GlobusId="+globusId);
        }
        //users.put(username,userRecord);
        return userRecord;
    }

    /** */
    public static SRMAuthorization getAuthorization(String kAuthFileName) {
        if(srmauthorization == null) {
          srmauthorization = new UnixfsAuthorization(kAuthFileName);
        }

        if(!srmauthorization.kAuthFileName.equals(kAuthFileName)) {
            srmauthorization = new UnixfsAuthorization(kAuthFileName);
        }

        return srmauthorization;

    }
}

//
// $Log: not supported by cvs2svn $
// Revision 1.2  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.2  2004/08/06 19:35:26  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.1  2004/07/19 20:44:09  aik
// UnixfsAuthorization.java,
// UnixfsUser.java  -- inital revision
//
// unixfs.java -- Pre-Initial release to cvs
//   so timur can work on main().
// Several methods are not implemeted, as well as some features
//  (e.g. "user" is ignored).
//
// templateStorageElement.java -- template file for SE,
//  all methods throw exception or return error 'not implemented'
//
//
// Initial Version: refer to
// = Log: DCacheAuthorization.java,v =
// == Revision 1.1.2.4  2004/06/18 22:20:51  timur =
//
