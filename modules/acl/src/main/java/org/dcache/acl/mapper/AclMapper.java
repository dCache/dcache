package org.dcache.acl.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.util.List;

import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.ACLException;
import org.dcache.acl.Owner;
import org.dcache.acl.Permission;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.RsType;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;

/**
 * The AclMapper has the task to evaluate an ACL taking information on the subject, object and
 * request origin as input and to map it onto two bit masks:
 *
 * @author David Melkumyan, DESY Zeuthen
 */
public class AclMapper {

    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + AclMapper.class.getName());

    private AclMapper() {
    }

    public static Permission getPermission(Subject subject, Origin origin, Owner owner, ACL acl) {
        // if ( logger.isDebugEnabled() ) {
        // logger.debug("Subject: " + subject);
        // logger.debug("Origin: " + origin);
        // logger.debug("Owner: " + owner);
        // logger.debug("ACL: " + acl);
        // }

        Permission permACL = new Permission();
        RsType rsType = null;
        try {
            if ( Subjects.isRoot(subject) ) {
                permACL.setAll();
                if ( logger.isDebugEnabled() ) {
                    logger.debug("ROOT has an access to everything.");
                }
                return permACL;
            }

            if ( acl == null ) {
                return permACL;
            }

            rsType = acl.getRsType();
            int def_msk = 0, allow_msk = 0;
            List<ACE> aces = acl.getList();
            for (ACE ace : aces) {
                int mask;
                Permission permACE = getPermission(subject, origin, owner, ace, rsType);
                if ( permACE != null && (mask = (permACE.getDefMsk() & (~def_msk))) != 0 ) {
                    // mask not empty and contains only "new" bits
                    def_msk |= mask;
                    if ( permACE.getAllowMsk() == 0 ) {
                        allow_msk |= mask;
                    }
                }

                // if ( logger.isDebugEnabled() )
                // logger.debug("Step " + ace.getOrder() + ") " + (new Permission(def_msk,
                // allow_msk).asString(rsType)));
            }

            permACL.setDefMsk(def_msk);
            permACL.setAllowMsk(allow_msk);

        } catch (ACLException e) {
            logger.error(e.getMessage());
        } finally {
            if ( logger.isDebugEnabled() ) {
                logger.debug("Getted Permission: " + (rsType == null ? permACL
                        .toString() : permACL.asString(rsType)));
            }
        }
        return permACL;
    }

    public static Permission[] getPermissions(Subject subject, Origin origin, Owner[] owners, ACL[] acls) {
        int len = acls.length;
        Permission[] perms = new Permission[len];
        for (int index = 0; index < len; index++) {
            perms[index] = getPermission(subject, origin, owners[index], acls[index]);
        }

        return perms;
    }

    private static Permission getPermission(Subject subject, Origin origin, Owner owner, ACE ace, RsType rsType) throws ACLException {
        // if ( logger.isDebugEnabled() ) {
        // logger.debug("Subject: " + subject);
        // logger.debug("Origin: " + origin);
        // logger.debug("Owner: " + owner);
        // logger.debug("ACE: " + ace.toNFSv4String(rsType));
        // logger.debug("rsType: " + rsType);
        // }

        Permission perm = null;
        // match this ace only if either recourse is not a directory or an INHERIT_ONLY_ACE bit is not set in ace.flags
        if ( rsType == RsType.DIR && AceFlags.INHERIT_ONLY_ACE.matches(ace.getFlags()) ) {
            return null;
        }

        // // match this ace only if either recourse is not a directory or an INHERIT_ONLY_ACE bit is not set in ace.flags
        // if ( (rsType == RsType.DIR && AceFlags.INHERIT_ONLY_ACE.matches(ace.getFlags())) || InetAddrMatcherImpl.matches(ace.getAddressMsk(), origin.getAddress()) == false )
        // return null;

        switch (ace.getWho()) {
        case OWNER:
            if ( Subjects.hasUid(subject, owner.getUid()) ) {
                perm = new Permission(ace.getAccessMsk(), ace.getType()
                        .getValue());
            }
            break;

        case OWNER_GROUP:
            if ( Subjects.hasGid(subject, owner.getGid()) ) {
                perm = new Permission(ace.getAccessMsk(), ace.getType()
                        .getValue());
            }
            break;

        case EVERYONE:
            perm = new Permission(ace.getAccessMsk(), ace.getType().getValue());
            break;

        case ANONYMOUS:
            if ( origin != null && origin.getAuthType() == Origin.AuthType.ORIGIN_AUTHTYPE_WEAK ) {
                perm = new Permission(ace.getAccessMsk(), ace.getType().getValue());
            } else if (origin == null) {
                perm = new Permission(ace.getAccessMsk(), ace.getType().getValue());
            }
            break;

        case AUTHENTICATED:
            if ( origin != null && origin.getAuthType() == Origin.AuthType.ORIGIN_AUTHTYPE_STRONG ) {
                perm = new Permission(ace.getAccessMsk(), ace.getType().getValue());
            } else if (origin == null) {
                perm = new Permission(ace.getAccessMsk(), ace.getType().getValue());
            }
            break;

        case USER:
            if ( Subjects.hasUid(subject, ace.getWhoID()) ) {
                perm = new Permission(ace.getAccessMsk(), ace.getType()
                        .getValue());
            }
            break;

        case GROUP:
            if ( Subjects.hasGid(subject, ace.getWhoID()) ) {
                perm = new Permission(ace.getAccessMsk(), ace.getType()
                        .getValue());
            }
            break;

        default:
            throw new ACLException("Get Permission", "Invalid who: " + ace.getWho());
        }

        return perm;
    }

}
