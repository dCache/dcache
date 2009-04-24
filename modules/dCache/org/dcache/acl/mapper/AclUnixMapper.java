package org.dcache.acl.mapper;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.Permission;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.RsType;
import org.dcache.acl.enums.Who;
import org.dcache.acl.unix.ACLUnix;
import org.dcache.acl.unix.AMUnix;

/**
 * Mapping Between NFSv4 and Unix ACLs.
 *
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class AclUnixMapper {

    private static final Logger logger = Logger.getLogger("logger.org.dcache.authorization." + AclUnixMapper.class.getName());

    private static AclUnixMapper _SINGLETON;
    static {
        _SINGLETON = new AclUnixMapper();
    }

    private AclUnixMapper() {
        super();
    }

    public static AclUnixMapper instance() {
        return _SINGLETON;
    }

    public static ACLUnix map(ACL acl) {

        final RsType rsType = acl.getRsType();
        final List<ACE> aces = acl.getList();

        List<ACE> accessACEs = aces;
        List<ACE> defaultACEs;

        int[] defaultMasks = null;
        if ( rsType == RsType.DIR ) {
            splitACEs(aces, accessACEs = new ArrayList<ACE>(), defaultACEs = new ArrayList<ACE>());
            defaultMasks = getMasks(defaultACEs);
        }

        int[] accessMasks = getMasks(accessACEs);

        ACLUnix aclUnix = new ACLUnix(acl.getRsId(), RsType.DIR.equals(rsType), accessMasks, defaultMasks);
        return aclUnix;
    }

    private static int[] getMasks(List<ACE> aces) {
        int[] masks = new int[ACLUnix.NUM_ACES];

        Permission permOwner = new Permission();
        Permission permGroup = new Permission();
        Permission permEveryone = new Permission();

        for (ACE ace : aces) {
            Who who = ace.getWho();
            boolean allowed = (AceType.ACCESS_ALLOWED_ACE_TYPE == ace.getType());

            switch (who) {
            case OWNER:
                applyMask(permOwner, ace.getAccessMsk(), allowed);
                break;

            case OWNER_GROUP:
                applyMask(permGroup, ace.getAccessMsk(), allowed);
                break;

            case EVERYONE:
                int msk = applyMask(permEveryone, ace.getAccessMsk(), allowed);
                if ( msk != 0 ) {
                    applyMask(permOwner, msk, allowed);
                    applyMask(permGroup, msk, allowed);
                }
                break;

            default:
                logger.info("Unsupported who: " + who);
            }
        }

        masks[ACLUnix.OWNER_INDEX] = perm2accessMask(permOwner);
        masks[ACLUnix.GROUP_OWNER_INDEX] = perm2accessMask(permGroup);
        masks[ACLUnix.OTHER_INDEX] = perm2accessMask(permEveryone);

        return masks;
    }

    private static int applyMask(Permission perm, int access_msk, boolean allowed) {
        int msk = access_msk & (~perm.getDefMsk());
        if ( msk != 0 ) {
            perm.appendDefMsk(msk);
            if ( allowed )
                perm.appendAllowMsk(msk);
        }
        return msk;
    }

    private static int perm2accessMask(Permission perm) {

        int allow_msk = perm.getAllowMsk();

        int mask = 0;
        if ( AccessMask.READ_DATA.matches(allow_msk) || AccessMask.LIST_DIRECTORY.matches(allow_msk) )
            mask |= AMUnix.READ.getValue();

        if ( AccessMask.WRITE_DATA.matches(allow_msk) || AccessMask.ADD_FILE.matches(allow_msk) || AccessMask.APPEND_DATA.matches(allow_msk)
                || AccessMask.ADD_SUBDIRECTORY.matches(allow_msk) || AccessMask.DELETE_CHILD.matches(allow_msk) )

            mask |= AMUnix.WRITE.getValue();

        if ( AccessMask.EXECUTE.matches(allow_msk) )
            mask |= AMUnix.EXECUTE.getValue();

        return mask;
    }

    /**
     * If the ACL in question is for a directory, we split it into two ACLs, one
     * purely effective and one purely inherited.
     *
     * @param aces
     *            List of ACEs
     * @param effectiveACEs
     *            Returnes list of purely effective ACEs
     * @param inheritedACEs
     *            Returns list of purely inherited ACEs
     *
     */
    private static void splitACEs(List<ACE> aces, List<ACE> effectiveACEs, List<ACE> inheritedACEs) {
        for (ACE ace : aces) {
            int flags = ace.getFlags();
            if ( AceFlags.INHERIT_ONLY_ACE.matches(flags) == false )
                effectiveACEs.add(ace);

            if ( AceFlags.DIRECTORY_INHERIT_ACE.matches(flags) || AceFlags.FILE_INHERIT_ACE.matches(flags) )
                inheritedACEs.add(ace);
        }
    }

}
