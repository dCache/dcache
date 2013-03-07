package diskCacheV111.services.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.VOInfo;

import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.FQAN;
import org.dcache.auth.GroupList;

/**
 *
 * @author timur
 */
public class SimpleSpaceManagerAuthorizationPolicy
        implements SpaceManagerAuthorizationPolicy {
	private static Logger logger =
            LoggerFactory.getLogger(SimpleSpaceManagerAuthorizationPolicy.class);

    @Override
    public void checkReleasePermission(AuthorizationRecord authRecord, Space space)
        throws SpaceAuthorizationException {
        String spaceGroup = space.getVoGroup();
        String spaceRole  = space.getVoRole();

        if((spaceGroup == null || spaceGroup.equals(authRecord.getVoGroup())) &&
            (spaceRole == null || spaceRole.equals(authRecord.getVoRole()))) {
            if (logger.isDebugEnabled()) {
                logger.trace("userGroup : "+authRecord.getVoGroup()+", userRole : "+
                             authRecord.getVoRole()+ " have permission to release ");
            }
            return;
        }

        for(GroupList groupList: authRecord.getGroupLists()) {
            String attribute = groupList.getAttribute();
            if (attribute == null) {
                continue;
            }

            String userGroup;
            String userRole;
            if( FQAN.isValid( attribute)) {
                FQAN fqan = new FQAN(attribute);
                userGroup = fqan.getGroup();
                userRole  = fqan.getRole();
            } else {
                userGroup = attribute;
                userRole = "";
            }
            if((spaceGroup == null || spaceGroup.equals(userGroup)) &&
                (spaceRole == null || spaceRole.equals(userRole))) {
                if (logger.isDebugEnabled()) {
                    logger.trace("userGroup : "+userGroup+", userRole : "+userRole+
                                 " have permission to release ");
                }
                return;
            }
        }

        throw new SpaceAuthorizationException("user with "+authRecord+
                " has no permission to release "+space);
    }

    @Override
    public VOInfo checkReservePermission(AuthorizationRecord authRecord, LinkGroup linkGroup)
        throws SpaceAuthorizationException {
        VOInfo[] voInfos = linkGroup.getVOs();
        for(VOInfo voInfo:voInfos) {
            String userGroup = authRecord.getVoGroup();
            String userRole = authRecord.getVoRole();
            if (voInfo.match(userGroup,userRole)) {
                if (logger.isDebugEnabled()) {
                    logger.trace("userGroup : "+userGroup+", userRole : "+userRole+
                                 " have permission to reserve ");
                }
                return new VOInfo(userGroup,userRole );
            }

            for(GroupList groupList: authRecord.getGroupLists()) {
                String attribute = groupList.getAttribute();
                if (attribute == null) {
                    continue;
                }
                if( FQAN.isValid( attribute)) {
                    FQAN fqan = new FQAN(attribute);
                    userGroup = fqan.getGroup();
                    userRole = fqan.getRole();
                } else {
                    userGroup = attribute;
                    userRole = "";
                }
                if (voInfo.match(userGroup,userRole)) {
                    if (logger.isDebugEnabled()) {
                        logger.trace("userGroup : "+userGroup+", userRole : "+userRole+
                                     " have permission to reserve ");
                        return new VOInfo(userGroup,userRole );
                        }
                }
            }

        }
        throw new SpaceAuthorizationException("user with "+authRecord+
                                                  " has no permission to reserve in "+linkGroup);
    }

}
