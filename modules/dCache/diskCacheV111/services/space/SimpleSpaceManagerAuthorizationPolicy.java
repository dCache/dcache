package diskCacheV111.services.space;

import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.GroupList;
import diskCacheV111.util.VOInfo;
import org.dcache.auth.FQAN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class SimpleSpaceManagerAuthorizationPolicy
        implements SpaceManagerAuthorizationPolicy {
	private static Logger logger =
            LoggerFactory.getLogger(SimpleSpaceManagerAuthorizationPolicy.class);

    public void checkReleasePermission(AuthorizationRecord authRecord, Space space)
        throws SpaceAuthorizationException {
        String spaceGroup = space.getVoGroup();
        String spaceRole  = space.getVoRole();

        if((spaceGroup == null || spaceGroup.equals(authRecord.getVoGroup())) &&
            (spaceRole == null || spaceRole.equals(authRecord.getVoRole()))) {
            if (logger.isDebugEnabled()) {
                logger.debug("userGroup : "+authRecord.getVoGroup()+", userRole : "+
                             authRecord.getVoRole()+ " have permission to release ");
            }
            return;
        }

        for(GroupList groupList: authRecord.getGroupLists()) {
            if (groupList.getAttribute()==null) continue;
            FQAN voAttribute = new FQAN(groupList.getAttribute());
            String userGroup = voAttribute.getGroup();
            String userRole  = voAttribute.getRole();
            if((spaceGroup == null || spaceGroup.equals(userGroup)) &&
                (spaceRole == null || spaceRole.equals(userRole))) {
                if (logger.isDebugEnabled()) {
                    logger.debug("userGroup : "+userGroup+", userRole : "+userRole+
                                 " have permission to release ");
                }
                return;
            }
        }

        throw new SpaceAuthorizationException("user with "+authRecord+
                " has no permission to release "+space);
    }

    public VOInfo checkReservePermission(AuthorizationRecord authRecord, LinkGroup linkGroup)
        throws SpaceAuthorizationException {
        VOInfo[] voInfos = linkGroup.getVOs();
        for(VOInfo voInfo:voInfos) {
            String userGroup = authRecord.getVoGroup();
            String userRole = authRecord.getVoRole();
            if (VOInfo.match(voInfo,userGroup,userRole)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("userGroup : "+userGroup+", userRole : "+userRole+
                                 " have permission to reserve ");
                }
                return new VOInfo(userGroup,userRole );
            }

            for(GroupList groupList: authRecord.getGroupLists()) {
                if (groupList.getAttribute()==null) continue;
                FQAN voAttribute = new FQAN(groupList.getAttribute());
                userGroup = voAttribute.getGroup();
                userRole = voAttribute.getRole();
                if (VOInfo.match(voInfo,userGroup,userRole)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("userGroup : "+userGroup+", userRole : "+userRole+
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
