package diskCacheV111.services.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.util.VOInfo;

import org.dcache.auth.FQAN;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.Subjects;

/**
 *
 * @author timur
 */
public class SimpleSpaceManagerAuthorizationPolicy
        implements SpaceManagerAuthorizationPolicy {
	private static Logger logger =
            LoggerFactory.getLogger(SimpleSpaceManagerAuthorizationPolicy.class);

    @Override
    public void checkReleasePermission(Subject subject, Space space)
        throws SpaceAuthorizationException {
        String spaceGroup = space.getVoGroup();
        String spaceRole  = space.getVoRole();

        if (spaceGroup == null && spaceRole != null) {
            logger.debug("Space {} has no owner and can be released by anybody", space);
            return;
        }
        if (spaceGroup != null && spaceGroup.equals(Subjects.getUserName(subject)) && spaceRole == null) {
            logger.debug("Subject with user name {} has permission to release space {}",
                    Subjects.getUserName(subject), space);
            return;
        }

        for (FQANPrincipal principal : subject.getPrincipals(FQANPrincipal.class)) {
            FQAN fqan = principal.getFqan();
            if ((spaceGroup == null || spaceGroup.equals(fqan.getGroup())) &&
                    (spaceRole == null || spaceRole.equals(fqan.getRole()))) {
                logger.debug("Subject with fqan {} has permission to release space {}",
                        fqan, space);
                return;
            }
        }

        throw new SpaceAuthorizationException("Subject " + subject.getPrincipals() +
                " has no permission to release " + space);
    }

    @Override
    public VOInfo checkReservePermission(Subject subject, LinkGroup linkGroup)
        throws SpaceAuthorizationException {
        for (VOInfo voInfo: linkGroup.getVOs()) {
            String userName = Subjects.getUserName(subject);
            if (userName != null && voInfo.match(userName, null)) {
                logger.debug("Subject with user name {} has permission to reserve {}", userName, linkGroup);
                return new VOInfo(userName, null);
            }

            for (FQANPrincipal principal : subject.getPrincipals(FQANPrincipal.class)) {
                FQAN fqan = principal.getFqan();

                if (voInfo.match(fqan.getGroup(), fqan.getRole())) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Subject with FQAN {} has permission to reserve {}", fqan, linkGroup);
                    }
                    return new VOInfo(fqan.getGroup(), fqan.getRole());
                }
            }
        }
        throw new SpaceAuthorizationException("Subject " + subject.getPrincipals() +
                " has no permission to reserve in " + linkGroup);
    }
}
