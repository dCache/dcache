package diskCacheV111.services.space;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import diskCacheV111.util.VOInfo;

import org.dcache.auth.FQAN;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.Subjects;

public class SimpleSpaceManagerAuthorizationPolicy
        implements SpaceManagerAuthorizationPolicy {
	private static final Logger logger =
            LoggerFactory.getLogger(SimpleSpaceManagerAuthorizationPolicy.class);

    @Override
    public void checkReleasePermission(Subject subject, Space space)
        throws SpaceAuthorizationException {
        String spaceGroup = space.getVoGroup();
        String spaceRole  = space.getVoRole();

        if (spaceGroup != null) {
            if (spaceRole == null) {
                if (spaceGroup.equals(Subjects.getUserName(subject))) {
                    logger.debug("Subject with user name {} has permission to release space {}",
                                 Subjects.getUserName(subject), space);
                    return;
                }

                try {
                    long authorisedGid = Long.parseLong(spaceGroup);

                    if (Subjects.hasGid(subject, authorisedGid)) {
                        logger.debug("Subject with gid {} has permission to release space {}",
                                authorisedGid, space);
                        return;
                    }
                } catch (NumberFormatException e) {
                    // It is OK for spaceGroup not to be a valid Long.
                }
            }

            for (FQANPrincipal principal : subject.getPrincipals(FQANPrincipal.class)) {
                FQAN fqan = principal.getFqan();
                if (spaceGroup.equals(fqan.getGroup()) && (spaceRole == null || spaceRole.equals(fqan.getRole()))) {
                    logger.debug("Subject with fqan {} has permission to release space {}",
                                 fqan, space);
                    return;
                }
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

            for (long gid : Subjects.getGids(subject)) {
                if (voInfo.match(Long.toString(gid), null)) {
                    logger.debug("Subject with gid {} has permission to reserve {}", gid, linkGroup);
                    return new VOInfo(Long.toString(gid), null);
                }
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
