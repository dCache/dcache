package org.dcache.gplazma.validation;

import java.security.Principal;
import javax.security.auth.Subject;
import java.util.Set;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.SessionAttribute;
import org.dcache.gplazma.HomeDirectory;
import org.dcache.gplazma.RootDirectory;
import org.dcache.gplazma.ReadOnly;
import org.dcache.gplazma.SessionID;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.LoginReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Door specific validation strategy
 * which checks that there is one and only one user principal, one and only one
 * Uid Principals, one and only one primary GID principal present in principals,
 * and there is one and only one of each home and root directories in the
 * attributes
 * @author timur
 */
public class DoorValidationStrategy  implements ValidationStrategy {

    private static final Logger LOGGER = LoggerFactory.getLogger(DoorValidationStrategy.class);

    @Override
    public void validate(SessionID sessionId, LoginReply loginReply)
            throws AuthenticationException {
        LOGGER.debug("Validating loginReply {}",loginReply);
        if(loginReply == null) {
            throw new NullPointerException ("loginReply is null");
        }
        Set<Principal> principals = getPrincipalsFromLoginReply(loginReply);
        validatePrincipals(sessionId,principals);
        Set<SessionAttribute> attributes = getSessionAttributesFromLoginReply(loginReply);
        validateAttributes(sessionId,attributes);
    }

    /**
     * checks if authorizedPrincipals set contain at one and only one
     * instance of each type of
     * {@link UidPrincipal UidPrincipal},
     * {@link GidPrincipal GidPrincipal} and
     * {@link UserNamePrincipal UserNamePrincipal}
     * @param principals
     * @throws AuthenticationException if check fails
     */
    private static void validatePrincipals(SessionID sessionId,
            Set<Principal> principals)
            throws AuthenticationException {
        boolean userNamePrincipalFound = false;
        boolean uidPrincipalFound = false;
        boolean primaryGidPrincipalFound = false;
        for(Principal principal:principals) {
            if(principal instanceof UserNamePrincipal) {
                if(userNamePrincipalFound ) {
                    throw new AuthenticationException(
                            "more than one UserNamePrincipal found in loginReply");
                }
                userNamePrincipalFound = true;
                continue;
            }
            if(principal instanceof UidPrincipal) {
                if(uidPrincipalFound ) {
                    throw new AuthenticationException(
                            "more than one UidPrincipal found in loginReply");
                }
                uidPrincipalFound = true;
                continue;
            }
            if(principal instanceof GidPrincipal) {
                GidPrincipal gidPrincipal = (GidPrincipal) principal;
                if(gidPrincipal.isPrimaryGroup()) {
                    if(primaryGidPrincipalFound ) {
                        throw new AuthenticationException(
                                "more than one primary GidPrincipal found in loginReply");
                    }
                    primaryGidPrincipalFound = true;
                }
                continue;
            }
        }

        if( userNamePrincipalFound && uidPrincipalFound && primaryGidPrincipalFound ) {
            return;
        }
        StringBuilder errorMsg = new StringBuilder("loginReply validation failed :");
        if(!userNamePrincipalFound) {
            errorMsg.append(" UserNamePrincipal is not found;");
        }
        if(!uidPrincipalFound) {
            errorMsg.append(" UidPrincipal is not found;");
        }
        if(!primaryGidPrincipalFound) {
            errorMsg.append(" primary GidPrincipal is not found;");
        }
        throw new AuthenticationException(errorMsg.toString());
    }

    /**
     * checks if  {@link attributes} has at one and only one of each of
     * HomeDirectory and RootDirectory
     * @param attributes
     * @throws AuthenticationException if check fails
     */
    private static void validateAttributes(SessionID sessionId,
            Set<SessionAttribute> attributes)
            throws  AuthenticationException {
        boolean homeDirectoryFound = false;
        boolean rootDirectoryFound = false;
        boolean readOnlyFound = false;
        for(SessionAttribute attribute:attributes) {
            if(attribute instanceof HomeDirectory) {
               if(homeDirectoryFound ) {
                    throw new AuthenticationException(
                            "more than one HomeDirectory found in loginReply");
                }
                homeDirectoryFound = true;
            }
            if(attribute instanceof RootDirectory) {
               if(rootDirectoryFound ) {
                    throw new AuthenticationException(
                            "more than one RootDirectory found in loginReply");
                }
                rootDirectoryFound = true;
            }
            if(attribute instanceof ReadOnly) {
               if(readOnlyFound ) {
                    throw new AuthenticationException(
                            "more than one ReadOnly session attributes found in loginReply");
                }
                readOnlyFound = true;
            }
        }

        if ( homeDirectoryFound && rootDirectoryFound && readOnlyFound) {
            return;
        }

        StringBuilder errorMsg = new StringBuilder("loginReply validation failed :");
        if(!homeDirectoryFound) {
            errorMsg.append(" HomeDirectory is not found;");
        }
        if(!rootDirectoryFound) {
            errorMsg.append(" RootDirectory is not found;");
        }
        if(!readOnlyFound) {
            errorMsg.append(" ReadOnly session attribute is not found;");
        }
        throw new AuthenticationException(errorMsg.toString());
    }

    private static final Set<SessionAttribute> getSessionAttributesFromLoginReply(LoginReply loginReply) throws AuthenticationException {
        Set<SessionAttribute> attributes = loginReply.getSessionAttributes();
        if (attributes == null) {
            throw new AuthenticationException("loginReply attributes set is null");
        }
        return attributes;
    }

    private static final Set<Principal> getPrincipalsFromLoginReply(LoginReply loginReply) throws AuthenticationException {
        Subject subject = loginReply.getSubject();
        if (subject == null) {
            throw new AuthenticationException("loginReply subject is null");
        }
        Set<Principal> principals = subject.getPrincipals();
        if (principals == null) {
            throw new AuthenticationException("loginReply subject principals set is null");
        }
        return principals;
    }


}
