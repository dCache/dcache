package org.dcache.gplazma.validation;

import java.security.Principal;
import javax.security.auth.Subject;
import java.util.Set;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.auth.attributes.ReadOnly;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.LoginReply;
import org.dcache.gplazma.monitor.LoginMonitor;
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
    public void validate(LoginReply loginReply)
            throws AuthenticationException {
        LOGGER.debug("Validating loginReply {}",loginReply);
        if(loginReply == null) {
            throw new NullPointerException ("loginReply is null");
        }
        Set<Principal> principals = getPrincipalsFromLoginReply(loginReply);
        validatePrincipals(principals);
        Set<Object> attributes = getSessionAttributesFromLoginReply(loginReply);
        validateAttributes(attributes);
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
    private static void validatePrincipals(Set<Principal> principals)
            throws AuthenticationException {
        boolean userNamePrincipalFound = false;
        boolean uidPrincipalFound = false;
        boolean primaryGidPrincipalFound = false;
        for(Principal principal:principals) {
            if(principal instanceof UserNamePrincipal) {
                if(userNamePrincipalFound ) {
                    throw new AuthenticationException(
                            "multiple usernames");
                }
                userNamePrincipalFound = true;
                continue;
            }
            if(principal instanceof UidPrincipal) {
                if(uidPrincipalFound ) {
                    throw new AuthenticationException("multiple UIDs");
                }
                uidPrincipalFound = true;
                continue;
            }
            if(principal instanceof GidPrincipal) {
                GidPrincipal gidPrincipal = (GidPrincipal) principal;
                if(gidPrincipal.isPrimaryGroup()) {
                    if(primaryGidPrincipalFound ) {
                        throw new AuthenticationException("multiple GIDs");
                    }
                    primaryGidPrincipalFound = true;
                }
                continue;
            }
        }

        if( userNamePrincipalFound && uidPrincipalFound && primaryGidPrincipalFound ) {
            return;
        }
        StringBuilder errorMsg = new StringBuilder();
        if(!userNamePrincipalFound) {
            errorMsg.append("no username");
        }
        if(!uidPrincipalFound) {
            appendWithComma(errorMsg, "no UID");
        }
        if(!primaryGidPrincipalFound) {
            appendWithComma(errorMsg, "no primary GID");
        }
        throw new AuthenticationException(errorMsg.toString());
    }

    /**
     * checks if  {@link attributes} has at one and only one of each of
     * HomeDirectory and RootDirectory
     * @param attributes
     * @throws AuthenticationException if check fails
     */
    private static void validateAttributes(Set<Object> attributes)
            throws  AuthenticationException {
        boolean homeDirectoryFound = false;
        boolean rootDirectoryFound = false;
        boolean readOnlyFound = false;
        for(Object attribute:attributes) {
            if(attribute instanceof HomeDirectory) {
               if(homeDirectoryFound ) {
                    throw new AuthenticationException(
                            "multiple home-directories");
                }
                homeDirectoryFound = true;
            }
            if(attribute instanceof RootDirectory) {
               if(rootDirectoryFound ) {
                    throw new AuthenticationException(
                            "multiple root-directories");
                }
                rootDirectoryFound = true;
            }
            if(attribute instanceof ReadOnly) {
               if(readOnlyFound ) {
                    throw new AuthenticationException(
                            "multiple read-only declarations");
                }
                readOnlyFound = true;
            }
        }

        if ( homeDirectoryFound && rootDirectoryFound && readOnlyFound) {
            return;
        }

        StringBuilder errorMsg = new StringBuilder();
        if(!homeDirectoryFound) {
            errorMsg.append("no home-directory");
        }
        if(!rootDirectoryFound) {
            appendWithComma(errorMsg, "no root-directory");
        }
        if(!readOnlyFound) {
            appendWithComma(errorMsg, "no read-only declaration");
        }
        throw new AuthenticationException(errorMsg.toString());
    }

    private static StringBuilder appendWithComma(StringBuilder sb, String message) {
        if(sb.length() > 0) {
            sb.append(", ");
        }

        return sb.append(message);
    }


    private static Set<Object> getSessionAttributesFromLoginReply(LoginReply loginReply) throws AuthenticationException {
        Set<Object> attributes = loginReply.getSessionAttributes();
        if (attributes == null) {
            throw new AuthenticationException("attributes is null");
        }
        return attributes;
    }

    private static Set<Principal> getPrincipalsFromLoginReply(LoginReply loginReply) throws AuthenticationException {
        Subject subject = loginReply.getSubject();
        if (subject == null) {
            throw new AuthenticationException("subject is null");
        }
        Set<Principal> principals = subject.getPrincipals();
        if (principals == null) {
            throw new AuthenticationException("subject principals is null");
        }
        return principals;
    }


}
