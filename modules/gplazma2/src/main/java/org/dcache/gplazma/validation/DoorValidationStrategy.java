package org.dcache.gplazma.validation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.Set;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.LoginReply;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

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
        boolean hasUserName = false;
        boolean hasUid = false;
        boolean hasPrimaryGid = false;
        for(Principal principal:principals) {
            if(principal instanceof UserNamePrincipal) {
                checkAuthentication(!hasUserName, "multiple usernames");
                hasUserName = true;
                continue;
            }
            if(principal instanceof UidPrincipal) {
                checkAuthentication(!hasUid, "multiple UIDs");
                hasUid = true;
                continue;
            }
            if(principal instanceof GidPrincipal) {
                GidPrincipal gidPrincipal = (GidPrincipal) principal;
                if(gidPrincipal.isPrimaryGroup()) {
                    checkAuthentication(!hasPrimaryGid, "multiple GIDs");
                    hasPrimaryGid = true;
                }
            }
        }

        checkAuthentication(hasUserName && hasUid && hasPrimaryGid,
                principalsErrorMessage(hasUserName, hasUid, hasPrimaryGid));
    }


    private static String principalsErrorMessage(boolean hasUserName,
            boolean hasUid, boolean hasPrimaryGid)
    {
        StringBuilder errorMessage = new StringBuilder();

        if(!hasUserName) {
            errorMessage.append("no username");
        }
        if(!hasUid) {
            appendWithComma(errorMessage, "no UID");
        }
        if(!hasPrimaryGid) {
            appendWithComma(errorMessage, "no primary GID");
        }

        return errorMessage.toString();
    }

    /**
     * checks if  {@link attributes} has at one and only one of each of
     * HomeDirectory and RootDirectory
     * @param attributes
     * @throws AuthenticationException if check fails
     */
    private static void validateAttributes(Set<Object> attributes)
            throws  AuthenticationException
    {
        boolean hasHome = false;
        boolean hasRoot = false;

        for(Object attribute:attributes) {
            if(attribute instanceof HomeDirectory) {
                checkAuthentication(!hasHome, "multiple home-directories");
                hasHome = true;
            }
            if(attribute instanceof RootDirectory) {
                checkAuthentication(!hasRoot, "multiple root-directories");
                hasRoot = true;
            }
        }

        checkAuthentication(hasHome && hasRoot, attributesErrorMessage(hasHome, hasRoot));
    }

    private static String attributesErrorMessage(boolean hasHome, boolean hasRoot)
    {
        StringBuilder errorMsg = new StringBuilder();

        if (!hasHome) {
            errorMsg.append("no home-directory");
        }
        if (!hasRoot) {
            if (!hasHome) {
                errorMsg.append(", ");
            }
            errorMsg.append("no root-directory");
        }

        return errorMsg.toString();
    }

    private static StringBuilder appendWithComma(StringBuilder sb,
            String message)
    {
        if(sb.length() > 0) {
            sb.append(", ");
        }

        return sb.append(message);
    }

    private static Set<Object> getSessionAttributesFromLoginReply(LoginReply loginReply)
            throws AuthenticationException
    {
        Set<Object> attributes = loginReply.getSessionAttributes();
        checkAuthentication(attributes != null, "attributes is null");
        return attributes;
    }

    private static Set<Principal> getPrincipalsFromLoginReply(LoginReply loginReply)
            throws AuthenticationException
    {
        Subject subject = loginReply.getSubject();
        checkAuthentication(subject != null, "subject is null");

        Set<Principal> principals = subject.getPrincipals();
        checkAuthentication(principals != null, "subject principals is null");

        return principals;
    }
}
