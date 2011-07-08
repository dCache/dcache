package org.dcache.gplazma;

import java.security.Principal;

/**
 *
 */
public class NoSuchPrincipalException extends Exception {

    static final long serialVersionUID = 87356059991976395L;

    public NoSuchPrincipalException(Principal principal) {
        super("No such principal: " + principal.getName());
    }

}
