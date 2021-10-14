package org.dcache.services.login;

import diskCacheV111.vehicles.Message;
import java.security.Principal;

/**
 * Requests a map operation by a login cell.
 */
public class MapMessage extends Message {

    private static final long serialVersionUID = 6607289513753033937L;

    private final Principal _principal;
    private Principal _mappedPrincipal;

    public MapMessage(Principal principal) {
        _principal = principal;
    }

    public Principal getPrincipal() {
        return _principal;
    }

    public void setMappedPrincipal(Principal principal) {
        _mappedPrincipal = principal;
    }

    public Principal getMappedPrincipal() {
        return _mappedPrincipal;
    }
}
