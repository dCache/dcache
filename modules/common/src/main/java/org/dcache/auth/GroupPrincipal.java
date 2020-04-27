package org.dcache.auth;

import java.security.Principal;

/**
 * An interface for group principals. Group principals distinguish
 * themselves from other principals by having a primaryGroup property.
 * @since 2.1
 */
public interface GroupPrincipal extends Principal
{
    boolean isPrimaryGroup();
}
