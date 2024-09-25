package org.dcache.auth;

/**
 * This Principal represents the UID of a person.  In contrast to LoginUidPrincipal, UidPrincipal
 * represents an identity that the end-user is allowed to adopt.  Therefore, it is safe to base
 * authorisation decisions on this principal.
 *
 * @see LoginUidPrincipal
 * @since 2.1
 */
@AuthenticationOutput
@AuthenticationInput
public class UidPrincipal extends AbstractIdPrincipal {

    private static final long serialVersionUID = -6614351509379265417L;

    public UidPrincipal(long uid) {
        super(uid);
    }

    public UidPrincipal(String uid) { super(uid); }

    public long getUid() {
        return getId();
    }
}
