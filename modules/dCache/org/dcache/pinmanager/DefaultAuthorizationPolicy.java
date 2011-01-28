package org.dcache.pinmanager;

import javax.security.auth.Subject;
import org.dcache.pinmanager.model.Pin;
import org.dcache.auth.Subjects;

public class DefaultAuthorizationPolicy implements AuthorizationPolicy
{
    private boolean isAuthorized(Subject subject, Pin pin)
    {
        return (Subjects.isRoot(subject) ||
                Subjects.hasUid(subject, pin.getUid()) ||
                Subjects.hasGid(subject, pin.getGid()));
    }

    public boolean canUnpin(Subject subject, Pin pin)
    {
        return isAuthorized(subject, pin);
    }

    public boolean canExtend(Subject subject, Pin pin)
    {
        return isAuthorized(subject, pin);
    }
}
