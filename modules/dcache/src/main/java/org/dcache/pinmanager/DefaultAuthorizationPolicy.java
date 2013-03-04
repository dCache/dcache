package org.dcache.pinmanager;

import javax.security.auth.Subject;

import org.dcache.auth.Subjects;
import org.dcache.pinmanager.model.Pin;

public class DefaultAuthorizationPolicy implements AuthorizationPolicy
{
    private boolean isAuthorized(Subject subject, Pin pin)
    {
        return (Subjects.isRoot(subject) ||
                Subjects.hasUid(subject, pin.getUid()) ||
                Subjects.hasGid(subject, pin.getGid()));
    }

    @Override
    public boolean canUnpin(Subject subject, Pin pin)
    {
        return isAuthorized(subject, pin);
    }

    @Override
    public boolean canExtend(Subject subject, Pin pin)
    {
        return isAuthorized(subject, pin);
    }
}
