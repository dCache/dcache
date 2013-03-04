package org.dcache.pinmanager;

import javax.security.auth.Subject;

import org.dcache.pinmanager.model.Pin;

public interface AuthorizationPolicy
{
    boolean canUnpin(Subject subject, Pin pin);
    boolean canExtend(Subject subject, Pin pin);
}
