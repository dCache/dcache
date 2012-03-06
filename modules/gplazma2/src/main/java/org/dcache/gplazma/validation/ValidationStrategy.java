package org.dcache.gplazma.validation;

import org.dcache.gplazma.LoginReply;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.monitor.LoginMonitor;

/**
 * Implementing classes will implement validate method where the loginReply will
 * be validated according to the strategy specific rules, the implementation is
 * not expected to modify the loginReply in any way
 * The implementation is expected to throw AuthenticationException if the
 * validation fails.
 *
 */
public interface ValidationStrategy {

    public void validate(LoginReply loginReply)
                throws AuthenticationException;
}
