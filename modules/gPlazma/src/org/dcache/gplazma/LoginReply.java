package org.dcache.gplazma;

import java.util.Set;

import javax.security.auth.Subject;

/**
 * Message returned as a result of a login operation by the LoginHandler
 *
 */
public class LoginReply {
    private Subject _subject;
    private Set<SessionAttribute> _sessionAttributes;

    public void setSubject(Subject subject) {
        _subject = subject;
    }

    public Subject getSubject() {
        return _subject;
    }

    public void setSessionAttributes(Set<SessionAttribute> sessionAttributes) {
        _sessionAttributes = sessionAttributes;
    }

    public Set<SessionAttribute> getSessionAttributes() {
        return _sessionAttributes;
    }
}
