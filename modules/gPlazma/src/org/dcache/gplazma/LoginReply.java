package org.dcache.gplazma;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.security.auth.Subject;

/**
 * Message returned as a result of a login operation by the LoginHandler
 *
 */
public class LoginReply {
    private Subject _subject;
    private Set<SessionAttribute> _sessionAttributes = Collections.emptySet();

    public void setSubject(Subject subject) {
        _subject = subject;
    }

    public Subject getSubject() {
        return _subject;
    }

    public void setSessionAttributes(Set<SessionAttribute> sessionAttributes) {
        if( sessionAttributes == null) {
            throw new IllegalArgumentException("Not allowed to pass null as a parameter");
        }

        _sessionAttributes = sessionAttributes;
    }

    public Set<SessionAttribute> getSessionAttributes() {
        return Collections.unmodifiableSet( _sessionAttributes);
    }

    public Set<SessionAttribute> getSessionAttributeByName( String name) {
        Set<SessionAttribute> resultSet = new HashSet<SessionAttribute>();

        for( SessionAttribute attribute : _sessionAttributes) {
            if( name.equals( attribute.getName())) {
                resultSet.add( attribute);
            }
        }

        return resultSet;
    }

    public Set<SessionAttribute> getSessionAttributesByType( Class<? extends SessionAttribute> type) {
        Set<SessionAttribute> resultSet = new HashSet<SessionAttribute>();

        for( SessionAttribute attribute : _sessionAttributes) {
            if( type.isInstance( attribute)) {
                resultSet.add( attribute);
            }
        }

        return resultSet;
    }
}
