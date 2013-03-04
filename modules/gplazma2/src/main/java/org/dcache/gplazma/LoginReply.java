package org.dcache.gplazma;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.security.auth.Subject;

import java.util.Collections;
import java.util.Set;

public class LoginReply
{
    private Subject _subject;
    private Set<Object> _sessionAttributes = Collections.emptySet();

    public void setSubject(Subject subject) {
        _subject = subject;
    }

    public Subject getSubject() {
        return _subject;
    }

    public void setSessionAttributes(Set<Object> sessionAttributes) {
        if (sessionAttributes == null) {
            throw new NullPointerException();
        }
        _sessionAttributes = sessionAttributes;
    }

    public Set<Object> getSessionAttributes() {
        return Collections.unmodifiableSet(_sessionAttributes);
    }

    public <T> Set<T> getSessionAttributesByType(Class<T> c) {
        return Sets.newHashSet(Iterables.filter(_sessionAttributes, c));
    }

    @Override
    public String toString() {
        return "LoginReply[" + _subject + "," + Joiner.on(',').join(_sessionAttributes) + "]";
    }
}
