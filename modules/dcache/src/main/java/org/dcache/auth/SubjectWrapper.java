package org.dcache.auth;

import com.google.common.collect.Collections2;

import javax.security.auth.Subject;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * The class is a Wrapper for the Subject, used for StringTemplate
 * to fetch particular subject's attributes that could be specified
 * in billing.properties or dcache.conf and will be printed in billing files.
 */
public class SubjectWrapper {

    private final Subject _subject;

    public SubjectWrapper(Subject subject) {
        _subject = subject;
    }

    public Long getUid() {
        return Subjects.isNobody(_subject) ? null : Subjects.getUid(_subject);
    }

    public Long getPrimaryGid() {
        return Subjects.isNobody(_subject) ? null : Subjects.getPrimaryGid(_subject);
    }

    public long[] getGids() {
        return Subjects.getGids(_subject);
    }

    public String getDn() {
        return Subjects.getDn(_subject);
    }

    public String getPrimaryFqan() {
        return Objects.toString(Subjects.getPrimaryFqan(_subject), null);
    }

    public Collection<String> getFqans() {
        return Collections2.transform(Subjects.getFqans(_subject), FQAN::toString);
    }

    public String getUserName() {
        return Subjects.getUserName(_subject);
    }

    public String getLoginName() {
        return Subjects.getLoginName(_subject);
    }

    public String getDisplayName() {
        return Subjects.getDisplayName(_subject);
    }

    public List<Principal> getPrincipals() {
        return new ArrayList<>(_subject.getPrincipals());
    }

    @Override
    public String toString() {
        return _subject.getPrincipals().toString();
    }
}
