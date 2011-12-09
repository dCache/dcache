package org.dcache.auth;

import java.util.Collection;
import javax.security.auth.Subject;

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

    public long[] getUids() {
        return Subjects.getUids(_subject);
    }

    public long getUid() {
        return Subjects.getUid(_subject);
    }

    public long[] getGids() {
        return Subjects.getGids(_subject);
    }

    public long getPrimaryGid() {
        return Subjects.getPrimaryGid(_subject);
    }

    public String getDn() {
        return Subjects.getDn(_subject);
    }

    public String getPrimaryFqan() {
        return Subjects.getPrimaryFqan(_subject);
    }

    public Collection<String> getFqans() {
        return Subjects.getFqans(_subject);
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

    @Override
    public String toString() {
        return _subject.getPrincipals().toString();
    }
}
