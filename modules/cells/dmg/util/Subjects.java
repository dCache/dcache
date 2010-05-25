package dmg.util;

import java.security.Principal;
import java.util.Iterator;
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.auth.Origin;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.dcache.auth.UserNamePrincipal;

public class Subjects {

    private static final String UNKNOWN = "unknown";

    /*
     * Try user name, X509, kerberos, Origin
     */
    private final static Class<? extends Principal>[] CLASSES = new Class[] {
        UserNamePrincipal.class,
        GlobusPrincipal.class,
        KerberosPrincipal.class,
        Origin.class,
        Principal.class
    };

    public static String getDisplayName(Subject subject) {

        for(Class<? extends Principal> principalClass: CLASSES) {

            Set<? extends Principal> principals = subject.getPrincipals(principalClass);
            if( !principals.isEmpty()) {
                return principals.iterator().next().getName();
            }
        }
        return UNKNOWN;
    }

    /**
     * Returns the the user name of a subject. If UserNamePrincipal is
     * not defined then default string is returned.
     */
    public static String getUserName(Subject subject) {
        Iterator<UserNamePrincipal> principals = subject.getPrincipals(UserNamePrincipal.class).iterator();
        if(principals.hasNext()) {
            return principals.next().getName();
        }
        return UNKNOWN;
    }
}
