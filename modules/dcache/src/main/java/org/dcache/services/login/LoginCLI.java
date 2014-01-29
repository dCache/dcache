package org.dcache.services.login;

import javax.security.auth.Subject;

import java.lang.reflect.Constructor;
import java.security.Principal;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellCommandListener;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.LoginReply;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.Subjects;
import org.dcache.auth.UidPrincipal;
import org.dcache.util.Args;

public class LoginCLI
    implements CellCommandListener
{
    private LoginStrategy _loginStrategy;

    public void setLoginStrategy(LoginStrategy loginStrategy)
    {
        _loginStrategy = loginStrategy;
    }

    public LoginStrategy getLoginStrategy()
    {
        return _loginStrategy;
    }

    public static final String fh_test_login = ""
            + "This command simulates an attempt to login with a particular set of\n"
            + "principals, as extracted by a door.  The result of the login is shown:\n"
            + "either the login succeeds or fails.  If the login succeeds then the set\n"
            + "of identities is shown.\n"
            + "\n"
            + "Each supplied principal has the form <type>:<value> (e.g. 'name:paul').\n"
            + "If a principal has spaces then surround the declaration with quote-marks\n"
            + "(e.g., \"dn:/C=DE/O=ACME/CN=Example certificate\").\n"
            + "\n"
            + "Valid principal types are:\n"
            + "\n"
            + "    name      a user-requested username\n"
            + "    kerberos  a kerberos principal (e.g. paul@EXAMPLE.ORG)\n"
            + "    dn        the distinguished name from an X509 certificate\n"
            + "    fqan      an FQAN, the first is taken as the primary FQAN\n";
    public static final String hh_test_login = "<principal> [<principal> ...] # show result of login";
    public String ac_test_login_$_1_99(Args args) {
        Subject subject = Subjects.subjectFromArgs(args.getArguments());
        try {
            LoginReply reply = _loginStrategy.login(subject);
            return reply.toString();
        } catch(CacheException e) {
            return e.toString();
        }
    }


    public static final String fh_get_identity = "get identity <principal> <type>"
            + "\n"
            + "Get identity for provided principal."
            + "\nExample:"
            + "   get identity atlas01 UserNamePrincipal";
    public static final String hh_get_identity = "<principal> <type>";
    public String ac_get_identity_$_2(Args args) throws Exception {
        String name = args.argv(0);
        String type = args.argv(1);

        Principal p = _loginStrategy.map( principalOf(type, name) );
        if(p != null) {
            return p.getName();
        }
        return "No mapping for specified principal found.";
    }

    public static final String fh_get_ridentity = "get ridentity -group <pringipal>\n"+
            "\n"+
            "Get reverse identity mapping for provided id." +
            "  -group  provided id represents a group id." +
            "\nExample:"+
            "  get ridentity -group 100";
    public static final String hh_get_ridentity = "<principal>";
    public String ac_get_ridentity_$_1(Args args)
            throws CacheException {
        String id = args.argv(0);
        boolean isGroup = args.hasOption("group");

        Principal principal;
        if(isGroup) {
            principal = new GidPrincipal(id, false);
        }else{
            principal = new UidPrincipal(id);
        }
        return _loginStrategy.reverseMap(principal).toString();
    }

    private final static String PREFIX = "org.dcache.auth.";
    private Principal principalOf(String type, String name) throws Exception {
        Class<? extends Principal> c;
        try {
            c = Class.forName(type).asSubclass(Principal.class);
        }catch(ClassNotFoundException e) {
            c = Class.forName(PREFIX + type).asSubclass(Principal.class);
        }

        Constructor<? extends Principal> constructor = c.getConstructor(String.class);
        return constructor.newInstance(name);
    }

}
