package org.dcache.services.login;

import javax.security.auth.Subject;

import org.globus.gsi.jaas.GlobusPrincipal;

import org.dcache.cells.CellCommandListener;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.LoginStrategy;
import diskCacheV111.util.CacheException;

import dmg.util.Args;
import java.lang.reflect.Constructor;
import java.security.Principal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;

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

    public final String hh_get_mapping = "<DN> [<FQAN>[,<FQAN>]...]";
    public String ac_get_mapping_$_1_2(Args args)
        throws CacheException
    {
        String principal = args.argv(0);
        String[] fqans =
            (args.argc() < 2) ? new String[0] : args.argv(1).split(",");

        Subject subject = new Subject();
        subject.getPrincipals().add(new GlobusPrincipal(principal));
        if (fqans.length > 0) {
            subject.getPrincipals().add(new FQANPrincipal(fqans[0], true));
            for (int i = 1; i < fqans.length; i++) {
                subject.getPrincipals().add(new FQANPrincipal(fqans[i], false));
            }
        }

        return _loginStrategy.login(subject).toString();
    }

    public final String fh_get_identity = "get identity <principal> <type>"
            + "\n"
            + "Get identity for provided principal."
            + "\nExample:"
            + "   get identity atlas01 UserNamePrincipal";
    public final String hh_get_identity = "<principal> <type>";
    public String ac_get_identity_$_2(Args args) throws Exception {
        String name = args.argv(0);
        String type = args.argv(1);

        return _loginStrategy.map( principalOf(type, name) ).getName();
    }

    public final String fh_get_ridentity = "get ridentity -group <pringipal>\n"+
            "\n"+
            "Get reverse identity mapping for provided id." +
            "  -group  provided id represents a group id." +
            "\nExample:"+
            "  get ridentity -group 100";
    public final String hh_get_ridentity = "<principal>";
    public String ac_get_ridentity_$_1(Args args)
            throws CacheException {
        String id = args.argv(0);
        boolean isGroup = args.getOpt("group") != null;

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
        Class c;
        try {
            c = Class.forName(type);
        }catch(ClassNotFoundException e) {
            c = Class.forName(PREFIX + type);
        }

        Constructor constructor = c.getConstructor(String.class);
        return (Principal)constructor.newInstance(name);
    }

}