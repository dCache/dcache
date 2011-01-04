package org.dcache.services.login;

import javax.security.auth.Subject;

import org.globus.gsi.jaas.GlobusPrincipal;

import org.dcache.cells.CellCommandListener;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.LoginStrategy;
import org.dcache.auth.LoginReply;
import diskCacheV111.util.CacheException;

import dmg.util.Args;

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
}