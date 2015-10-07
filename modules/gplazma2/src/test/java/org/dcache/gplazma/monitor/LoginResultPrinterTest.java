package org.dcache.gplazma.monitor;

import com.google.common.collect.Sets;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.monitor.LoginMonitor.Result;
import org.dcache.gplazma.monitor.LoginResult.AccountPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.AccountPluginResult;
import org.dcache.gplazma.monitor.LoginResult.AuthPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.AuthPluginResult;
import org.dcache.gplazma.monitor.LoginResult.MapPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.MapPluginResult;
import org.dcache.gplazma.monitor.LoginResult.SessionPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.SessionPluginResult;

import static com.google.common.collect.Iterables.concat;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.*;
import static org.dcache.gplazma.monitor.LoginMonitor.Result.FAIL;
import static org.dcache.gplazma.monitor.LoginMonitor.Result.SUCCESS;

public class LoginResultPrinterTest
{
    // Allow printing of resulting tree
    private static final boolean IS_OUTPUT_STDOUT_ENABLED = true;
//            "true".equals(System.getenv("DCACHE_UNITTEST_PRINT_ENABLED"));

    private static final Set<Principal> NO_PRINCIPALS = Collections.emptySet();

    private static final Principal DN =
            new GlobusPrincipal("/C=DE/O=GermanGrid/OU=DESY/CN=Alexander Paul Millar");

    private static final Principal ATLAS = new FQANPrincipal("/atlas");
    private static final Principal ATLAS_PROD =
            new FQANPrincipal("/atlas/Role=production", true);
    private static final Principal UID = new UidPrincipal(1000);
    private static final Principal GID_ATLAS_PROD = new GidPrincipal(5000, false);
    private static final Principal NAME_ATLAS_PROD
            = new GroupNamePrincipal("atlas-prod");

    private static final HomeDirectory HOMEDIR = new HomeDirectory("/home/paul");
    private static final RootDirectory ROOTDIR = new RootDirectory("/");
    private static final Restriction POLICY = Restrictions.none();

    @Rule
    public TestName name = new TestName();

    LoginResult _result;
    LoginResultPrinter _printer;

    @Before
    public void setup()
    {
        _result = new LoginResult();
        _printer = new LoginResultPrinter(_result);
    }

    @Test
    public void shouldWorkForEmptyResult()
    {
        print();
    }

    @Test
    public void shouldWorkForFailingAuth()
    {
        givenAuthPhaseWith(FAIL);
        givenAuthPhaseRuns(authPlugin("foo", REQUIRED, FAIL, "bar needed"));
        givenValidation(FAIL, "missing uid");

        print();
    }

    @Test
    public void shouldWorkForSuccessfulLogin()
    {
        Set<Principal> identified = principals(DN, ATLAS, ATLAS_PROD);
        Set<Principal> authorized = principals(DN, ATLAS, ATLAS_PROD, UID,
                NAME_ATLAS_PROD, GID_ATLAS_PROD);

        givenAuthPhaseWith(SUCCESS, NO_PRINCIPALS,
                principals(DN, ATLAS, ATLAS_PROD));

        givenAuthPhaseRuns(
                authPlugin("x509", OPTIONAL, SUCCESS, null,
                        NO_PRINCIPALS, principals(DN)),
                authPlugin("voms", OPTIONAL, SUCCESS, null,
                        principals(DN), identified));


        givenMapPhaseWith(SUCCESS, identified, authorized);

        givenMapPhaseRuns(
                mapPlugin("kerberos", OPTIONAL, FAIL,
                        "no kerberos principal found", identified, identified),
                mapPlugin("vorolemap", OPTIONAL, SUCCESS, null, identified,
                        concat(identified, principals(NAME_ATLAS_PROD))),
                mapPlugin("authzdb", SUFFICIENT, SUCCESS, null,
                        concat(identified, principals(NAME_ATLAS_PROD)),
                        concat(identified, principals(NAME_ATLAS_PROD, UID, GID_ATLAS_PROD))));

        givenAccountPhaseWith(SUCCESS, authorized, authorized);

        givenAccountPhaseRuns(
                accountPlugin("argus", REQUIRED, SUCCESS,
                        authorized, authorized)
                );

        Set<Object> attributes = attributes(HOMEDIR, ROOTDIR, POLICY);

        givenSessionPhaseWith(SUCCESS, authorized, authorized, attributes);

        givenSessionPhaseRuns(
                sessionPlugin("authzdb", OPTIONAL, SUCCESS, authorized,
                        authorized, attributes)
                );

        givenValidation(SUCCESS, null);

        print();
    }


    public void print()
    {
        String output = _printer.print();
        if(IS_OUTPUT_STDOUT_ENABLED) {
            System.out.println("\n\nFrom test " + name.getMethodName()+"\n");
            System.out.append(output);
        }
    }

    /*
     * AUTH support
     */

    void givenAuthPhaseWith(Result result)
    {
        givenAuthPhaseWith(result, NO_PRINCIPALS, NO_PRINCIPALS);
    }

    void givenAuthPhaseWith(Result result, Set<Principal> before,
            Set<Principal> after)
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        auth.setResult(result);
        auth.setPrincipals(before, after);
    }

    void givenAuthPhaseRuns(AuthPluginResult... results)
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        for(AuthPluginResult result : results) {
            auth.addPluginResult(result);
        }
    }

    AuthPluginResult authPlugin(String name, ConfigurationItemControl control,
            Result result, String error)
    {
        return authPlugin(name, control, result, error, NO_PRINCIPALS,
                NO_PRINCIPALS);
    }

    AuthPluginResult authPlugin(String name, ConfigurationItemControl control,
            Result result, String error, Set<Principal> before,
            Set<Principal> after)
    {
        AuthPluginResult plugin = new AuthPluginResult(name, control);
        plugin.setResult(result);
        if(result == Result.FAIL) {
            plugin.setError(error);
        }
        plugin.setIdentified(before, after);
        return plugin;
    }

    /*
     * MAP support
     */

    void givenMapPhaseWith(Result result, Set<Principal> before,
            Set<Principal> after)
    {
        MapPhaseResult map = _result.getMapPhase();
        map.setResult(result);
        map.setPrincipals(before, after);
    }

    void givenMapPhaseRuns(MapPluginResult... results)
    {
        MapPhaseResult map = _result.getMapPhase();
        for(MapPluginResult result : results) {
            map.addPluginResult(result);
        }
    }

    MapPluginResult mapPlugin(String name, ConfigurationItemControl control,
            Result result, String error, Iterable<Principal> principalsBefore,
            Iterable<Principal> principalsAfter)
    {
        MapPluginResult plugin = new MapPluginResult(name, control);
        plugin.setPrincipals(principalsBefore, principalsAfter);
        plugin.setResult(result);
        if(result == Result.FAIL) {
            plugin.setError(error);
        }
        return plugin;
    }

    /*
     * ACCOUNT support
     */

    void givenAccountPhaseWith(Result result, Set<Principal> before,
            Set<Principal> after)
    {
        AccountPhaseResult account = _result.getAccountPhase();
        account.setResult(result);
        account.setPrincipals(before, after);
    }


    void givenAccountPhaseRuns(AccountPluginResult... results)
    {
        AccountPhaseResult account = _result.getAccountPhase();
        for(AccountPluginResult result : results) {
            account.addPluginResult(result);
        }
    }

    AccountPluginResult accountPlugin(String name,
            ConfigurationItemControl control, Result result,
            Set<Principal> before, Set<Principal> after)
    {
        AccountPluginResult plugin = new AccountPluginResult(name, control);
        plugin.setAuthorized(before, after);
        plugin.setResult(result);
        return plugin;
    }

    /*
     * SESSION support
     */

    void givenSessionPhaseWith(Result result, Set<Principal> before,
            Set<Principal> after, Set<Object> attributes)
    {
        SessionPhaseResult session = _result.getSessionPhase();
        session.setResult(result);
        session.setPrincipals(before, after);
        session.setAttributes(attributes);
    }

    void givenSessionPhaseRuns(SessionPluginResult... results)
    {
        SessionPhaseResult session = _result.getSessionPhase();
        for(SessionPluginResult result : results) {
            session.addPluginResult(result);
        }
    }

    SessionPluginResult sessionPlugin(String name,
            ConfigurationItemControl control, Result result,
            Set<Principal> before, Set<Principal> after,
            Set<Object> attributes)
    {
        SessionPluginResult plugin = new SessionPluginResult(name, control);
        plugin.setResult(result);
        plugin.setAuthorized(before, after);
        plugin.setAttributes(attributes);
        return plugin;
    }

    void givenValidation(Result result, String error)
    {
        _result.setValidationResult(result);
        if(result == Result.FAIL) {
            _result.setValidationError(error);
        }
    }

    static Set<Principal> principals(Principal... principals)
    {
        return Sets.newHashSet(principals);
    }

    static Set<Object> attributes(Object... attributes)
    {
        return Sets.newHashSet(attributes);
    }
}
