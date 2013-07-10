package org.dcache.gplazma.monitor;

import java.security.Principal;
import java.util.HashSet;
import java.util.Set;

import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.monitor.LoginResult.AccountPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.AccountPluginResult;
import org.dcache.gplazma.monitor.LoginResult.AuthPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.AuthPluginResult;
import org.dcache.gplazma.monitor.LoginResult.MapPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.MapPluginResult;
import org.dcache.gplazma.monitor.LoginResult.SessionPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.SessionPluginResult;

/**
 * This class records the result of a gPlazma login run by implementing the
 * LoginMonitor interface.
 */
public class RecordingLoginMonitor implements LoginMonitor
{
    LoginResult _result = new LoginResult();

    /*
     * The following sets only have significance while processing a phase
     * or plug
     */
    private Set<Principal> _atPhaseStartPrincipals;
    private Set<Principal> _principals;
    private Set<Object> _publicCredentials;
    private Set<Object> _privateCredentials;

    @Override
    public void authBegins(Set<Object> publicCredentials,
            Set<Object> privateCredentials, Set<Principal> principals)
    {
        _atPhaseStartPrincipals = new HashSet<>(principals);
        AuthPhaseResult auth = _result.getAuthPhase();
        auth.setPublicCredentials(publicCredentials);
        auth.setPrivateCredentials(privateCredentials);
    }

    @Override
    public void authPluginBegins(String name, ConfigurationItemControl control,
            Set<Object> publicCredentials, Set<Object> privateCredentials,
            Set<Principal> principals)
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        auth.addPluginResult(new AuthPluginResult(name, control));
        _principals = new HashSet<>(principals);
        _publicCredentials = new HashSet<>(publicCredentials);
        _privateCredentials = new HashSet<>(privateCredentials);
    }

    @Override
    public void authPluginEnds(String name, ConfigurationItemControl control,
            Result result, String error, Set<Object> publicCredentials,
            Set<Object> privateCredentials, Set<Principal> principals)
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        AuthPluginResult plugin = auth.getLastPlugin();
        plugin.setIdentified(_principals, principals);
        plugin.setPublicCredentials(_publicCredentials, publicCredentials);
        plugin.setPrivateCredentials(_privateCredentials, privateCredentials);
        plugin.setResult(result);
        if(result == Result.FAIL) {
            plugin.setError(error);
        }
    }

    @Override
    public void authEnds(Set<Principal> principals, Result result)
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        auth.setPrincipals(_atPhaseStartPrincipals, principals);
        auth.setResult(result);
    }

    @Override
    public void mapBegins(Set<Principal> principals)
    {
        _atPhaseStartPrincipals = new HashSet<>(principals);
    }

    @Override
    public void mapPluginBegins(String name, ConfigurationItemControl control,
            Set<Principal> principals)
    {
        MapPhaseResult map = _result.getMapPhase();
        map.addPluginResult(new MapPluginResult(name, control));
        _principals = new HashSet<>(principals);
    }

    @Override
    public void mapPluginEnds(String name, ConfigurationItemControl control,
            Result result, String error, Set<Principal> principals)
    {
        MapPhaseResult map = _result.getMapPhase();
        MapPluginResult plugin = map.getLastPlugin();
        plugin.setResult(result);
        plugin.setPrincipals(_principals, principals);
        if(result == Result.FAIL) {
            plugin.setError(error);
        }
    }

    @Override
    public void mapEnds(Set<Principal> principals, Result result)
    {
        MapPhaseResult map = _result.getMapPhase();
        map.setPrincipals(_atPhaseStartPrincipals, principals);
        map.setResult(result);
    }

    @Override
    public void accountBegins(Set<Principal> principals)
    {
        _atPhaseStartPrincipals = new HashSet<>(principals);
    }

    @Override
    public void accountPluginBegins(String name,
            ConfigurationItemControl control, Set<Principal> principals)
    {
        AccountPhaseResult account = _result.getAccountPhase();
        account.addPluginResult(new AccountPluginResult(name, control));
        _principals = new HashSet<>(principals);
    }

    @Override
    public void accountPluginEnds(String name,
            ConfigurationItemControl control, Result result, String error,
            Set<Principal> principals)
    {
        AccountPhaseResult account = _result.getAccountPhase();
        AccountPluginResult plugin = account.getLastPlugin();
        plugin.setAuthorized(_principals, principals);
        plugin.setResult(result);
        if(result == Result.FAIL) {
            plugin.setError(error);
        }
    }

    @Override
    public void accountEnds(Set<Principal> principals, Result result)
    {
        AccountPhaseResult account = _result.getAccountPhase();
        account.setPrincipals(_atPhaseStartPrincipals, principals);
        account.setResult(result);
    }

    @Override
    public void sessionBegins(Set<Principal> principals)
    {
        _atPhaseStartPrincipals = new HashSet<>(principals);
    }

    @Override
    public void sessionPluginBegins(String name,
            ConfigurationItemControl control, Set<Principal> principals,
            Set<Object> attributes)
    {
        SessionPhaseResult session = _result.getSessionPhase();
        session.addPluginResult(new SessionPluginResult(name, control));
        _principals = new HashSet<>(principals);
    }

    @Override
    public void sessionPluginEnds(String name,
            ConfigurationItemControl control, Result result, String error,
            Set<Principal> principals, Set<Object> attributes)
    {
        SessionPhaseResult session = _result.getSessionPhase();
        SessionPluginResult plugin = session.getLastPlugin();
        plugin.setAuthorized(_principals, principals);
        plugin.setAttributes(attributes);
        plugin.setResult(result);
        if(result == Result.FAIL) {
            plugin.setError(error);
        }
    }

    @Override
    public void sessionEnds(Set<Principal> principals, Set<Object> attributes,
            Result result)
    {
        SessionPhaseResult session = _result.getSessionPhase();
        session.setPrincipals(_atPhaseStartPrincipals, principals);
        session.setResult(result);
        session.setAttributes(attributes);
    }

    @Override
    public void validationResult(Result result, String error)
    {
        _result.setValidationResult(result);
        if(result == Result.FAIL) {
            _result.setValidationError(error);
        }
    }

    public LoginResult getResult()
    {
        return _result;
    }
}
