package org.dcache.gplazma.monitor;

import com.google.common.collect.Sets;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.dcache.gplazma.configuration.ConfigurationItemControl;
import org.dcache.gplazma.monitor.LoginMonitor.Result;

import static com.google.common.base.Preconditions.checkState;

/**
 * This class holds a detailed report of the activity when gPlazma processes
 * a login request.
 */
public class LoginResult
{
    private final AuthPhaseResult _authPhase = new AuthPhaseResult();
    private final MapPhaseResult _mapPhase = new MapPhaseResult();
    private final AccountPhaseResult _accountPhase = new AccountPhaseResult();
    private final SessionPhaseResult _sessionPhase = new SessionPhaseResult();
    private Result _validationResult;
    private String _validationError = "unknown";
    private boolean _validationHasHappened;

    public AuthPhaseResult getAuthPhase()
    {
        return _authPhase;
    }

    public MapPhaseResult getMapPhase()
    {
        return _mapPhase;
    }

    public AccountPhaseResult getAccountPhase()
    {
        return _accountPhase;
    }

    public SessionPhaseResult getSessionPhase()
    {
        return _sessionPhase;
    }

    public void setValidationResult(Result result)
    {
        _validationHasHappened = true;
        _validationResult = result;
    }

    public Result getValidationResult()
    {
        return _validationResult;
    }

    public boolean hasValidationHappened()
    {
        return _validationHasHappened;
    }

    public void setValidationError(String error)
    {
        _validationError = error;
    }

    public String getValidationError()
    {
        checkState(_validationResult == Result.FAIL, "Validation error is "+
                "only valid if the plugin has failed");
        return _validationError;
    }

    /**
     * Returns whether gPlazma started processing the login request represent
     * by this LoginResult object.
     *
     * A login request may fail before the first phase was attempted.  This can
     * happen, for examples, if the gPlazma configuration is syntactically
     * wrong or one of the plugins fails when being instantiated (throws an
     * exception when being instantiated).
     */
    public boolean hasStarted()
    {
        return getAuthPhase().hasHappened();
    }

    /**
     * Returns whether gPlazma finished all four phases of the login process.
     *
     * This is not the same as the login being successful.  This value will
     * return true yet the login has failed if the session phase or the final
     * validation step fails.
     */
    public boolean hasCompleted()
    {
        return getSessionPhase().hasHappened();
    }

    /**
     * Base class for storing the result of running a phase.
     * @param <T> the class for storing the result of running a plugin
     */
    public static class PhaseResult<T extends PAMPluginResult>
    {
        private final List<T> _plugins = new ArrayList<>();
        private final String _name;
        private SetDiff<Principal> _principals;
        private Result _result;
        private boolean _hasHappened;

        public PhaseResult(String name)
        {
            _name = name;
        }

        public String getName()
        {
            return _name;
        }

        public void addPluginResult(T result)
        {
            _plugins.add(result);
        }

        public List<T> getPluginResults()
        {
            return _plugins;
        }

        public boolean hasPlugins()
        {
            return !_plugins.isEmpty();
        }

        public T getLastPlugin()
        {
            return _plugins.get(_plugins.size() - 1);
        }

        public void setPrincipals(Set<Principal> before, Set<Principal> after)
        {
            _principals = new SetDiff<>(before, after);
        }

        public SetDiff<Principal> getPrincipals()
        {
            return _principals;
        }

        public void setResult(Result result)
        {
            _result = result;
            _hasHappened = true;
        }

        public Result getResult()
        {
            return _result;
        }

        public boolean hasHappened()
        {
            return _hasHappened;
        }
    }

    /**
     * Class that stores the result of the AUTH phase.
     */
    public static class AuthPhaseResult extends PhaseResult<AuthPluginResult>
    {
        private final Set<Object> _publicCredentials = new HashSet<>();
        private final Set<Object> _privateCredentials = new HashSet<>();

        public AuthPhaseResult()
        {
            super("AUTH");
        }

        public void setPublicCredentials(Set<Object> publicCredentials)
        {
            _publicCredentials.clear();
            _publicCredentials.addAll(publicCredentials);
        }

        public void setPrivateCredentials(Set<Object> privateCredentials)
        {
            _privateCredentials.clear();
            _privateCredentials.addAll(privateCredentials);
        }

        public Set<Object> getPublicCredentials()
        {
            return _publicCredentials;
        }

        public Set<Object> getPrivateCredentials()
        {
            return _privateCredentials;
        }
    }

    /**
     * Class that stores the result of the MAP phase
     */
    public static class MapPhaseResult extends PhaseResult<MapPluginResult>
    {
        public MapPhaseResult()
        {
            super("MAP");
        }
    }

    /**
     * Class that stores the result of the ACCOUNT phase.
     */
    public static class AccountPhaseResult
            extends PhaseResult<AccountPluginResult>
    {
        public AccountPhaseResult()
        {
            super("ACCOUNT");
        }
    }

    /**
     * Class that stores the result of the SESSION phase
     */
    public static class SessionPhaseResult
            extends PhaseResult<SessionPluginResult>
    {
        private final Set<Object> _attributes = new HashSet<>();
        public SessionPhaseResult()
        {
            super("SESSION");
        }

        void setAttributes(Set<Object> attributes)
        {
            _attributes.clear();
            _attributes.addAll(attributes);
        }

        public Set<Object> getAttributes()
        {
            return _attributes;
        }
    }

    /**
     * Store two sets (with same Generics interface) and allow discovery of
     * how the objects has changed.
     */
    public static class SetDiff<T>
    {
        private final Set<T> _before;
        private final Set<T> _after;

        SetDiff(Iterable<T> before, Iterable<T> after)
        {
            _before = Sets.newHashSet(before);
            _after = Sets.newHashSet(after);
        }

        public Set<T> getBefore()
        {
            return _before;
        }

        public Set<T> getAfter()
        {
            return _after;
        }

        public Set<T> getAdded()
        {
            Set<T> added = new HashSet<>(_after);
            added.removeAll(_before);
            return added;
        }

        public Set<T> getRemoved()
        {
            Set<T> removed = new HashSet<>(_before);
            removed.removeAll(_after);
            return removed;
        }
    }

    /**
     * Class for holding the result of running a PAM-style plugin,
     * irrespective of in which phase the plugin was run.  This serves as a
     * base class for phase-specific result classes
     *
     * If a plugin fails then there should be an associated error message.
     */
    public static class PAMPluginResult
    {
        private final String _name;
        private final ConfigurationItemControl _control;
        private Result _result;
        private String _error = "unknown";

        PAMPluginResult(String name, ConfigurationItemControl control)
        {
            _name = name;
            _control = control;
        }

        void setResult(Result result)
        {
            _result = result;
        }

        void setError(String error)
        {
            _error = error;
        }

        public String getName()
        {
            return _name;
        }

        public ConfigurationItemControl getControl()
        {
            return _control;
        }

        public Result getResult()
        {
            return _result;
        }

        /**
         * Provide the error message the plugin supplied if a call fails.
         */
        public String getError()
        {
            checkState(_result == Result.FAIL, "error message only available "
                    + "if the plugin failed");
            return _error;
        }
    }

    /**
     * Store the result of an AUTH plugin
     */
    public static class AuthPluginResult extends PAMPluginResult
    {
        private SetDiff<Principal> _identified;
        private SetDiff<Object> _publicCredentials;
        private SetDiff<Object> _privateCredentials;

        AuthPluginResult(String name, ConfigurationItemControl control)
        {
            super(name, control);
        }

        public void setIdentified(Set<Principal> before, Set<Principal> after)
        {
            _identified = new SetDiff<>(before, after);
        }

        public void setPublicCredentials(Set<Object> before, Set<Object> after)
        {
            _publicCredentials = new SetDiff<>(before, after);
        }

        public void setPrivateCredentials(Set<Object> before, Set<Object> after)
        {
            _privateCredentials = new SetDiff<>(before, after);
        }

        public SetDiff<Principal> getIdentified()
        {
            return _identified;
        }

        public SetDiff<Object> getPublicCredentials()
        {
            return _publicCredentials;
        }

        public SetDiff<Object> getPrivateCredentials()
        {
            return _privateCredentials;
        }
    }

    /**
     * Store the result of running a MAP plugin
     */
    public static class MapPluginResult extends PAMPluginResult
    {
        private SetDiff<Principal> _principals;

        MapPluginResult(String name, ConfigurationItemControl control)
        {
            super(name, control);
        }

        public void setPrincipals(Iterable<Principal> before, Iterable<Principal> after)
        {
            _principals = new SetDiff<>(before, after);
        }

        public SetDiff<Principal> getPrincipals()
        {
            return _principals;
        }
    }


    /**
     * Store the result of running an ACCOUNT plugin
     */
    public static class AccountPluginResult extends PAMPluginResult
    {
        private SetDiff<Principal> _authorized;

        AccountPluginResult(String name, ConfigurationItemControl control)
        {
            super(name, control);
        }

        void setAuthorized(Set<Principal> before, Set<Principal> after)
        {
            _authorized = new SetDiff<>(before, after);
        }

        public SetDiff<Principal> getAuthorized()
        {
            return _authorized;
        }
    }

    /**
     * Store the result of running a SESSION plugin.
     */
    public static class SessionPluginResult extends PAMPluginResult
    {
        private SetDiff<Principal> _principals;
        private Set<Object> _attributes;

        SessionPluginResult(String name, ConfigurationItemControl control)
        {
            super(name, control);
        }

        public void setAuthorized(Set<Principal> before, Set<Principal> after)
        {
            _principals = new SetDiff<>(before, after);
        }

        public SetDiff<Principal> getAuthorized()
        {
            return _principals;
        }

        public void setAttributes(Set<Object> attributes)
        {
            _attributes = new HashSet<>(attributes);
        }

        public Set<Object> getAttributes()
        {
            return _attributes;
        }
    }
}
