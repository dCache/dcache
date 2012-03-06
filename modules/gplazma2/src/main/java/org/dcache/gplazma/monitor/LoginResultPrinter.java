package org.dcache.gplazma.monitor;

import org.dcache.gplazma.monitor.LoginResult.PhaseResult;
import org.dcache.gplazma.monitor.LoginResult.AuthPluginResult;
import org.dcache.gplazma.monitor.LoginResult.AuthPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.MapPluginResult;
import org.dcache.gplazma.monitor.LoginResult.MapPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.AccountPluginResult;
import org.dcache.gplazma.monitor.LoginResult.AccountPhaseResult;
import org.dcache.gplazma.monitor.LoginResult.SessionPhaseResult;
import java.security.Principal;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.dcache.gplazma.configuration.ConfigurationItemControl;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.OPTIONAL;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.SUFFICIENT;
import static org.dcache.gplazma.configuration.ConfigurationItemControl.REQUISITE;
import org.dcache.gplazma.monitor.LoginMonitor.Result;
import org.dcache.gplazma.monitor.LoginResult.PAMPluginResult;
import org.dcache.gplazma.monitor.LoginResult.SessionPluginResult;
import org.dcache.gplazma.monitor.LoginResult.SetDiff;
import static org.dcache.gplazma.monitor.LoginMonitor.Result.FAIL;
import static org.dcache.gplazma.monitor.LoginMonitor.Result.SUCCESS;

/**
 * This class takes a LoginResult and provides an ASCII-art description
 * of the process.
 */
public class LoginResultPrinter
{
    private static final EnumSet<ConfigurationItemControl> ALWAYS_OK =
            EnumSet.of(OPTIONAL, SUFFICIENT);

    private final LoginResult _result;
    private StringBuilder _sb;

    public LoginResultPrinter(LoginResult result)
    {
        _result = result;
    }

    public String print()
    {
        _sb = new StringBuilder();
        printInitialPart();
        printPhase(_result.getAuthPhase());
        printPhase(_result.getMapPhase());
        printPhase(_result.getAccountPhase());
        printPhase(_result.getSessionPhase());
        printValidation();
        return _sb.toString();
    }


    private void printInitialPart()
    {
        Result result = getOverallResult();
        _sb.append("LOGIN ").append(stringFor(result)).append("\n");

        printPrincipals(" in", initialPrincipals());
        printPrincipals("out", finalPrincipals());

        _sb.append(" |\n");
    }


    private void printPrincipals(String label, Set<Principal> principals)
    {
        boolean isFirst = true;

        for(Principal principal : principals) {
            _sb.append(" |   ");

            if(isFirst) {
                _sb.append(label).append(": ");
            } else {
                _sb.append("     ");
            }
            _sb.append(principal).append("\n");
            isFirst = false;
        }
    }

    private Set<Principal> initialPrincipals()
    {
        Set<Principal> principal;

        AuthPhaseResult auth = _result.getAuthPhase();
        if(auth.hasHappened()) {
            principal = auth.getPrincipals().getBefore();
        } else {
            principal = Collections.emptySet();
        }

        return principal;
    }

    private Set<Principal> finalPrincipals()
    {
        SessionPhaseResult session = _result.getSessionPhase();
        Set<Principal> principals;
        if(session.hasHappened()) {
            principals = session.getPrincipals().getAfter();
        } else {
            principals = Collections.emptySet();
        }

        return principals;
    }


    private <T extends PAMPluginResult> void printPhase(PhaseResult<T> result)
    {
        if(result.hasHappened()) {
            _sb.append(String.format(" +--%s %s\n", result.getName(),
                    stringFor(result.getResult())));
            printPrincipalsDiff(" |   |  ", result.getPrincipals());

            int count = result.getPluginResults().size();

            if(count > 0) {
                _sb.append(" |   |\n");

                int index = 1;
                for(T plugin : result.getPluginResults()) {
                    boolean isLast = index == count;
                    printPlugin(plugin, isLast);
                    index++;
                }
            }

            _sb.append(" |\n");

        } else {
            _sb.append(" +--(").append(result.getName()).append(") skipped\n");
            _sb.append(" |\n");
        }
    }

    private void printPlugin(PAMPluginResult result, boolean isLast)
    {
        printPluginHeader(result);

        if(result instanceof AuthPluginResult) {
            printPluginBehaviour((AuthPluginResult) result, isLast);
        } else if(result instanceof MapPluginResult) {
            printPluginBehaviour((MapPluginResult) result, isLast);
        } else if(result instanceof AccountPluginResult) {
            printPluginBehaviour((AccountPluginResult) result, isLast);
        } else if(result instanceof SessionPluginResult) {
            printPluginBehaviour((SessionPluginResult) result, isLast);
        } else {
            throw new IllegalArgumentException("unknown type of plugin " +
                    "result: " + result.getClass().getCanonicalName());
        }

        if(!isLast) {
            _sb.append(" |   |\n");
        }
    }


    private void printPluginBehaviour(AuthPluginResult plugin, boolean isLast)
    {
        String prefix = isLast ? " |        " : " |   |    ";
        printPrincipalsDiff(prefix, plugin.getIdentified());
    }

    private void printPluginBehaviour(MapPluginResult plugin, boolean isLast)
    {
        String prefix = isLast ? " |        " : " |   |    ";
        printPrincipalsDiff(prefix, plugin.getIdentified());
        printPrincipalsDiff(prefix, plugin.getAuthorized());
    }

    private void printPluginBehaviour(AccountPluginResult plugin, boolean isLast)
    {
        String prefix = isLast ? " |        " : " |   |    ";
        printPrincipalsDiff(prefix, plugin.getAuthorized());
    }

    private void printPluginBehaviour(SessionPluginResult plugin, boolean isLast)
    {
        String prefix = isLast ? " |        " : " |   |    ";
        printPrincipalsDiff(prefix, plugin.getAuthorized());
    }

    private void printValidation()
    {
        if(_result.hasValidationHappened()) {
            Result result = _result.getValidationResult();
            String label = stringFor(_result.getValidationResult());
            _sb.append(" +--VALIDATION ").append(label);
            if(result == Result.FAIL) {
                _sb.append(" (").append(_result.getValidationError()).append(")");
            }
            _sb.append('\n');
        } else {
            _sb.append(" +--(VALIDATION) skipped\n");
        }
    }


    private void printPluginHeader(PAMPluginResult plugin)
    {
        ConfigurationItemControl control = plugin.getControl();
        Result result = plugin.getResult();
        String resultLabel = stringFor(result);
        String name = plugin.getName();
        String error;
        if(result == SUCCESS) {
            error = "";
        } else {
            error = " (" + plugin.getError() + ")";
        }
        _sb.append(String.format(" |   +--%s %s:%s%s => %s", name,
                plugin.getControl().name(), resultLabel, error,
                ALWAYS_OK.contains(control) ? "OK" : resultLabel));

        if((result == SUCCESS && control == SUFFICIENT) ||
                (result == FAIL && control == REQUISITE)) {
            _sb.append(" (ends the phase)");
        }

        _sb.append('\n');
    }


    private Result getOverallResult()
    {
        AuthPhaseResult auth = _result.getAuthPhase();
        MapPhaseResult map = _result.getMapPhase();
        AccountPhaseResult account = _result.getAccountPhase();
        SessionPhaseResult session = _result.getSessionPhase();

        boolean success = auth.hasHappened() && auth.getResult() == SUCCESS &&
                map.hasHappened() && map.getResult() == SUCCESS &&
                account.hasHappened() && account.getResult() == SUCCESS &&
                session.hasHappened() && session.getResult() == SUCCESS &&
                _result.getValidationResult() == SUCCESS;

        return success ? SUCCESS : FAIL;
    }


    private String stringFor(Result result)
    {
        return result == SUCCESS ? "OK" : "FAIL";
    }

    private void printPrincipalsDiff(String prefix, SetDiff<Principal> diff)
    {
        if(diff == null) {
            return;
        }

        Set<Principal> added = diff.getAdded();

        boolean isFirst = true;
        for(Principal principal : added) {
            if(isFirst) {
                _sb.append(prefix).append("  added: ");
                isFirst = false;
            } else {
                _sb.append(prefix).append("         ");
            }
            _sb.append(principal).append('\n');
        }

        Set<Principal> removed = diff.getRemoved();

        isFirst = true;
        for(Principal principal : removed) {
            if(isFirst) {
                _sb.append(prefix).append("removed: ");
                isFirst = false;
            } else {
                _sb.append(prefix).append("         ");
            }
            _sb.append(principal).append('\n');
        }
    }

}
