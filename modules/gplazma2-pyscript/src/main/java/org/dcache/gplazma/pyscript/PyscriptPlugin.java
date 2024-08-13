package org.dcache.gplazma.pyscript;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Principal;
import java.util.*;
import java.util.stream.Collectors;

import org.dcache.auth.Subjects;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.dcache.gplazma.plugins.GPlazmaMappingPlugin;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PySet;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PyscriptPlugin implements GPlazmaAuthenticationPlugin, GPlazmaMappingPlugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(PyscriptPlugin.class);

    private final PythonInterpreter PI = new PythonInterpreter();
    private final String pluginDirectoryString;
    private List<Path> pluginPathList = new ArrayList<>();

    public PyscriptPlugin(Properties properties) {
        // constructor
        pluginDirectoryString = properties.getProperty("gplazma.pyscript.workdir");
        try {
            pluginPathList = listDirectoryContents(pluginDirectoryString);
        } catch (IOException e) {
            LOGGER.error("Error reading Pyscript directory");
        }
    }

    private static List<Path> listDirectoryContents(String directoryPath) throws IOException {
        return Files.list(Path.of(directoryPath)).toList();
    }

    private void executePythonFile(Path path) throws IOException {
        String content = Files.lines(path).collect(
                Collectors.joining("\n")
        );
        // execute file content, loading the function "py_auth" into the namespace
        PI.exec(content);
    }

    private static Set<Principal> stringListToPrincipalSet(List<String> convertedprincipals) {
        // pyPrincipals is a set of strings
        return Subjects.principalsFromArgs(convertedprincipals);
    }

    @Override
    public void authenticate(
            Set<Object> publicCredentials,
            Set<Object> privateCredentials,
            Set<Principal> principals
    ) throws AuthenticationException {
        for (Path pluginPath : pluginPathList) {
            // for each path in pluginPathList
            try {
                // ===========
                // IO
                // ===========
                // read file into string
                LOGGER.debug("gplazma2-pyscript auth running {}", pluginPath);
                executePythonFile(pluginPath);

                // ======================
                // Prepare Python Data
                // ======================
                // prepare datatypes for python
                PySet pyPublicCredentials = new PySet();
                PySet pyPrivateCredentials = new PySet();
                PySet pyPrincipals = new PySet();

                // add credentials regardless of type -> type handling is done in Python!
                pyPublicCredentials.addAll(publicCredentials);
                pyPrivateCredentials.addAll(privateCredentials);

                // Convert principals into string representation
                pyPrincipals.addAll(Subjects.toStringList(Subjects.ofPrincipals(principals)));

                // =============
                // Run in Python
                // =============
                // Get function
                PyFunction pluginAuthFunc = PI.get(
                        "py_auth",
                        PyFunction.class
                );
                // Invoke function
                PyObject pyAuthOutput = pluginAuthFunc.__call__(
                        pyPublicCredentials,
                        pyPrivateCredentials,
                        pyPrincipals
                );
                // throw exception if needed
                Boolean AuthenticationPassed = (Boolean) pyAuthOutput.__tojava__(Boolean.class);
                if (!AuthenticationPassed) {
                    throw new AuthenticationException(
                        "Authentication failed in file " + pluginPath.getFileName()
                    );
                }
                // ================
                // update principals
                // =================
                // Convert principals back from string representation
                if (!pyPrincipals.isEmpty()) {
                    List<String> convertedPyPrincipals = List.copyOf(pyPrincipals);
                    // Update original Principals
                    principals.addAll(stringListToPrincipalSet(convertedPyPrincipals));
                }
            } catch (IOException e) {
                throw new AuthenticationException(
                    "Authentication failed due to I/O error: " + e.getMessage(), e
                );
            }
        }



        // finish authentication
        LOGGER.debug("Finished gplazma2-pyscript step.");
    }

    @Override
    public void map(
        Set<Principal> principals
    ) throws AuthenticationException {
        for (Path pluginPath : pluginPathList) {
            try {
                LOGGER.debug("gplazma2-pyscript map running {}", pluginPath);
                // read and execute file
                executePythonFile(pluginPath);
                // prepare credentials set
                PySet pyPrincipals = new PySet();
                pyPrincipals.addAll(Subjects.toStringList(Subjects.ofPrincipals(principals)));
                // run py_map function
                PyFunction pluginMapFunc = PI.get(
                        "py_map",
                        PyFunction.class
                );
                PyObject pyMapOutput = pluginMapFunc.__call__(
                        pyPrincipals
                );
                // possibly throw authenticationexception if mapping step defined in python doesnt work
                Boolean MappingPassed = (Boolean) pyMapOutput.__tojava__(Boolean.class);
                if (!MappingPassed) {
                    throw new AuthenticationException(
                            "Authentication failed in file " + pluginPath.getFileName()
                    );
                }
                // update principals
                if (!pyPrincipals.isEmpty()) {
                    principals.addAll(stringListToPrincipalSet(List.copyOf(pyPrincipals)));
                }
            } catch (IOException e) {
                throw new AuthenticationException("Authentication failed due to I/O error: " + e.getMessage(), e);
            }
        }
    }
}
