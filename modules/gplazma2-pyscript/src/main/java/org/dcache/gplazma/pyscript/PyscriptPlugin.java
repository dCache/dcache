package org.dcache.gplazma.pyscript;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.*;
import java.util.stream.Collectors;

import org.dcache.auth.Subjects;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.plugins.GPlazmaAuthenticationPlugin;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.PySet;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PyscriptPlugin implements GPlazmaAuthenticationPlugin {

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
                LOGGER.debug("gplazma2-pyscript running {}", pluginPath);
                String content = Files.lines(pluginPath).collect(
                    Collectors.joining("\n")
                );
                // execute file content, loading the function "py_auth" into the namespace
                PI.exec(content);

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
                if (!principals.isEmpty()) {
                    String principalsString = Subjects.toString(
                            Subjects.ofPrincipals(principals)
                    );
                    String[] principalStringList = principalsString.substring(
                            1, principalsString.length() - 1
                    ).split(", ");
                    pyPrincipals.addAll(Arrays.asList(principalStringList));
                }

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
                        "Authentication failed in file " + pluginPath.getFileName().toString()
                    );
                }
                // ================
                // update principals
                // =================
                // Convert principals back from string representation
                if (!pyPrincipals.isEmpty()) {
                    List<String> convertedPyPrincipals = List.copyOf(pyPrincipals);
                    // Update original Principals
                    principals.addAll(Subjects.principalsFromArgs(convertedPyPrincipals));
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

}
