package org.dcache.gplazma.pyscript;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
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
    private List<Path> pluginPathList = new ArrayList<>();

    public PyscriptPlugin(Properties properties) {
        // constructor
        try {
            pluginPathList = listDirectoryContents("gplazma2-pyscript");  // TODO fix this, currently just reads from local folder
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

                // Only handle select datatypes
                for (Object pubCred : publicCredentials) {
                    if (
                            pubCred instanceof String
                            || pubCred instanceof Number // all number datatypes
                    ) {
                        pyPublicCredentials.add(pubCred);
                    } else {
                        LOGGER.warn(
                            "gplazma2-pyscript: Public Credential {} dropped. Use String or Number.",
                            pubCred
                        );
                    }
                }
                for (Object privCred : privateCredentials) {
                    if (
                        privCred instanceof String
                        || privCred instanceof Number // all number datatypes
                    ) {
                        pyPrivateCredentials.add(privCred);
                    } else {
                        LOGGER.warn(
                            "gplazma2-pyscript: Private Credential {} dropped. Use String or Number.",
                            privCred.toString()
                        );
                    }
                }

                // Convert principals into string representation
                String principalsString = Subjects.toString(
                    Subjects.ofPrincipals(principals)
                );
                String[] principalStringList = principalsString.substring(
                    1, principalsString.length() - 1
                ).split(", ");
                pyPrincipals.addAll(Arrays.asList(principalStringList));



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
                List<String> convertedPyPrincipals = List.copyOf(pyPrincipals);

                // Update original Principals
                principals.addAll(Subjects.principalsFromArgs(convertedPyPrincipals));

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
