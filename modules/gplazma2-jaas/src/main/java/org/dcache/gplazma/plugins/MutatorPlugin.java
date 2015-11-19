package org.dcache.gplazma.plugins;

import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.UserNamePrincipal;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.filter;

/**
 * A {@link GPlazmaMappingPlugin} that maps a one principal type to an other one.
 * This is required to convert equivalent principals into principals provided by
 * {@code org.dcache.auth} package.
 * NOTICE, this plugin modifies the original set of principals.
 */
public class MutatorPlugin implements GPlazmaMappingPlugin {

    private final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
    static final String IN_OPTION = "gplazma.mutator.accept";
    static final String OUT_OPTION = "gplazma.mutator.produce";
    private final Class<? extends Principal> inPrincipal;
    private final Class<? extends Principal> outPrincipal;
    private Constructor<? extends Principal> outConstructor;

    public MutatorPlugin(Properties properties) throws ClassNotFoundException, NoSuchMethodException {
        String inClass = properties.getProperty(IN_OPTION);
        String outClass = properties.getProperty(OUT_OPTION);

        checkArgument(inClass != null, "Undefined property: " + IN_OPTION);
        checkArgument(outClass != null, "Undefined property: " + OUT_OPTION);

        Class<?> principal = classLoader.loadClass(inClass);
        checkArgument(Principal.class.isAssignableFrom(principal), inClass + " is not a Principal");
        inPrincipal = (Class<? extends Principal>) principal;

        outPrincipal = classOf(outClass);
        outConstructor = outPrincipal.getConstructor(String.class);
    }

    @Override
    public void map(Set<Principal> principals) {
        Set<Principal> mutated = new HashSet<>();
        for (Principal p : filter(principals, inPrincipal)) {
            try {
                Principal out = outConstructor.newInstance(p.getName());
                mutated.add(out);
            } catch (IllegalAccessException | IllegalArgumentException |
                    InstantiationException | InvocationTargetException ignored) {
            }
        }
        principals.addAll(mutated);
    }

    public static Class<? extends Principal> classOf(String type) {

        switch (type) {
            case "dn":
                return GlobusPrincipal.class;
            case "kerberos":
                return KerberosPrincipal.class;
            case "fqan":
                return FQANPrincipal.class;
            case "name":
                return LoginNamePrincipal.class;
            case "username":
                return UserNamePrincipal.class;
            default:
                throw new IllegalArgumentException("unknown type: " + type);
        }
    }

}
