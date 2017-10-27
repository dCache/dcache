package org.dcache.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

public class Version
{
    private final Optional<String> _version;
    private final Optional<String> _buildTime;
    private final Optional<String> _buildNumber;
    private final Optional<String> _branch;

    public Version(Optional<String> buildTime, Optional<String> version,
                   Optional<String> buildNumber, Optional<String> branch)
    {
        _buildTime = buildTime;
        _buildNumber = buildNumber;
        _version = version;
        _branch = branch;
    }

    public static Version of(Class<?> clazz)
    {
        Optional<String> buildTime = Optional.empty();
        Optional<String> version = Optional.empty();
        Optional<String> buildNumber = Optional.empty();
        Optional<String> branch = Optional.empty();

        ProtectionDomain pd = clazz.getProtectionDomain();
        CodeSource cs = pd.getCodeSource();
        URL u = cs.getLocation();

        try (InputStream is = u.openStream()) {
            JarInputStream jis = new JarInputStream(is);
            Manifest m = jis.getManifest();
            if (m != null) {
                Attributes as = m.getMainAttributes();
                buildTime = Optional.ofNullable(as.getValue("Build-Time"));
                version = Optional.ofNullable(as.getValue("Implementation-Version"));
                buildNumber = Optional.ofNullable(as.getValue("Implementation-Build"));
                branch = Optional.ofNullable(as.getValue("Implementation-Branch"));
            }
        } catch (IOException ignored) {
        }

        return new Version(buildTime, version, buildNumber, branch);
    }

    public static Version of(Object object)
    {
        return of(object.getClass());
    }

    public String getVersion()
    {
        return _version.orElse("undefined");
    }

    public String getBuildTime()
    {
        return _buildTime.orElse("undefined");
    }

    public String getBuild()
    {
        return _buildNumber.orElse("undefined");
    }

    public String getBranch()
    {
        return _branch.orElse("undefined");
    }

    public static void main(String[] args) throws IOException
    {
        System.out.println(Version.of(Version.class).getVersion());
    }
}
