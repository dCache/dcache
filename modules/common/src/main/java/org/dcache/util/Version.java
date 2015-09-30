package org.dcache.util;

import com.google.common.base.Optional;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;
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
        Optional<String> buildTime = Optional.absent();
        Optional<String> version = Optional.absent();
        Optional<String> buildNumber = Optional.absent();
        Optional<String> branch = Optional.absent();

        ProtectionDomain pd = clazz.getProtectionDomain();
        CodeSource cs = pd.getCodeSource();
        URL u = cs.getLocation();

        try (InputStream is = u.openStream()) {
            JarInputStream jis = new JarInputStream(is);
            Manifest m = jis.getManifest();
            if (m != null) {
                Attributes as = m.getMainAttributes();
                buildTime = Optional.fromNullable(as.getValue("Build-Time"));
                version = Optional.fromNullable(as.getValue("Implementation-Version"));
                buildNumber = Optional.fromNullable(as.getValue("Implementation-Build"));
                branch = Optional.fromNullable(as.getValue("Implementation-Branch"));
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
        return _version.or("undefined");
    }

    public String getBuildTime()
    {
        return _buildTime.or("undefined");
    }

    public String getBuild()
    {
        return _buildNumber.or("undefined");
    }

    public String getBranch()
    {
        return _branch.or("undefined");
    }

    public static void main(String[] args) throws IOException
    {
        System.out.println(Version.of(Version.class).getVersion());
    }
}
