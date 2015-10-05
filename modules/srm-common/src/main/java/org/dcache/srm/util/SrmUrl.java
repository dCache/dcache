package org.dcache.srm.util;

import java.net.URI;
import java.net.URISyntaxException;

public class SrmUrl
{
    public static final int DEFAULT_SRM_PORT = 8443;

    private SrmUrl()
    {
    }

    public static URI withDefaultPort(URI location, int defaultSrmPort) throws URISyntaxException
    {
        return (location.getPort() == -1 && !location.getScheme().equals("file"))
               ? new URI(location.getScheme(), location.getUserInfo(), location.getHost(),
                         getDefaultPort(location.getScheme(), defaultSrmPort),
                         location.getPath(), location.getQuery(), location.getFragment())
               : location;
    }

    public static URI createWithDefaultPort(String location, int defaultSrmPort) throws URISyntaxException
    {
        return withDefaultPort(new URI(location), defaultSrmPort);
    }

    public static URI withDefaultPort(URI location) throws URISyntaxException
    {
        return withDefaultPort(location, DEFAULT_SRM_PORT);
    }

    public static URI createWithDefaultPort(String location) throws URISyntaxException
    {
        return withDefaultPort(new URI(location));
    }

    public static int getDefaultPort(String protocol, int defaultSrmPort)
    {
        switch (protocol) {
        case "ftp":
            return 21;
        case "gsiftp":
        case "gridftp":
            return 2811;
        case "http":
            return 80;
        case "https":
            return 443;
        case "ldap":
            return 389;
        case "ldaps":
            return 636;
        case "srm":
            return defaultSrmPort;
        default:
            return -1;
        }
    }
}
