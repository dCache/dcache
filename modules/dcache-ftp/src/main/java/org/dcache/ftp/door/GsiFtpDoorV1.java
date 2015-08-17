package org.dcache.ftp.door;

import javatunnel.dss.DssContextFactory;
import javatunnel.dss.GsiDssContextFactory;
import org.ietf.jgss.GSSException;

import javax.security.auth.Subject;

import java.io.IOException;

import org.dcache.auth.Subjects;
import org.dcache.cells.Option;
import org.dcache.util.Crypto;
import org.dcache.util.NetLoggerBuilder;

public class GsiFtpDoorV1 extends GssFtpDoorV1
{
    @Option(
        name="service-key",
        required=true
    )
    protected String service_key;

    @Option(
        name="service-cert",
        required=true
    )
    protected String service_cert;

    @Option(
        name="service-trusted-certs",
        required=true
    )
    protected String service_trusted_certs;

    @Option(
            name="gridftp.security.ciphers",
            required=true
    )
    protected String cipherFlags;

    public GsiFtpDoorV1()
    {
        super("GSI FTP", "gsiftp", "gsi");
    }

    @Override
    protected void logSubject(NetLoggerBuilder log, Subject subject)
    {
        log.add("user.dn", Subjects.getDn(subject));
    }

    @Override
    protected DssContextFactory createFactory() throws IOException, GSSException
    {
        return new GsiDssContextFactory(service_key, service_cert, service_trusted_certs,
                                        Crypto.getBannedCipherSuitesFromConfigurationValue(cipherFlags));
    }

}
