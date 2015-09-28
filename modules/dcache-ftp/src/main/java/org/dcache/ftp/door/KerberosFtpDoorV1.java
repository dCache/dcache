package org.dcache.ftp.door;

import org.dcache.dss.DssContextFactory;
import org.dcache.dss.KerberosDssContextFactory;
import org.ietf.jgss.GSSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;

import org.dcache.auth.Subjects;
import org.dcache.util.Option;
import org.dcache.util.NetLoggerBuilder;

public class KerberosFtpDoorV1 extends GssFtpDoorV1
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosFtpDoorV1.class);

    @Option(name = "svc-principal",
            required = true)
    private String servicePrincipal;

    @Option(name = "kdc-list")
    private String kdcList;

    public KerberosFtpDoorV1()
    {
        super("Kerberos FTP", "krbftp", "k5");
    }

    @Override
    protected void logSubject(NetLoggerBuilder log, Subject subject)
    {
        log.add("user.kerberos", Subjects.getKerberosName(subject));
    }

    @Override
    protected DssContextFactory createFactory() throws IOException, GSSException
    {
        int nretry = 10;
        String[] kdcList = (this.kdcList != null) ? this.kdcList.split(",") : new String[0];
        GSSException error;
        do {
            if (kdcList.length > 0) {
                String kdc = kdcList[nretry % kdcList.length];
                System.getProperties().put("java.security.krb5.kdc", kdc);
            }
            try {
                return new KerberosDssContextFactory(servicePrincipal);
            } catch (GSSException e) {
                LOGGER.debug("KerberosFTPDoorV1::getServiceContext: got exception " +
                             " while looking up credential: {}", e.getMessage());
                error = e;
            }
            --nretry;
        } while (nretry > 0);
        throw error;
   }
}
