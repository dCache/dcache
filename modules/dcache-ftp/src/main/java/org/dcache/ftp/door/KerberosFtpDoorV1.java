package org.dcache.ftp.door;

import javax.security.auth.Subject;

import org.dcache.auth.Subjects;
import org.dcache.dss.DssContextFactory;
import org.dcache.util.NetLoggerBuilder;

public class KerberosFtpDoorV1 extends GssFtpDoorV1
{
    public KerberosFtpDoorV1(DssContextFactory dssContextFactory)
    {
        super("Kerberos FTP", "krbftp", "k5", dssContextFactory);
    }

    @Override
    protected void logSubject(NetLoggerBuilder log, Subject subject)
    {
        log.add("user.kerberos", Subjects.getKerberosName(subject));
    }
}
