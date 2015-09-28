package org.dcache.ftp.door;

import javax.security.auth.Subject;

import org.dcache.auth.Subjects;
import org.dcache.dss.DssContextFactory;
import org.dcache.util.NetLoggerBuilder;

public class GsiFtpDoorV1 extends GssFtpDoorV1
{
    public GsiFtpDoorV1(DssContextFactory dssContextFactory)
    {
        super("GSI FTP", "gsiftp", "gsi", dssContextFactory);
    }

    @Override
    protected void logSubject(NetLoggerBuilder log, Subject subject)
    {
        log.add("user.dn", Subjects.getDn(subject));
    }
}
