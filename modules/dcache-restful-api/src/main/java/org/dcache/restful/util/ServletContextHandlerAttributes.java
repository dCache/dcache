package org.dcache.restful.util;

import javax.security.auth.Subject;
import javax.servlet.ServletContext;

import java.security.AccessController;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.RemotePoolMonitor;
import org.dcache.util.list.ListDirectoryHandler;


public class ServletContextHandlerAttributes {
    public final static String DL = "org.dcache.restful";
    public final static String CS = "org.dcache.restful.CS";
    public final static String PM = "org.dcache.restful.PM";
    public final static String PinMngStub = "org.dcache.restful.PinMngStub";

    public static Subject getSubject()
    {
        return Subject.getSubject(AccessController.getContext());
    }

    public static ListDirectoryHandler getListDirectoryHandler(ServletContext ctx)
    {
        return (ListDirectoryHandler) (ctx.getAttribute(DL));
    }

    public static CellStub getCellStub(ServletContext ctx)
    {
        return (CellStub) (ctx.getAttribute(CS));
    }

    public static RemotePoolMonitor getRemotePoolMonitor(ServletContext ctx)
    {
        return (RemotePoolMonitor) (ctx.getAttribute(PM));
    }

    public static CellStub getPinManager(ServletContext ctx)
    {
        CellStub cellStub = (CellStub) (ctx.getAttribute(PinMngStub));
        return cellStub;

    }
}
