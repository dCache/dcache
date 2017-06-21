package org.dcache.restful.util;

import javax.security.auth.Subject;
import javax.servlet.ServletContext;

import java.security.AccessController;

import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.poolmanager.RemotePoolMonitor;
import org.dcache.restful.services.transfers.TransferInfoService;
import org.dcache.util.list.ListDirectoryHandler;

public class ServletContextHandlerAttributes {
    public final static String DL = "org.dcache.restful";
    public final static String CS = "org.dcache.restful.CS";
    public final static String POOL_MANAGER = "org.dcache.restful.PoolManager";
    public final static String PM = "org.dcache.restful.PM";
    public final static String TF = "org.dcache.restful.TF";
    public final static String PinMngStub = "org.dcache.restful.PinMngStub";
    public final static String PathMapper = "org.dcache.restful.PathMapper";



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

    public static PathMapper getPathMapper(ServletContext ctx)
    {
        return (PathMapper) (ctx.getAttribute(PathMapper));
    }

    public static TransferInfoService getTransferInfoService(ServletContext ctx)
    {
        return (TransferInfoService) ctx.getAttribute(TF);
    }
    public static CellStub getPoolManger(ServletContext ctx)
    {
        CellStub cellStub = (CellStub) ctx.getAttribute(POOL_MANAGER);
        return cellStub;

    }


}
