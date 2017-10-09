package org.dcache.restful.util;

import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import java.security.AccessController;

import org.dcache.cells.CellStub;
import org.dcache.http.PathMapper;
import org.dcache.poolmanager.RemotePoolMonitor;
import org.dcache.restful.services.alarms.AlarmsInfoService;
import org.dcache.restful.services.billing.BillingInfoService;
import org.dcache.restful.services.cells.CellInfoService;
import org.dcache.restful.services.pool.PoolInfoService;
import org.dcache.restful.services.restores.RestoresInfoService;
import org.dcache.restful.services.transfers.TransferInfoService;
import org.dcache.util.list.ListDirectoryHandler;

public class ServletContextHandlerAttributes {
    public final static String AL           = "org.dcache.restful.AL";
    public final static String BL           = "org.dcache.restful.BL";
    public final static String DL           = "org.dcache.restful";
    public final static String CI           = "org.dcache.restful.CI";
    public final static String PNFS_MANAGER = "org.dcache.restful.PNFS_MANAGER";
    public final static String POOL_MANAGER = "org.dcache.restful.PoolManager";
    public final static String PI           =  "org.dcache.restful.PI";
    public final static String PM           = "org.dcache.restful.PM";
    public final static String RS           = "org.dcache.restful.RS";
    public final static String TF           = "org.dcache.restful.TF";
    public final static String PIN_MANAGER  = "org.dcache.restful.PIN_MANAGER";
    public final static String POOL_STUB = "org.dcache.restful.POOL_STUB";
    public final static String PathMapper   = "org.dcache.restful.PathMapper";

    public static Subject getSubject()
    {
        return Subject.getSubject(AccessController.getContext());
    }

    public static ListDirectoryHandler getListDirectoryHandler(ServletContext ctx)
    {
        return (ListDirectoryHandler) (ctx.getAttribute(DL));
    }

    public static CellStub getPoolStub(ServletContext ctx)
    {
        return (CellStub) ctx.getAttribute(POOL_STUB);
    }

    public static RemotePoolMonitor getRemotePoolMonitor(ServletContext ctx)
    {
        return (RemotePoolMonitor) (ctx.getAttribute(PM));
    }

    public static CellStub getPinManager(ServletContext ctx)
    {
        return (CellStub) (ctx.getAttribute(PIN_MANAGER));
    }

    public static CellStub getPnfsManager(ServletContext ctx)
    {
        return (CellStub) (ctx.getAttribute(PNFS_MANAGER));
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
        return (CellStub) ctx.getAttribute(POOL_MANAGER);
    }

    public static BillingInfoService getBillingInfoService(ServletContext ctx)
    {
        return (BillingInfoService) ctx.getAttribute(BL);
    }

    public static CellInfoService getCellInfoService(ServletContext ctx)
    {
        return (CellInfoService) (ctx.getAttribute(CI));
    }

    public static RestoresInfoService getRestoresInfoService(ServletContext ctx)
    {
        return (RestoresInfoService) ctx.getAttribute(RS);
    }

    public static AlarmsInfoService getAlarmsInfoService(ServletContext ctx)
    {
        return (AlarmsInfoService) ctx.getAttribute(AL);
    }

    public static PoolInfoService getPoolInfoService(ServletContext ctx)
    {
        return (PoolInfoService) (ctx.getAttribute(PI));
    }
}
