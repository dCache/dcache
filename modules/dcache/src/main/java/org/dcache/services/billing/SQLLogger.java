package org.dcache.services.billing;

import diskCacheV111.util.BillingDB;

import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.PoolCostInfoMessage;

import java.sql.SQLException;
import dmg.util.Args;

public class SQLLogger
    extends BillingComponent
{
    private BillingDB _sqlLog;
    private Args _args;

    public SQLLogger(String args)
    {
        _args = new Args(args);
    }

    public boolean getEnabled()
    {
        return _sqlLog != null;
    }

    public void setEnabled(boolean enabled)
        throws SQLException
    {
        _sqlLog = enabled ? new BillingDB(_args) : null;
    }

    public void messageArrived(MoverInfoMessage message)
    {
        try {
            if (_sqlLog != null)
                _sqlLog.log(message);
        }catch (SQLException e) {
            error("Can't log billing into SQL database : " + e.getMessage());
        }
    }

    public void messageArrived(StorageInfoMessage message)
    {
        try {
            if (_sqlLog != null)
                _sqlLog.log(message);
        }catch (SQLException e) {
            error("Can't log billing into SQL database : " + e.getMessage());
        }
    }

    public void messageArrived(DoorRequestInfoMessage message)
    {
        try {
            if (_sqlLog != null)
                _sqlLog.log(message);
        }catch (SQLException e) {
            error("Can't log billing into SQL database : " + e.getMessage());
        }
    }

    public void messageArrived(PoolHitInfoMessage message)
    {
        try {
            if (_sqlLog != null)
                _sqlLog.log(message);
        }catch (SQLException e) {
            error("Can't log billing into SQL database : " + e.getMessage());
        }
    }

    public void messageArrived(PoolCostInfoMessage message)
    {
        try {
            if (_sqlLog != null)
                _sqlLog.log(message);
        }catch (SQLException e) {
            error("Can't log billing into SQL database : " + e.getMessage());
        }
    }
}