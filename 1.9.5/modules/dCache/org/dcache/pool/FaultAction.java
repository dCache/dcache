package org.dcache.pool;

/**
 * Characterizes the severity of a pool error by describing the
 * suggested action.
 */
public enum FaultAction
{
    LOG, READONLY, DISABLED, DEAD, RESTART
}
