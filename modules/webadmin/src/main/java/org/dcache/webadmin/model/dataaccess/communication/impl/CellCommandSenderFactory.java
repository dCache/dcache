package org.dcache.webadmin.model.dataaccess.communication.impl;

import org.dcache.cells.CellStub;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * instantiates a CellCommandSender for the given CellMessageGenerator and injects
 * the cellStub in it
 * @author jans
 */
public class CellCommandSenderFactory implements CommandSenderFactory {

    private static final Logger _log = LoggerFactory.getLogger(CellCommandSenderFactory.class);
    private CellStub _poolCellStub;

    @Override
    public CommandSender createCommandSender(CellMessageGenerator messageGenerator) {
        CellCommandSender commandSender = new CellCommandSender(messageGenerator);
        commandSender.setCellStub(_poolCellStub);
        return commandSender;
    }

    public void setPoolCellStub(CellStub poolCellStub) {
        _poolCellStub = poolCellStub;
    }
}
