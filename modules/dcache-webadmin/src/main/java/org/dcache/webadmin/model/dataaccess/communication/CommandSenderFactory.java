package org.dcache.webadmin.model.dataaccess.communication;

import org.dcache.cells.CellStub;

/**
 * describes the mechanism to get a CommandSender instantiated
 * @author jans
 */
public interface CommandSenderFactory {

    CommandSender createCommandSender(CellMessageGenerator<?> messageGenerator);

    CellStub getCellStub();
}
