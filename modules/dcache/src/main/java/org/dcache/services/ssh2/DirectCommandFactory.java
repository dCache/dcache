/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.services.ssh2;

import static com.google.common.base.Preconditions.checkArgument;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import java.util.Arrays;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.server.command.Command;
import org.apache.sshd.server.command.CommandFactory;
import org.dcache.cells.CellStub;
import org.dcache.util.list.ListDirectoryHandler;
import org.springframework.beans.factory.annotation.Required;

/**
 * Implementation of Command Factory interface that enables direct command execution in dCache admin
 * server. Commands are semicolon (;) separated: ssh -p PORT user@example.org
 * "command1;command2;command3"
 *
 * @author litvinse
 */
public class DirectCommandFactory implements CommandFactory, CellMessageSender {

    private static String COMMAND_SEPARATOR = ";";
    private CellEndpoint endpoint;
    private CellStub pnfsManager;
    private CellStub poolManager;
    private CellStub acm;
    private String prompt;
    private ListDirectoryHandler list;

    @Required
    public void setPnfsManager(CellStub stub) {
        this.pnfsManager = stub;
    }

    @Required
    public void setPoolManager(CellStub stub) {
        this.poolManager = stub;
    }

    @Required
    public void setAcm(CellStub stub) {
        this.acm = stub;
    }

    @Required
    public void setPrompt(String prompt) {
        this.prompt = prompt;
    }

    @Required
    public void setListHandler(ListDirectoryHandler list) {
        this.list = list;
    }

    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public Command createCommand(ChannelSession channelSession, String command) {
        checkArgument(command != null, "No command");
        return new DirectCommand(Arrays.asList(command.split(COMMAND_SEPARATOR)),
              endpoint,
              poolManager,
              pnfsManager,
              acm,
              prompt,
              list);
    }
}

