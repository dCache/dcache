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
package org.dcache.resilience.admin;

import dmg.util.command.Command;

/**
 * <p>Version of the commands to export in standalone mode.</p>
 */
public final class StandaloneResilienceCommands extends ResilienceCommands {

    @Command(name = "diag", hint = HINT_DIAG, description = DESC_DIAG)
    class DiagCommand extends ResilienceCommands.DiagCommand {}

    @Command(name = "diag history", hint = HINT_DIAG_HIST, description = DESC_DIAG_HIST)
    class DiagHistoryCommand extends ResilienceCommands.DiagHistoryCommand {}

    @Command(name = "disable", hint = HINT_DISABLE, description = DESC_DISABLE)
    class DisableCommand extends ResilienceCommands.DisableCommand {}

    @Command(name = "enable", hint = HINT_ENABLE, description = DESC_ENABLE)
    class EnableCommand extends ResilienceCommands.EnableCommand {}

    @Command(name = "inaccessible", hint = HINT_INACCESSIBLE, description = DESC_INACCESSIBLE)
    class InaccessibleFilesCommand extends ResilienceCommands.InaccessibleFilesCommand {}

    @Command(name = "history", hint = HINT_HIST, description = DESC_HIST)
    class FileOpHistoryCommand extends ResilienceCommands.FileOpHistoryCommand {}

    @Command(name = "file cancel", hint = HINT_FILE_CNCL, description = DESC_FILE_CNCL)
    class FileOpCancelCommand extends ResilienceCommands.FileOpCancelCommand {}

    @Command(name = "file check", hint = HINT_CHECK, description = DESC_CHECK)
    class FileCheckCommand extends ResilienceCommands.FileCheckCommand {}

    @Command(name = "file ctrl", hint = HINT_FILE_CTRL, description = DESC_FILE_CTRL)
    class FileControlCommand extends ResilienceCommands.FileControlCommand {}

    @Command(name = "file ls", hint = HINT_FILE_LS, description = DESC_PNFSLS)
    class FileOpLsCommand extends ResilienceCommands.FileOpLsCommand {}

    @Command(name = "pool cancel", hint = HINT_POOL_CNCL, description = DESC_POOL_CNCL)
    class PoolOpCancelCommand extends ResilienceCommands.PoolOpCancelCommand {}

    @Command(name = "pool ctrl", hint= HINT_POOL_CTRL,  description = DESC_POOL_CTRL)
    class PoolControlCommand extends ResilienceCommands.PoolControlCommand {}

    @Command(name = "pool exclude", hint = HINT_POOL_EXCL, description = DESC_POOL_EXCL_INCL)
    class PoolOpExcludeCommand extends ResilienceCommands.PoolOpExcludeCommand {}

    @Command(name = "pool group info", hint = HINT_PGROUP_INFO, description = DESC_PGROUP_INFO)
    class PoolGroupInfoCommand extends ResilienceCommands.PoolGroupInfoCommand {}

    @Command(name = "pool include", hint = HINT_POOL_INCL, description = DESC_POOL_EXCL_INCL)
    class PoolOpIncludeCommand extends ResilienceCommands.PoolOpIncludeCommand {}

    @Command(name = "pool info", hint = HINT_POOL_INFO, description = DESC_POOL_INFO)
    class PoolInfoCommand extends ResilienceCommands.PoolInfoCommand {}

    @Command(name = "pool ls", hint = HINT_POOL_LS, description = DESC_POOL_LS)
    class PoolOpLsCommand extends ResilienceCommands.PoolOpLsCommand {}

    @Command(name = "pool scan", hint = HINT_SCAN, description = DESC_SCAN)
    class PoolScanCommand extends ResilienceCommands.PoolScanCommand {}
}
