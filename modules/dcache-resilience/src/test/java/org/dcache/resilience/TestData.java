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
package org.dcache.resilience;

import diskCacheV111.util.PnfsId;

final class TestData {
    static final PnfsId[] CUSTODIAL_NEARLINE = {
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD00")
    };

    static final PnfsId[] REPLICA_ONLINE = {
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD10"),
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD11"),
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD12"),
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD13"),
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD14"),
    };

    static final PnfsId[] CUSTODIAL_ONLINE = {
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD20"),
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD21"),
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD22"),
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD23"),
                    new PnfsId("0000B64FF6C247D84D42BE5ACE9CB688AD24"),
    };

    static final int[][] COST_METRICS = {
                    {0, 100, 0, 0, 0, 0, 0, 0, 0},
                    {50, 100, 0, 0, 0, 0, 0, 0, 0},
                    {0, 100, 0, 0, 0, 1, 0, 0, 1},
                    {0, 100, 0, 10, 0, 0, 0, 0, 0},
                    {50, 100, 0, 10, 0, 0, 5, 0, 0},
                    {0, 100, 0, 0, 0, 0, 0, 0, 0},
                    {50, 100, 0, 0, 0, 0, 0, 0, 0},
                    {0, 100, 0, 0, 0, 1, 0, 0, 1},
                    {0, 100, 0, 10, 0, 0, 0, 0, 0},
                    {50, 100, 0, 10, 0, 0, 5, 0, 0},
                    {0, 100, 0, 0, 0, 0, 0, 0, 0},
                    {50, 100, 0, 0, 0, 0, 0, 0, 0},
                    {0, 100, 0, 0, 0, 1, 0, 0, 1},
                    {0, 100, 0, 10, 0, 0, 0, 0, 0},
                    {50, 100, 0, 10, 0, 0, 5, 0, 0}
    };

    static final long[][] POOL_SPACE = {
                    {10000000L, 9500000L, 0, 0},
                    {10000000L, 600000L, 0, 0},
                    {10000000L, 30000L, 0, 0},
                    {10000000L, 8000000L, 0, 0},
                    {10000000L, 40000L, 0, 0},
                    {10000000L, 9200000L, 0, 0},
                    {10000000L, 7500000L, 0, 0},
                    {10000000L, 390000L, 0, 0},
                    {10000000L, 9500000L, 0, 0},
                    {10000000L, 600000L, 0, 0},
                    {10000000L, 30000L, 0, 0},
                    {10000000L, 8000000L, 0, 0},
                    {10000000L, 40000L, 0, 0},
                    {10000000L, 9200000L, 0, 0},
                    {10000000L, 7500000L, 0, 0}
    };

    static final String[][] POOL_TAGS = {
                    {},
                    {},
                    {},
                    {},
                    {},
                    {"hostname:h0"},
                    {"hostname:h0"},
                    {"hostname:h1"},
                    {"hostname:h1"},
                    {"hostname:h2"},
                    {"hostname:h0", "rack:r0"},
                    {"hostname:h1", "rack:r0"},
                    {"hostname:h2", "rack:r0"},
                    {"hostname:h0", "rack:r1"},
                    {"hostname:h1", "rack:r1"}
    };

    static final String[] POOL_TYPE = {
                    "standard_pool-",
                    "resilient_pool-"
    };

    static final int[] POOL_COUNT = {
                    2,
                    POOL_TAGS.length
    };

    static final String HSM = "enstore";

    static final String[] PROTOCOL_UNITS = {
                    "*/*"
    };

    static final String[] NET_UNITS = {
                    "::/0",
                    "0.0.0.0/0.0.0.0"
    };

    static final String[] STORAGE_CLASSES = {
                    "resilient-0.dcache-devel-test",
                    "resilient-1.dcache-devel-test",
                    "resilient-2.dcache-devel-test",
                    "resilient-3.dcache-devel-test",
                    "resilient-4.dcache-devel-test",
                    "standard.dcache-devel-test",
                    "*@*"
    };

    static final String[] STORAGE_UNITS = {
                    getStorageUnitName(STORAGE_CLASSES[0]),
                    getStorageUnitName(STORAGE_CLASSES[1]),
                    getStorageUnitName(STORAGE_CLASSES[2]),
                    getStorageUnitName(STORAGE_CLASSES[3]),
                    getStorageUnitName(STORAGE_CLASSES[4]),
                    getStorageUnitName(STORAGE_CLASSES[5]),
                    STORAGE_CLASSES[6]
    };

    static final String[][] STORAGE_UNITS_SET = {
                    {"2", "hostname"},
                    {"2", "hostname,rack"},
                    {"2", "rack"},
                    {"2", "hostname,rack"},
                    {"3", ""},
                    null,
                    {"2", ""}
    };

    static final String[][] POOL_GROUPS = {
                    {"standard-group", ""},
                    {"resilient-group", "-resilient"}
    };

    static final String[] UNIT_GROUPS= {
                    "world-net",
                    "any-protocol",
                    "standard-storage",
                    "resilient-storage"
    };

    static final String[][] UNIT_GROUPS_ADD = {
                    {UNIT_GROUPS[0], NET_UNITS[0], "true"},
                    {UNIT_GROUPS[0], NET_UNITS[1], "true"},
                    {UNIT_GROUPS[1], PROTOCOL_UNITS[0], "false"},
                    {UNIT_GROUPS[2], STORAGE_UNITS[5], "false"},
                    {UNIT_GROUPS[3], STORAGE_UNITS[0], "false"},
                    {UNIT_GROUPS[3], STORAGE_UNITS[1], "false"},
                    {UNIT_GROUPS[3], STORAGE_UNITS[2], "false"},
                    {UNIT_GROUPS[3], STORAGE_UNITS[3], "false"},
                    {UNIT_GROUPS[3], STORAGE_UNITS[4], "false"},
                    {UNIT_GROUPS[3], STORAGE_UNITS[6], "false"}
    };

    static final String[][] LINKS = {
                    {"standard-link", UNIT_GROUPS[0], UNIT_GROUPS[1], UNIT_GROUPS[2]},
                    {"resilient-link", UNIT_GROUPS[0], UNIT_GROUPS[1], UNIT_GROUPS[3]}
    };

    static final String[][] LINKS_SET = {
                    {"10", "10", "10", "-1", null},
                    {"10", "10", "10", "-1", null}
    };

    static final String[][] LINKS_ADD = {
                    {POOL_GROUPS[0][0]},
                    {POOL_GROUPS[1][0]}
    };

    static String getStorageUnitName(String storageClass) {
        return storageClass + "@" + HSM;
    }

    static final String[][] NON_RESILIENT_LOCATIONS = {
                    {"standard_pool-0"}
    };

    static final String[][] NEW_RESILIENT_LOCATIONS = {
                    {"resilient_pool-0"},     // (3,H)   : {}
                    {"resilient_pool-1"},     // (2,H,R) : {}
                    {"resilient_pool-2"},     // (2,R)   : {}
                    {"resilient_pool-3"},     // (3,H,R) : {}
                    {"resilient_pool-4"},     // (3)     : {}
    };

    static final String[][] NEW_RESILIENT_LOCATIONS_H = {
                    {"resilient_pool-5"},     // (3,H)   : {H0}
                    {"resilient_pool-6"},     // (2,H,R) : {H0}
                    {"resilient_pool-7"},     // (2,R)   : {H1}
                    {"resilient_pool-8"},     // (3,H,R) : {H1}
                    {"resilient_pool-9"},     // (3)     : {H2}
    };

    static final String[][] NEW_RESILIENT_LOCATIONS_HR = {
                    {"resilient_pool-10"},    // (3,H)   : {H0,R0}
                    {"resilient_pool-10"},    // (2,H,R) : {H0,R0}
                    {"resilient_pool-10"},    // (2,R)   : {H0,R0}
                    {"resilient_pool-10"},    // (3,H,R) : {H0,R0}
                    {"resilient_pool-10"},    // (3)     : {H0,R0}
    };

    static final String[][] MIN_RESILIENT_LOCATIONS = {
                    {"resilient_pool-5", "resilient_pool-12"},
                    {"resilient_pool-10", "resilient_pool-13"},
                    {"resilient_pool-5", "resilient_pool-10"},
                    {"resilient_pool-11", "resilient_pool-13"},
                    {"resilient_pool-7", "resilient_pool-9", "resilient_pool-11"},
    };

    static final String[][] EXCESS_RESILIENT_LOCATIONS = {
                    {"resilient_pool-4", "resilient_pool-5", "resilient_pool-7",
                                    "resilient_pool-12"},
                    {"resilient_pool-3", "resilient_pool-10", "resilient_pool-13"},
                    {"resilient_pool-5", "resilient_pool-10", "resilient_pool-14"},
                    {"resilient_pool-0", "resilient_pool-1", "resilient_pool-2",
                                    "resilient_pool-11", "resilient_pool-13"},
                    {"resilient_pool-5", "resilient_pool-7", "resilient_pool-9",
                                    "resilient_pool-11", "resilient_pool-12"},
    };

    private TestData() {
    }
}
