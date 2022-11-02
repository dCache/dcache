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
package org.dcache.mock.verifiers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;

import diskCacheV111.pools.PoolV2Mode;
import org.dcache.qos.data.PoolQoSStatus;
import org.dcache.qos.services.scanner.data.PoolMatcher;
import org.dcache.qos.services.scanner.data.PoolOperationMap;
import org.mockito.internal.verification.Times;

public class PoolOperationMapVerifier implements MockVerifier<PoolOperationMap> {

    public static PoolOperationMapVerifier aPoolOperationMapVerifier() {
        return new PoolOperationMapVerifier();
    }

    @Override
    public void apply(PoolOperationMap mock, String method, int times, Object... params)
          throws NoSuchMethodException {
        switch (method) {
            case "handlePoolStatusChange":
                verify(mock, new Times(times)).handlePoolStatusChange(
                      params[0] == null ? (String) isNull()
                            : params[0].equals("any") ? anyString() : eq((String) params[0]),
                      params[1] == null ? any(): eq((PoolQoSStatus) params[1]));
                break;
            case "cancel":
                verify(mock, new Times(times)).cancel(
                      params == null ? any() : eq((PoolMatcher) params[0]));
                break;
            case "add":
                verify(mock, new Times(times)).add(
                      params[0] == null ? (String) isNull()
                            : params[0].equals("any") ? anyString() : eq((String) params[0]));
                break;
            case "remove":
                verify(mock, new Times(times)).remove(
                      params[0] == null ? (String) isNull()
                            : params[0].equals("any") ? anyString() : eq((String) params[0]));
                break;
            case "scan":
                verify(mock, new Times(times)).scan(eq((String) params[0]),
                      params[1] == null ? (String) isNull()
                            : params[1].equals("any") ? anyString() : eq((String) params[1]),
                      params[2] == null ? (String) isNull()
                            : params[2].equals("any") ? anyString() : eq((String) params[2]),
                      params[3] == null ? (String) isNull()
                            : params[3].equals("any") ? anyString() : eq((String) params[3]),
                      params[4] == null ? any() : eq((PoolV2Mode) params[4]),
                      params[5] == null ? anyBoolean() : eq((Boolean) params[5]));
                break;
            case "updateStatus":
                verify(mock, new Times(times)).updateStatus(
                      params[0] == null ? (String) isNull()
                            : params[0].equals("any") ? anyString() : eq((String) params[0]),
                      params[1] == null ? any(): eq((PoolQoSStatus) params[1]));
                break;
            default:
                throw new NoSuchMethodException(method);
        }
    }
}
