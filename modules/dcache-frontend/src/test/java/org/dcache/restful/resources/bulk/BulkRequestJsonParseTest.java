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
package org.dcache.restful.resources.bulk;

import static org.dcache.restful.resources.bulk.BulkResources.toBulkRequest;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.BadRequestException;
import org.dcache.services.bulk.BulkRequest;
import org.dcache.services.bulk.BulkRequest.Depth;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BulkRequestJsonParseTest {

    static final String JSON_FORMAT = "{'activity':'%s', 'target': %s,"
          + "'arguments':{'lifetime':'%s','lifetime-unit':'%s'},"
          + "'%s':'%s', '%s':'%s', '%s':'%s'}";

    String requestJson;
    BulkRequest bulkRequest;

    String activity;
    List<String> jsonTarget;
    List<String> javaTarget;
    Integer lifetime;
    TimeUnit lifetimeUnit;
    Boolean clearOnSuccess;
    Boolean clearOnFailure;
    Depth expandDirectories;

    @Before
    public void setUp() {
        activity = "PIN";
        jsonTarget = List.of("'/pnfs/fs/usr/test/tape/300'","'/pnfs/fs/usr/test/tape/301'");
        javaTarget = List.of("/pnfs/fs/usr/test/tape/300","/pnfs/fs/usr/test/tape/301");
        lifetime = 1;
        lifetimeUnit = TimeUnit.MINUTES;
        clearOnSuccess = true;
        clearOnFailure = true;
        expandDirectories = Depth.ALL;
    }

    @Test
    public void shouldSucceedWithMixedAttributes() throws Exception {
        givenJsonWithMixedAttributeStyle();
        whenParsed();
        assertThatRequestIsValid();
    }

    @Test(expected = BadRequestException.class)
    public void shouldFailWithRedundantAttribute() throws Exception {
        givenJsonWithAttributeDefinedTwice();
        whenParsed();
    }

    @Test(expected = BadRequestException.class)
    public void shouldFailWithUnsupportedAttribute() throws Exception {
        givenJsonWithUnsupportedAttribute();
        whenParsed();
    }

    @Test
    public void shouldSucceedWithMultipleTargetAttribute() throws Exception {
        givenJsonWithArrayTargetAttribute();
        whenParsed();
        assertThatRequestIsValid();
    }

    private void assertThatRequestIsValid() {
        assertEquals(activity, bulkRequest.getActivity());
        assertEquals(javaTarget, bulkRequest.getTarget());
        assertEquals((int) lifetime, Integer.parseInt(bulkRequest.getArguments().get("lifetime")));
        assertEquals(lifetimeUnit,
              TimeUnit.valueOf(bulkRequest.getArguments().get("lifetime-unit")));
        assertEquals(clearOnFailure, bulkRequest.isClearOnFailure());
        assertEquals(clearOnSuccess, bulkRequest.isClearOnSuccess());
        assertEquals(expandDirectories, bulkRequest.getExpandDirectories());
    }

    private void givenJsonWithMixedAttributeStyle() {
        requestJson = String.format(JSON_FORMAT, activity, jsonTarget, lifetime, lifetimeUnit,
              "clearOnSuccess", clearOnSuccess, "clear_on_failure", clearOnFailure,
              "expand-directories", expandDirectories);
    }

    private void givenJsonWithAttributeDefinedTwice() {
        /* clearOnSuccess = clear_on_success */
        requestJson = String.format(JSON_FORMAT, activity, jsonTarget, lifetime, lifetimeUnit,
              "clearOnSuccess", clearOnSuccess, "clear_on_success", clearOnFailure,
              "expand-directories", expandDirectories);
    }

    private void givenJsonWithUnsupportedAttribute() {
        /* cancelOnSuccess is nonsense */
        requestJson = String.format(JSON_FORMAT, activity, jsonTarget, lifetime, lifetimeUnit,
              "cancelOnSuccess", clearOnSuccess, "clear_on_failure", clearOnFailure,
              "expand-directories", expandDirectories);
    }

    private void givenJsonWithArrayTargetAttribute() {
        requestJson = String.format(JSON_FORMAT, activity, jsonTarget, lifetime, lifetimeUnit,
              "clear_on_success", clearOnSuccess, "clear_on_failure", clearOnFailure,
              "expand-directories", expandDirectories);
    }

    private void whenParsed() {
        bulkRequest = toBulkRequest(requestJson, null);
    }
}
