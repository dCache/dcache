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
package org.dcache.qos;

import static org.dcache.qos.QoSPolicyTest.QoSPolicyBuilder.aQoSPolicy;
import static org.dcache.qos.QoSPolicyTest.QoSStateBuilder.aQoSState;
import static org.dcache.qos.QoSPolicyTest.SpecificationBuilder.aDiskSpecification;
import static org.dcache.qos.QoSPolicyTest.SpecificationBuilder.aTapeSpecification;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.gson.GsonBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class QoSPolicyTest {

    static class QoSPolicyBuilder {

        QoSPolicy policy;

        static QoSPolicyBuilder aQoSPolicy(String name) {
            QoSPolicyBuilder builder = new QoSPolicyBuilder();
            builder.policy = new QoSPolicy();
            builder.policy.setName(name);
            return builder;
        }

        QoSPolicyBuilder withStates(QoSStateBuilder... builder) {
            policy.setStates(
                  Arrays.stream(builder).map(QoSStateBuilder::build).collect(Collectors.toList()));
            return this;
        }

        QoSPolicy build() {
            return policy;
        }
    }

    static class QoSStateBuilder {

        QoSState state;

        static QoSStateBuilder aQoSState() {
            QoSStateBuilder builder = new QoSStateBuilder();
            builder.state = new QoSState();
            return builder;
        }

        QoSStateBuilder with(SpecificationBuilder... builder) {
            state.setMedia(Arrays.stream(builder).map(SpecificationBuilder::build)
                  .collect(Collectors.toList()));
            return this;
        }

        QoSStateBuilder lasting(String duration) {
            state.setDuration(duration);
            return this;
        }

        QoSState build() {
            return state;
        }
    }

    static class SpecificationBuilder {

        QoSStorageMediumSpecification specification;

        static SpecificationBuilder aDiskSpecification(String type) {
            SpecificationBuilder builder = new SpecificationBuilder();
            builder.specification = new DefaultQoSDiskSpecification();
            ((DefaultQoSDiskSpecification) builder.specification).setType(type);
            return builder;
        }

        static SpecificationBuilder aTapeSpecification(String type) {
            SpecificationBuilder builder = new SpecificationBuilder();
            builder.specification = new DefaultQoSTapeSpecification();
            ((DefaultQoSTapeSpecification) builder.specification).setType(type);
            return builder;
        }

        SpecificationBuilder withCopies(int copies) {
            ((DefaultQoSDiskSpecification) specification).setNumberOfCopies(copies);
            return this;
        }

        SpecificationBuilder partitionedBy(String... keys) {
            ((DefaultQoSDiskSpecification) specification).setPartitionKeys(
                  Arrays.stream(keys).collect(
                        Collectors.toList()));
            return this;
        }

        SpecificationBuilder withInstance(String instance) {
            ((DefaultQoSTapeSpecification) specification).setInstance(instance);
            return this;
        }

        QoSStorageMediumSpecification build() {
            return specification;
        }
    }

    private QoSPolicy policy;
    private String jsonPolicy;

    @Test
    public void shouldParseQoSPolicyCorrectly() {
        givenAQoSPolicy();
        whenSerializedAsJson();
        verifyJsonEqualsJavaObject();
    }

    @Test
    public void shouldDeserializePolicyCorrectly() {
        givenAQoSPolicy();
        whenSerializedAsJson();
        assertEquals("JSON did not deserialize properly", policy, thePolicyDeserializedFromJson());
    }

    private void givenAQoSPolicy() {
        policy = aQoSPolicy("MyAnalysisData")
              .withStates(
                    aQoSState().lasting("P1D")
                          .with(aDiskSpecification("SSD").withCopies(2).partitionedBy("hostname"),
                                aDiskSpecification("spinning").withCopies(1)),
                    aQoSState().lasting("P3D")
                          .with(aDiskSpecification("spinning").withCopies(2)
                                      .partitionedBy("hostname"),
                                aTapeSpecification("CTA").withInstance("CTA::public")),
                    aQoSState().with(aTapeSpecification("CTA").withInstance("CTA::public"))).build();
    }

    private QoSPolicy thePolicyDeserializedFromJson() {
        return DefaultQoSPolicyJsonDeserializer.fromJsonString(jsonPolicy);
    }

    private void verifyJsonEqualsJavaObject() {
        JSONObject object = new JSONObject(jsonPolicy);
        assertEquals("different policy name!", policy.getName(), object.getString("name"));
        List<QoSState> states = policy.getStates();
        JSONArray stateArray = object.getJSONArray("states");
        int len = policy.getStates().size();
        assertEquals("different number of states!", len, stateArray.length());
        for (int i = 0; i < len; ++i) {
            QoSState origState = states.get(i);
            JSONObject jsonState = stateArray.optJSONObject(i);
            String duration = origState.getDuration();
            if (duration == null) {
                assertFalse("duration should have been null!", jsonState.has("duration"));
            } else {
                assertTrue("duration should not be null!", jsonState.has("duration"));
                assertEquals("duration are different!", duration,
                      jsonState.getString("duration"));
            }

            List<QoSStorageMediumSpecification> media = origState.getMedia();
            JSONArray mediaArray = jsonState.getJSONArray("media");
            int mLen = media.size();
            assertEquals(mLen, mediaArray.length());
            for (int j = 0; j < mLen; ++j) {
                QoSStorageMediumSpecification origSpec = media.get(j);
                JSONObject jsonSpec = mediaArray.getJSONObject(j);
                assertEquals("storage medium at " + j + " does not match!",
                      origSpec.getStorageMedium().name(),
                      jsonSpec.getString("storageMedium"));
                assertEquals("storage medium type does not match",
                      origSpec.getType(), jsonSpec.getString("type"));
                switch (origSpec.getStorageMedium()) {
                    case DISK:
                        assertEquals("number of copies does not match",
                              origSpec.getNumberOfCopies(),
                              (Integer) jsonSpec.getInt("numberOfCopies"));
                        QoSDiskSpecification disk = (QoSDiskSpecification) origSpec;
                        List<String> keys = disk.getPartitionKeys();
                        if (keys == null) {
                            assertFalse("partition keys should have been null!",
                                  jsonSpec.has("partitionKeys"));
                            continue;
                        }
                        assertTrue("partition keys should not be null!",
                              jsonSpec.has("partitionKeys"));
                        int kLen = keys.size();
                        JSONArray keyArray = jsonSpec.getJSONArray("partitionKeys");
                        assertEquals(kLen, keyArray.length());
                        for (int k = 0; k < kLen; ++k) {
                            assertEquals("partition key does not match!", keys.get(k),
                                  keyArray.get(k));
                        }
                        break;
                    case HSM:
                        QoSHsmSpecification hsm = (QoSHsmSpecification) origSpec;
                        assertEquals("hsm instance does not match!", hsm.getInstance(),
                              jsonSpec.getString("instance"));
                        break;
                }
            }
        }
    }

    private void whenSerializedAsJson() {
        jsonPolicy = new GsonBuilder().setPrettyPrinting()
              .disableHtmlEscaping()
              .create().toJson(policy);
    }
}
