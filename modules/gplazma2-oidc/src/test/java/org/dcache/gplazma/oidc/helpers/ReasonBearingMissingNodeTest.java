/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2024 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.gplazma.oidc.helpers;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.junit.Test;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;

public class ReasonBearingMissingNodeTest {

    @Test(expected=NullPointerException.class)
    public void shouldRejectNullReason() {
        new ReasonBearingMissingNode(null);
    }

    @Test
    public void shouldProvideReason() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node.getReason(), is(equalTo("What went wrong.")));
    }

    @Test
    public void shouldHaveCorrectNodeType() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node.getNodeType(), is(equalTo(JsonNodeType.MISSING)));
    }

    @Test
    public void shouldBeMissingNode() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertTrue(node.isMissingNode());
    }

    @Test
    public void shouldBeNotEqualToObject() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        var other = new Object();
        assertThat(node, is(not(equalTo(other))));
    }

    @Test
    public void shouldEqualSelf() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node, is(equalTo(node)));
    }

    @Test
    public void shouldEqualNodeWithSameReason() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        var otherNode = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node, is(equalTo(otherNode)));
    }

    @Test
    public void shouldNotEqualNodeWithDifferentReason() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        var otherNode = new ReasonBearingMissingNode("Some other problem.");
        assertThat(node, is(not(equalTo(otherNode))));
    }

    @Test
    public void shouldHaveSameHashCodeAsAnotherNodeWithSameReason() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        var otherNode = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node.hashCode(), is(equalTo(otherNode.hashCode())));
    }

    @Test
    public void shouldHaveTokenNotAvailable() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node.asToken(), sameInstance(JsonToken.NOT_AVAILABLE));
    }

    @Test
    public void shouldAsTextAsEmptyString() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node.asText(), equalTo(""));
    }

    @Test
    public void shouldToPrettyStringAsEmptyString() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node.toPrettyString(), equalTo(""));
    }

    @Test
    public void shouldToStringAsEmptyString() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node.toString(), equalTo(""));
    }

    @Test
    public void shouldAsTextWithDefaultToDefault() {
        var node = new ReasonBearingMissingNode("What went wrong.");
        assertThat(node.asText("default value"), equalTo("default value"));
    }

    @Test
    public void shouldSerialiseToNull() throws Exception {
        var node = new ReasonBearingMissingNode("What went wrong.");

        var g = mock(JsonGenerator.class);
        var p = mock(SerializerProvider.class);

        node.serialize(g, p);

        then(g).should().writeNull();
        then(g).shouldHaveNoMoreInteractions();
        then(p).shouldHaveNoInteractions();
    }
}
