/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2024 - 2026 Deutsches Elektronen-Synchrotron
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.JsonPointer;
import tools.jackson.core.JsonToken;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.SerializationContext;
import tools.jackson.databind.jsontype.TypeSerializer;
import tools.jackson.databind.node.BaseJsonNode;
import tools.jackson.databind.node.JsonNodeType;

/**
 * This is like Jackson's <tt>MissingNode</tt> but it carries a reason why the
 * node is missing.  Unfortunately, <tt>MissingNode</tt> is declared final, so
 * we cannot simply extend it.  Instead, this class mirrors its behaviour.
 */
public class ReasonBearingMissingNode extends BaseJsonNode {

    private final String reason;

    public ReasonBearingMissingNode(String reason) {
        this.reason = Objects.requireNonNull(reason);
    }

    public String getReason() {
        return reason;
    }

    @Override
    public ReasonBearingMissingNode deepCopy() {
        return this;
    }

    @Override
    public JsonNodeType getNodeType() {
        return JsonNodeType.MISSING;
    }

    @Override
    public boolean isMissingNode() {
        return true;
    }

    @Override
    public JsonToken asToken() {
        return JsonToken.NOT_AVAILABLE;
    }

    @Override
    protected String _valueDesc() {
        return "<missing>";
    }

    @Override
    public boolean asBoolean() {
        return false;
    }

    @Override
    public String asString() {
        return "";
    }

    @Override
    public short asShort() {
        return 0;
    }

    @Override
    public int asInt() {
        return 0;
    }

    @Override
    public long asLong() {
        return 0L;
    }

    @Override
    public BigInteger asBigInteger() {
        return BigInteger.ZERO;
    }

    @Override
    public float asFloat() {
        return 0.0f;
    }

    @Override
    public double asDouble() {
        return 0.0d;
    }

    @Override
    public BigDecimal asDecimal() {
        return BigDecimal.ZERO;
    }

    @Override
    public Optional<JsonNode> asOptional() {
        return Optional.empty();
    }

    @Override
    public JsonNode require() {
        return _reportRequiredViolation("require() called on `ReasonBearingMissingNode`");
    }

    @Override
    public JsonNode requireNonNull() {
        return _reportRequiredViolation("requireNonNull() called on `ReasonBearingMissingNode`");
    }

    @Override
    public JsonNode get(int index) {
        return null;
    }

    @Override
    public JsonNode path(String fieldName) {
        return this;
    }

    @Override
    public JsonNode path(int index) {
        return this;
    }

    @Override
    protected JsonNode _at(JsonPointer ptr) {
        return this;
    }

    @Override
    public JsonNode findValue(String fieldName) {
        return null;
    }

    @Override
    public JsonNode findParent(String fieldName) {
        return null;
    }

    @Override
    public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar) {
        return foundSoFar;
    }

    @Override
    public List<String> findValuesAsString(String fieldName, List<String> foundSoFar) {
        return foundSoFar;
    }

    @Override
    public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar) {
        return foundSoFar;
    }

    @Override
    public final void serialize(JsonGenerator g, SerializationContext ctxt)
          throws JacksonException {
        g.writeNull();
    }

    @Override
    public void serializeWithType(JsonGenerator g, SerializationContext ctxt,
          TypeSerializer typeSer) throws JacksonException {
        g.writeNull();
    }

    @Override
    public int hashCode() {
        return reason.hashCode();
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public String toPrettyString() {
        return "";
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof ReasonBearingMissingNode)) {
            return false;
        }

        ReasonBearingMissingNode o = (ReasonBearingMissingNode)other;
        return reason.equals(o.reason);
    }
}
