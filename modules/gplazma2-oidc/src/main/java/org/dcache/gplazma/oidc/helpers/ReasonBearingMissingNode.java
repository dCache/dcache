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
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import java.io.IOException;
import java.util.Objects;

/**
 * This is like Jackson's <tt>MissingNode</tt> but it carries a reason why the
 * node is missing.  Unfortunately, <tt>MissingNode</tt> is declared final, so
 * we cannot simply extend it.
 */
public class ReasonBearingMissingNode extends ValueNode {

    private final String reason;

    public ReasonBearingMissingNode(String reason) {
        this.reason = Objects.requireNonNull(reason);
    }

    public String getReason() {
        return reason;
    }

    @Override
    public final void serialize(JsonGenerator g, SerializerProvider provider)
        throws IOException
    {
        g.writeNull();
    }

    @Override
    public JsonToken asToken() {
        return JsonToken.NOT_AVAILABLE;
    }

    @Override
    public int hashCode() {
        return reason.hashCode();
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
    public String asText() {
        return "";
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
    public String asText(String defaultValue) {
        return defaultValue;
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
