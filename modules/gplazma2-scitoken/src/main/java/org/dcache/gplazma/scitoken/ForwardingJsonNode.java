/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 - 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.scitoken;

import com.fasterxml.jackson.core.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.node.JsonNodeType;

import java.io.IOException;
import java.util.List;

/**
 * A JsonNode that delegates all operations to some other JsonNode.
 */
public abstract class ForwardingJsonNode extends JsonNode
{
    protected abstract JsonNode delegate();

    @Override
    public JsonToken asToken()
    {
        return delegate().asToken();
    }

    @Override
    public JsonParser.NumberType numberType()
    {
        return delegate().numberType();
    }

    @Override
    public JsonNode get(int index)
    {
        return delegate().get(index);
    }

    @Override
    public JsonNode get(String fieldName)
    {
        return delegate().get(fieldName);
    }

    @Override
    public String asText()
    {
        return delegate().asText();
    }

    @Override
    public JsonNode findValue(String fieldName)
    {
        return delegate().findValue(fieldName);
    }

    @Override
    public JsonNode findPath(String fieldName)
    {
        return delegate().findPath(fieldName);
    }

    @Override
    public JsonNode findParent(String fieldName)
    {
        return delegate().findParent(fieldName);
    }

    @Override
    public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar)
    {
        return delegate().findValues(fieldName, foundSoFar);
    }

    @Override
    public List<String> findValuesAsText(String fieldName, List<String> foundSoFar)
    {
        return delegate().findValuesAsText(fieldName, foundSoFar);
    }

    @Override
    public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar)
    {
        return delegate().findParents(fieldName, foundSoFar);
    }

    @Override
    public JsonNode path(String fieldName)
    {
        return delegate().path(fieldName);
    }

    @Override
    public JsonNode path(int index)
    {
        return delegate().path(index);
    }

    @Override
    public JsonParser traverse()
    {
        return delegate().traverse();
    }

    @Override
    public String toString()
    {
        return delegate().toString();
    }

    @Override
    public <T extends JsonNode> T deepCopy() {
        return delegate().deepCopy();
    }

    @Override
    public boolean equals(Object o)
    {
        return delegate().equals(o);
    }

    @Override
    protected JsonNode _at(JsonPointer jsonPointer) {
        return null;
    }

    @Override
    public JsonNodeType getNodeType() {
        return delegate().getNodeType();
    }

    @Override
    public JsonParser traverse(ObjectCodec objectCodec) {
        return delegate().traverse(objectCodec);
    }

    @Override
    public void serialize(JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        delegate().serialize(jsonGenerator,serializerProvider);
    }

    @Override
    public void serializeWithType(JsonGenerator jsonGenerator, SerializerProvider serializerProvider, TypeSerializer typeSerializer) throws IOException {
        delegate().serializeWithType(jsonGenerator, serializerProvider, typeSerializer);
    }
}
