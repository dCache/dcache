/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

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
    public JsonParser.NumberType getNumberType()
    {
        return delegate().getNumberType();
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
    public boolean equals(Object o)
    {
        return delegate().equals(o);
    }
}
