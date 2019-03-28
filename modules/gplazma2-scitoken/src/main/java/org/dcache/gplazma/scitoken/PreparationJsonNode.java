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
 *  A JsonNode that executes some common task before calling the delegate.
 */
public abstract class PreparationJsonNode extends ForwardingJsonNode
{
    protected abstract void prepare();

    @Override
    public JsonToken asToken()
    {
        prepare();
        return super.asToken();
    }

    @Override
    public JsonParser.NumberType getNumberType()
    {
        prepare();
        return super.getNumberType();
    }

    @Override
    public JsonNode get(int index)
    {
        prepare();
        return super.get(index);
    }

    @Override
    public JsonNode get(String fieldName)
    {
        prepare();
        return super.get(fieldName);
    }

    @Override
    public String asText()
    {
        prepare();
        return super.asText();
    }

    @Override
    public JsonNode findValue(String fieldName)
    {
        prepare();
        return super.findValue(fieldName);
    }

    @Override
    public JsonNode findPath(String fieldName)
    {
        prepare();
        return super.findPath(fieldName);
    }

    @Override
    public JsonNode findParent(String fieldName)
    {
        prepare();
        return super.findParent(fieldName);
    }

    @Override
    public List<JsonNode> findValues(String fieldName, List<JsonNode> foundSoFar)
    {
        prepare();
        return super.findValues(fieldName, foundSoFar);
    }

    @Override
    public List<String> findValuesAsText(String fieldName, List<String> foundSoFar)
    {
        prepare();
        return super.findValuesAsText(fieldName, foundSoFar);
    }

    @Override
    public List<JsonNode> findParents(String fieldName, List<JsonNode> foundSoFar)
    {
        prepare();
        return super.findParents(fieldName, foundSoFar);
    }

    @Override
    public JsonNode path(String fieldName)
    {
        prepare();
        return super.path(fieldName);
    }

    @Override
    public JsonNode path(int index)
    {
        prepare();
        return super.path(index);
    }

    @Override
    public JsonParser traverse()
    {
        prepare();
        return super.traverse();
    }

    @Override
    public String toString()
    {
        prepare();
        return super.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        prepare();
        return super.equals(o);
    }
}
