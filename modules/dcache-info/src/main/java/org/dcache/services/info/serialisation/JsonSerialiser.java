package org.dcache.services.info.serialisation;

import com.google.common.base.Objects;
import com.google.gson.stream.JsonWriter;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StringStateValue;

import static com.google.common.base.Throwables.propagate;

/**
 * Serialise state into a json object.
 */
public class JsonSerialiser extends SubtreeVisitor implements StateSerialiser
{
    public static final String NAME = "json";
    private StateExhibitor _exhibitor;

    private JsonWriter _writer;
    private StringWriter _string;
    private StatePath _top;

    @Required
    public void setStateExhibitor(StateExhibitor exhibitor)
    {
        _exhibitor = exhibitor;
    }

    @Override
    public void visitString(StatePath path, StringStateValue metric)
    {
        try {
            _writer.name(path.getLastElement()).value(metric.getValue());
        } catch (IOException e) {
            propagate(e);
        }
    }

    @Override
    public void visitInteger(StatePath path, IntegerStateValue metric)
    {
        try {
            _writer.name(path.getLastElement()).value(metric.getValue());
        } catch (IOException e) {
            propagate(e);
        }
    }

    @Override
    public void visitBoolean(StatePath path, BooleanStateValue metric)
    {
        try {
            _writer.name(path.getLastElement()).value(metric.getValue());
        } catch (IOException e) {
            propagate(e);
        }
    }

    @Override
    public void visitFloatingPoint(StatePath path, FloatingPointStateValue metric)
    {
        try {
            _writer.name(path.getLastElement()).value(metric.getValue());
        } catch (IOException e) {
            propagate(e);
        }
    }

    @Override
    public void visitCompositePreDescend(StatePath path, Map<String, String> metadata)
    {
        if (!isInsideScope(path) || Objects.equal(_top, path)) {
            return;
        }

        try {
            _writer.name(path.getLastElement());
            _writer.beginObject();
        } catch (IOException e) {
            propagate(e);
        }
    }

    @Override
    public void visitCompositePostDescend(StatePath path, Map<String, String> metadata)
    {
        if (!isInsideScope(path) || Objects.equal(_top, path)) {
            return;
        }

        try {
            _writer.endObject();
        } catch (IOException e) {
            propagate(e);
        }
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public String serialise()
    {
        return serialise(null);
    }

    @Override
    public String serialise(StatePath start)
    {
        _top = start;

        _string = new StringWriter();
        _writer = new JsonWriter(_string);
        _writer.setIndent("  ");

        if (start != null) {
            setVisitScopeToSubtree(start);
        } else {
            setVisitScopeToEverything();
        }

        try {
            _writer.beginObject();
            _exhibitor.visitState(this);
            _writer.endObject();

            _writer.flush();
            _string.append('\n');
            _writer.close();
        } catch (IOException e) {
            propagate(e);
        }

        return _string.toString();
    }
}
