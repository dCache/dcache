package org.dcache.services.info.gathers.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StateValue;
import org.dcache.services.info.base.StringStateValue;
import org.dcache.services.info.gathers.CellMessageHandlerSkel;
import org.dcache.services.info.gathers.MessageMetadataRepository;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class handles reply messages from the ASCII command
 * "show context info.static".  The value is parsed and dCache's state is
 * updated accordingly.
 */
public class StaticDomainMsgHandler extends CellMessageHandlerSkel
{
    private static final Logger _log =
            LoggerFactory.getLogger(StaticDomainMsgHandler.class);

    private static final StatePath DOMAINS = StatePath.parsePath("domains");

    public StaticDomainMsgHandler(StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgMetaRepo)
    {
        super(sum, msgMetaRepo);
    }

    @Override
    public void process(Object payload, long lifetime)
    {
        if (payload == null) {
            _log.error("received null payload");
            return;
        }

        if (!(payload instanceof String)) {
            _log.error("received message of type {}",
                    payload.getClass().getCanonicalName());
            return;
        }

        String declaration = (String) payload;

        StatePath parent = metricsParent();
        StateUpdate update = processDeclaration(parent, lifetime, declaration);

        applyUpdates(update);
    }


    private StatePath metricsParent()
    {
        return DOMAINS.newChild(getDomain()).newChild("static");
    }


    private StateUpdate processDeclaration(StatePath parent, long lifetime,
            String declaration)
    {
        StateUpdate update = new StateUpdate();

        update.purgeUnder(parent);

        for (String rawLine : declaration.split("\n")) {
            String line = rawLine.trim();

            if (line.isEmpty()) {
                continue;
            }

            try {
                processLine(update, lifetime, parent, line);
            } catch (IllegalArgumentException e) {
                _log.error(e.getMessage());
            }
        }

        return update;
    }


    /**
     * Process a line of the format {@literal <type><sep><name><sep><data>}
     * where {@literal <type>} and {@literal <sep>} are single characters.
     */
    private void processLine(StateUpdate update, long lifetime,
            StatePath parent, String line)
    {
        checkArgument(line.length() >= 5, "Line too short: " + line);

        char type = line.charAt(0);
        char seperator = line.charAt(1);

        int idx = line.indexOf(seperator, 3);
        checkArgument(idx != -1, "Seperator character '" + seperator +
                "' missing");
        checkArgument(idx != line.length()-1, "Metric has no data");

        String value = line.substring(idx+1);
        StateValue metric = metricFor(type, value, lifetime);

        String name = line.substring(2, idx);
        StatePath relativePath = StatePath.parsePath(name);
        StatePath path = parent.newChild(relativePath);

        update.appendUpdate(path, metric);
    }


    private static StateValue metricFor(char type, String value, long lifetime)
    {
        switch(type) {
            case 'S':
                return new StringStateValue(value, lifetime);
            default:
                throw new IllegalArgumentException("Unknown type: " + type);
        }
    }
}
