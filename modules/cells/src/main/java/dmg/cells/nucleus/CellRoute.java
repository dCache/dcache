package dmg.cells.nucleus;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/*
* route add -default             <cell>[@<domain>]
* route add -domain  <domain>    <cell>[@<domain>]
*   WARNING : This Class is designed to be immutable.
*             All other class rely on that fact and
*             a lot of things may fail at runtime
*             if this design item is changed.
*/
public class CellRoute implements Serializable
{
    private static final long serialVersionUID = 4566260400288960984L;

    private final String _destCell;
    private final String _destDomain;
    private final CellAddressCore _gateway;
    private final int _type;
    // as Optional class is not serializable use raw String
    private final String _zone;

    public static final int AUTO = 0;
    public static final int EXACT = 1;
    public static final int QUEUE = 2;
    public static final int DOMAIN = 3;
    public static final int DEFAULT = 4;
    public static final int DUMPSTER = 5;
    public static final int ALIAS = 6;
    public static final int TOPIC = 7;

    // FIXME this should be an enum.
    // See CellShell#RouteCommand
    static final String[] TYPE_NAMES =
            {"Auto", "Exact", "Queue", "Domain",
                    "Default", "Dumpster", "Alias", "Topic"};

    public CellRoute(String dest, CellAddressCore gateway, Optional<String> zone, int type)
            throws IllegalArgumentException
    {
        _gateway = gateway;
        _zone = zone.orElse(null);

        String cell, domain;
        if (dest == null || dest.isEmpty()) {
            cell = null;
            domain = null;
        } else {
            int ind = dest.indexOf('@');
            if (ind < 0) {
                cell = dest;
                domain = null;
            } else {
                cell = dest.substring(0, ind);
                if (ind == (dest.length() - 1)) {
                    domain = null;
                } else {
                    domain = dest.substring(ind + 1);
                }
            }
        }

        switch (type) {
        case EXACT:
        case ALIAS:
            checkArgument(cell != null, "No destination cell spec.");
            _destCell = cell;
            _destDomain = (domain == null) ? "local" : domain;
            _type = type;
            break;
        case QUEUE:
            checkArgument(cell != null, "No destination cell spec.");
            checkArgument(domain == null, "QUEUE doesn't accept domain");
            _destCell = cell;
            _destDomain = "*";
            _type = type;
            break;
        case TOPIC:
            checkArgument(cell != null, "No destination cell spec.");
            checkArgument(domain == null, "TOPIC doesn't accept domain");
            _destCell = cell;
            _destDomain = "*";
            _type = type;
            break;
        case DOMAIN:
            checkArgument(domain == null, "DOMAIN doesn't accept cell");
            checkArgument(cell != null, "No destination domain spec.");
            _destDomain = cell;
            _destCell = "*";
            _type = type;
            break;
        case DUMPSTER:
            checkArgument(cell == null, "DUMPSTER doesn't accept cell");
            checkArgument(domain == null, "DUMPSTER doesn't accept domain");
            _destDomain = "*";
            _destCell = "*";
            _type = type;
            break;
        case DEFAULT:
            checkArgument(cell == null, "DEFAULT doesn't accept cell");
            checkArgument(domain == null, "DEFAULT doesn't accept domain");
            _destDomain = "*";
            _destCell = "*";
            _type = type;
            break;
        case AUTO:
            if ((cell != null) && (domain != null)) {
                if (cell.equals("*") && domain.equals("*")) {
                    _type = DEFAULT;
                } else if (cell.equals("*")) {
                    _type = DOMAIN;
                } else if (domain.equals("*")) {
                    _type = QUEUE;
                } else {
                    _type = EXACT;
                }
                _destCell = cell;
                _destDomain = domain;
            } else if (domain != null) {
                _destCell = "*";
                _destDomain = domain;
                _type = DOMAIN;
            } else if (cell != null) {
                _destCell = cell;
                _destDomain = "*";
                _type = QUEUE;
            } else {
                _destCell = "*";
                _destDomain = "*";
                _type = DEFAULT;
            }
            break;
        default:
            throw new IllegalArgumentException("Unknown Route type");
        }
    }

    public String getCellName()
    {
        return _destCell;
    }

    public String getDomainName()
    {
        return _destDomain;
    }

    public int getRouteType()
    {
        return _type;
    }

    public CellAddressCore getTarget()
    {

        return _gateway;
    }

    public Optional<String> getZone() {
        return Optional.ofNullable(_zone);
    }

    public String getRouteTypeName()
    {
        return TYPE_NAMES[_type];
    }

    @Override
    public int hashCode()
    {
        return _type ^ Objects.hash(_destCell, _destDomain, _gateway);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (!(o instanceof CellRoute)) {
            return false;
        }
        CellRoute route = (CellRoute) o;
        return route._destCell.equals(_destCell) &&
               route._destDomain.equals(_destDomain) &&
               route._gateway.equals(_gateway) &&
               route._type == _type &&
               Objects.equals(route._zone, _zone);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("cell", getCellName())
                .add("domain", getDomainName())
                .add("gateway", getTarget())
                .add("type", getRouteTypeName())
                .add("zone", _zone == null? "Undefined" : _zone)
                .toString();
    }
}

