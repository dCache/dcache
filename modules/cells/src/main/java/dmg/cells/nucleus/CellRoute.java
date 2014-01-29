package dmg.cells.nucleus;

import com.google.common.base.Objects;

import java.io.Serializable;

import dmg.util.Formats;

import org.dcache.util.Args;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;

/**
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 */
/*
* route add -default             <cell>[@<domain>]
* route add -domain  <domain>    <cell>[@<domain>]
*   WARNING : This Class is designed to be imutual.
*             All other class rely on that fact and
*             a lot of things may fail at runtime
*             if this design item is changed.
*/
public class CellRoute implements Cloneable, Serializable
{
    private static final long serialVersionUID = 4566260400288960984L;

    private final String _destCell;
    private final String _destDomain;
    private final String _gateway;
    private final int _type;

    public static final int AUTO = 0;
    public static final int EXACT = 1;
    public static final int WELLKNOWN = 2;
    public static final int DOMAIN = 3;
    public static final int DEFAULT = 4;
    public static final int DUMPSTER = 5;
    public static final int ALIAS = 6;

    private final static String[] __typeNames =
            {"Auto", "Exact", "Wellknown", "Domain",
                    "Default", "Dumpster", "Alias"};

    private static int getTypeOf(Args args)
    {
        if (args.argc() == 0) {
            throw new IllegalArgumentException("Not enough arguments");
        }

        String opt = (args.optc() == 0) ? "-auto" : args.optv(0);
        int type = AUTO;
        switch (opt) {
        case "auto":
            type = AUTO;
            break;
        case "domain":
            type = DOMAIN;
            break;
        case "wellknown":
            type = WELLKNOWN;
            break;
        case "exact":
            type = EXACT;
            break;
        case "default":
            type = DEFAULT;
            break;
        case "dumpster":
            type = DUMPSTER;
            break;
        case "alias":
            type = ALIAS;
            break;
        }

        if (args.argc() == 1) {
            if ((type != DEFAULT) && (type != DUMPSTER)) {
                throw new IllegalArgumentException("Not enough arguments");
            }
        } else if (args.argc() == 2) {
            if ((type == DEFAULT) || (type == DUMPSTER)) {
                throw new IllegalArgumentException("Too many arguments");
            }
        } else {
            throw new IllegalArgumentException("Too many arguments");
        }

        return type;
    }

    private static int getTypeOf(String type)
    {
        int i = asList(__typeNames).indexOf(type);
        if (i == -1) {
            throw new IllegalArgumentException("Illegal Route Type: " + type);
        }
        return i;
    }

    public CellRoute(Args args)
            throws IllegalArgumentException
    {
        this((args.argc() != 2) ? null : args.argv(0),
                (args.argc() != 2) ? args.argv(0) : args.argv(1),
                getTypeOf(args));
    }

    public CellRoute(String dest, String gateway, String type)
            throws IllegalArgumentException
    {
        this(dest, gateway, getTypeOf(type));
    }

    public CellRoute(String dest, String gateway, int type)
            throws IllegalArgumentException
    {
        _gateway = gateway;

        String cell, domain;
        if ((dest == null) || (dest.equals(""))) {
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
        case WELLKNOWN:
            checkArgument(cell != null, "No destination cell spec.");
            checkArgument(domain == null, "WELLKNOWN doesn't accept domain");
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
                    _type = WELLKNOWN;
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
                _type = WELLKNOWN;
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

    public String getTargetName()
    {
        return _gateway;
    }

    public int getRouteType()
    {
        return _type;
    }

    public CellAddressCore getTarget()
    {

        return new CellAddressCore(_gateway);
    }

    public String getRouteTypeName()
    {
        return __typeNames[_type];
    }

    public int hashCode()
    {
        return Objects.hashCode(_destCell, _destDomain);
    }

    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (!(o instanceof CellRoute)) {
            return false;
        }
        CellRoute route = (CellRoute) o;
        return (route._destCell.equals(_destCell)) &&
                (route._destDomain.equals(_destDomain));
    }

    private static final int _destLength = 15;
    private static final int _domainLength = 15;
    private static final int _gateLength = 25;

    public static String headerToString()
    {
        return Formats.field("Dest Cell", _destLength, Formats.CENTER) +
                Formats.field("Dest Domain", _domainLength, Formats.CENTER) +
                Formats.field("Gateway", _gateLength, Formats.CENTER) +
                Formats.field("Type", 10, Formats.CENTER);
    }

    public String toString()
    {
        return Formats.field(_destCell, _destLength, Formats.CENTER) +
                Formats.field(_destDomain, _domainLength, Formats.CENTER) +
                Formats.field(_gateway, _gateLength, Formats.CENTER) +
                Formats.field(__typeNames[_type], 10, Formats.LEFT);
    }
}

