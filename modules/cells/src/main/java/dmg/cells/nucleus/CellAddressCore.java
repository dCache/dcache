package dmg.cells.nucleus;

import com.google.common.collect.ComparisonChain;

import java.io.Serializable;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 *
 * Is the core of the CellDomain addressing scheme. The
 * <code>CellAddressCore</code> specifies the name of the cell and the name of
 * the domain the cell exists in.<br>
 * <strong>This Class is designed to be immutable. The whole package relies on
 * that fact. </strong>
 *
 * @author Patrick Fuhrmann
 * @version 0.1,02/15/1998
 *
 *
 */

/*
 * @Immutable
 */
public final class CellAddressCore implements Cloneable, Serializable, Comparable<CellAddressCore> {

    private static final long serialVersionUID = 4072907109959708379L;

    private final String _domain;
    private final String _cell;

    /**
     * Creates a CellAddressCore by scanning the argument string. The syntax can
     * be only one of the following :<br>
     * {@literal cellName} or {@literal cellName@domainName}. If the {@literal domainName} 
     * is omitted, the keyword 'local' is used instead. The specified <code>addr</code>
     * is not checked for existence.
     *
     * @param addr the string representation of cell address.
     */
    public CellAddressCore(String addr) {
        int ind = addr.indexOf('@');
        if (ind < 0) {
            _cell = addr;
            _domain = "local";
        } else {
            _cell = addr.substring(0, ind);
            if (ind == (addr.length() - 1)) {
                _domain = "local";
            } else {
                _domain = addr.substring(ind + 1);
            }
        }
    }

    public CellAddressCore(String addr, String domain) {
        requireNonNull(addr);
        _cell = addr;
        _domain = (domain == null) ? "local" : domain;
    }

    /*
    CellAddressCore getClone(){
       try {
          return (CellAddressCore)this.clone() ;
       }catch( CloneNotSupportedException cnse ){
          return null ;
       }
    }
    */
    public String getCellName() {
        return _cell;
    }

    public String getCellDomainName() {
        return _domain;
    }

    @Override
    public String toString() {
        return _cell + '@' + _domain;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CellAddressCore)) {
            return false;
        }

        CellAddressCore other = (CellAddressCore) obj;
        return other._domain.equals(_domain) && other._cell.equals(_cell);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_cell, _domain);
    }

    @Override
    public int compareTo(CellAddressCore other)
    {
        return ComparisonChain.start()
                .compare(_cell, other._cell)
                .compare(_domain, other._domain)
                .result();
    }

    public boolean isDomainAddress()
    {
        return _cell.equals("*");
    }

    public boolean isLocalAddress()
    {
        return _domain.equals("local");
    }

    /* REVISIT: The class is in a transition phase to allow _domain to be null for
     * unqualified cell addresses. After the next golden release, this class can be
     * adjusted to use null rather than 'local' and reversing the role of readResolve.
     */
    public Object readResolve()
    {
        return (_domain == null) ? new CellAddressCore(_cell, null) : this;
    }
}
