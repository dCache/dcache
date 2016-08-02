package diskCacheV111.vehicles;

import java.util.Arrays;
import java.util.List;

import dmg.cells.nucleus.CellAddressCore;

public class IoDoorInfo extends DoorInfo
{
    private static final long serialVersionUID = 33390606479807121L;

    public IoDoorInfo(String cellName, String cellDomainName)
    {
        super(cellName, cellDomainName);
    }

    public IoDoorInfo(CellAddressCore address)
    {
        super(address.getCellName(), address.getCellDomainName());
    }

    public void setIoDoorEntries(IoDoorEntry[] entries)
    {
        setDetail(Arrays.copyOf(entries, entries.length));
    }

    public List<IoDoorEntry> getIoDoorEntries()
    {
        return Arrays.asList((IoDoorEntry[]) getDetail());
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());

        IoDoorEntry[] entries = (IoDoorEntry[]) getDetail();
        if (entries.length > 0) {
            sb.append('\n');
        }
        for (IoDoorEntry entry : entries) {
            sb.append(entry.toString()).append('\n');
        }
        return sb.toString();
    }
}
