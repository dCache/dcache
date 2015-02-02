package diskCacheV111.vehicles;

public class PoolManagerGetPoolsByLinkMessage
    extends PoolManagerGetPoolsMessage
{
    private static final long serialVersionUID = 1860087087699860426L;

    private final String _link;

    public PoolManagerGetPoolsByLinkMessage(String link)
    {
        _link = link;
    }

    public String getLink()
    {
        return _link;
    }
}
