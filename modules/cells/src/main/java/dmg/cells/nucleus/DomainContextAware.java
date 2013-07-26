package dmg.cells.nucleus;

import java.util.Map;

public interface DomainContextAware
{
    void setDomainContext(Map<String,Object> context);
}
