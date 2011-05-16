package dmg.cells.nucleus;

import java.util.Map;

public interface EnvironmentAware
{
    void setEnvironment(Map<String,Object> environment);
}