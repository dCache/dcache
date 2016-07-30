package dmg.cells.services.login;

import com.google.common.collect.Ordering;

import java.util.ServiceLoader;

import dmg.cells.nucleus.CellEndpoint;

import org.dcache.util.Args;

public class LoginCellFactoryBuilder
{
    private static final ServiceLoader<LoginCellProvider> PROVIDERS =
            ServiceLoader.load(LoginCellProvider.class);

    private String name;
    private Args args;
    private String loginManagerName;
    private CellEndpoint endpoint;

    public LoginCellFactoryBuilder setName(String name)
    {
        this.name = name;
        return this;
    }

    public LoginCellFactoryBuilder setArgs(Args args)
    {
        this.args = args;
        return this;
    }

    public LoginCellFactoryBuilder setLoginManagerName(String loginManagerName)
    {
        this.loginManagerName = loginManagerName;
        return this;
    }

    public LoginCellFactoryBuilder setCellEndpoint(CellEndpoint endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public LoginCellFactory build()
    {
        LoginCellProvider bestProvider =
                Ordering.natural().onResultOf((LoginCellProvider p) -> p.getPriority(name)).max(PROVIDERS);
        if (bestProvider.getPriority(name) == Integer.MIN_VALUE) {
            throw new IllegalArgumentException("No login cell provider found for " + name);
        }
        return bestProvider.createFactory(name, args, endpoint, loginManagerName);
    }

}
