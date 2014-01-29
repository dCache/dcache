package dmg.cells.services.login;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;

import java.util.ServiceLoader;

import org.dcache.util.Args;

public class LoginCellFactoryBuilder
{
    private static final ServiceLoader<LoginCellProvider> PROVIDERS =
            ServiceLoader.load(LoginCellProvider.class);

    private String name;
    private Args args;
    private String loginManagerName;

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

    public LoginCellFactory build()
    {
        LoginCellProvider bestProvider =
                Ordering.natural().onResultOf(priorityFor(name)).max(PROVIDERS);
        if (bestProvider.getPriority(name) == Integer.MIN_VALUE) {
            throw new IllegalArgumentException("No login cell provider found for " + name);
        }
        return bestProvider.createFactory(name, args, loginManagerName);
    }

    private static Function<LoginCellProvider, Integer> priorityFor(final String name)
    {
        return new Function<LoginCellProvider, Integer>()
        {
            @Override
            public Integer apply(LoginCellProvider provider)
            {
                return provider.getPriority(name);
            }
        };
    }
}
