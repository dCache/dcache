package org.dcache.gplazma.strategies;

/**
 *
 * @author timur
 */
public class DefaultStrategyFactory extends StrategyFactory {

    @Override
    public AccountStrategy newAccountStrategy() {
        return new DefaultAccountStrategy();
    }

    @Override
    public AuthenticationStrategy newAuthenticationStrategy() {
        return new DefaultAuthenticationStrategy();
    }

    @Override
    public MappingStrategy newMappingStrategy() {
        return new DefaultMappingStrategy();
    }

    @Override
    public SessionStrategy newSessionStrategy() {
        return new DefaultSessionStrategy();
    }

    @Override
    public IdentityStrategy newIdentityStrategy() {
        return new DefaultIdentityStrategy();
    }

}
