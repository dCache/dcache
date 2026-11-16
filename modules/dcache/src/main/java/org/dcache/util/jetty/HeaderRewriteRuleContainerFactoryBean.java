package org.dcache.util.jetty;

import com.google.common.collect.ImmutableMap;
import javax.annotation.PostConstruct;
import org.eclipse.jetty.rewrite.handler.HeaderPatternRule;
import org.eclipse.jetty.rewrite.handler.RuleContainer;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * Spring factory bean for creating a Jetty rewriting Rule where all configuration with some prefix
 * are always set in the response header.
 */
public class HeaderRewriteRuleContainerFactoryBean implements FactoryBean<RuleContainer> {

    private ImmutableMap<String, String> _headers;
    private final RuleContainer _rule = new RuleContainer();

    @Required
    public void setHeaders(ImmutableMap<String, String> headers) {
        _headers = headers;
    }

    @PostConstruct
    private void buildMap() {
        _headers.forEach((name, value) ->
              _rule.addRule(new HeaderPatternRule("/*", name, value)));
    }

    @Override
    public RuleContainer getObject() throws Exception {
        return _rule;
    }


    @Override
    public Class<?> getObjectType() {
        return RuleContainer.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
