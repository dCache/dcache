package dmg.util;

import java.util.Properties;

/**
 * Properties subclass which uses a Replaceable as a repository for default values.
 * <p>
 * Not technically an adapter.
 */
public class ReplaceableBackedProperties extends Properties {

    private static final long serialVersionUID = 6635498670855942285L;
    private final Replaceable _replaceable;

    public ReplaceableBackedProperties(Replaceable replaceable) {
        _replaceable = replaceable;
    }

    @Override
    public String getProperty(String key) {
        String value = super.getProperty(key);
        return (value != null) ? value : _replaceable.getReplacement(key);
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String value = this.getProperty(key);
        return (value != null) ? value : defaultValue;
    }
}
