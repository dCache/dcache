package org.dcache.gplazma.configuration;

/**require(1), sufficient(1), requisite(2), optional(3)
 *
 * @author timur
 */
public enum  ConfigurationItemControl {
    REQUIRED("required"),
    SUFFICIENT("sufficient"),
    REQUISITE("requisite"),
    OPTIONAL("optional");

    private final String name;
    ConfigurationItemControl(String name) {
        this.name = name;
    }

    public static ConfigurationItemControl getConfigurationItemControl(String name)
            throws IllegalArgumentException {
        if(name == null) {
            throw new NullPointerException(" null name ");
        }

        for(ConfigurationItemControl aConfigurationItemControl: values()) {
            if(aConfigurationItemControl.name.equalsIgnoreCase(name)) {
                return aConfigurationItemControl;
            }
        }
        throw new IllegalArgumentException("Unknown Name ConfigurationItemControl:"+name);
    }

    @Override
    public String toString() {
        return name;
    }
}
