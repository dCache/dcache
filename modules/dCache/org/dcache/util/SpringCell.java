package org.dcache.util;

import dmg.cells.nucleus.CellAdapter;
import dmg.util.Args;
import java.util.Dictionary;
import java.util.Properties;
import java.util.Enumeration;
import java.io.ByteArrayOutputStream;
import java.io.File;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;

/**
 * Helper cell to bridge between the Cell framework and the Spring
 * framework.
 *
 * This cell can be instantiated from a Cell batch file and takes a
 * Spring XML configuration file as an argument, for instance:
 *
 * {@literal
 * create org.dcache.util.SpringCell spring*  \
 *   "file:${ourHomeDir}/config/pool.xml"
 * }
 *
 * A ClassPathXmlApplicationContext is instantiated for this XML
 * file. Based on the content of the XML file, Spring can instantiate
 * other cells.
 *
 * The domain context and the cell arguments are made available as a
 * Spring resource called 'domaincontext:' in Java Properties property
 * file format. This can be used for setting up placeholder
 * substitution in the Spring container, for instance:
 *
 * {@literal
 * <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer">
 *   <property name="location" value="domaincontext:"/>
 * </bean>
 * }
 *
 * Options on the form '-key=value' are made available as 'key=value'
 * properties.  The cell argument string is made available through a
 * property with the key 'arguments'. This can be used to pass the
 * argument string on to beans. The XML file resource name and any
 * unresolved Cell placeholders are stripped from the argument string.
 */
public class SpringCell extends CellAdapter
{
    private void mergeDictionary(Properties properties, Dictionary dictionary)
    {
        Enumeration e = dictionary.keys();
        while (e.hasMoreElements()) {
            Object key = e.nextElement();
            Object value = dictionary.get(key);
            properties.setProperty(key.toString(),value.toString());
        }
    }

    public SpringCell(String name, String arg) throws Exception
    {
	super(name, arg, true);
        try {
            Args args = getArgs();
            String setup = args.argv(0);
            args.shift();

            /* Merge domain context and options to a set of
             * properties.
             */
            Properties properties = new Properties();

            String arguments =
                args.toString().replaceAll("-?\\$\\{.*\\}", "");

            properties.setProperty("arguments", arguments);
            mergeDictionary(properties, getDomainContext());
            mergeDictionary(properties, args.options());

            /* Convert to byte array form such that we can make it
             * available as a Spring resource.
             */
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            properties.store(out, "");
            final byte[] conf = out.toByteArray();

            /* Create the application context with the configuration
             * available as a Spring resource.
             */
            ApplicationContext context =
                new ClassPathXmlApplicationContext(setup)
                {
                    public Resource getResource(String location)
                    {
                        if (location.startsWith("domaincontext:")) {
                            return new ByteArrayResource(conf) {
                                /** Fake file name to make
                                 * PropertyPlaceholderConfigurer
                                 * happy.
                                 */
                                public String getFilename()
                                {
                                    return "domaincontext.properties";
                                }
                            };
                        } else {
                            return super.getResource(location);
                        }
                    }
                };
        } finally {
            kill();
        }
    }
}