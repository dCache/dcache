package org.dcache.web;

import java.net.URL;
import java.util.Date;

import javax.sql.DataSource;

import org.apache.commons.digester.Digester;
import org.apache.commons.digester.xmlrules.DigesterLoader;

public class DigestTableConfig {

    public static void main( String[] args ) {
        DigestTableConfig tableConfig = new DigestTableConfig("tableRules.xml");
        tableConfig.digest("tableConfig.xml");
    }

    public DigestTableConfig(String rules)
    {
        this.rules = rules;
    }

    public void digest(String config)
    {
        try {
            URL url = this.getClass().getClassLoader().getResource(this.rules);
            System.err.println("Url="+url);  // Debug

            // Create Digester using rules defined in provided file
            Digester digester = DigesterLoader.createDigester(url);

            // Parse the provided configuration file using digester
            tlist = (TableList)digester.parse(this.getClass().getClassLoader().getResourceAsStream(config));

        } catch( Exception e ) {
            e.printStackTrace();
        }
    }

    public void execute(DataSource ds)
    {
//      tlist.execute(ds, new Date(System.currentTimeMillis()).toString());
        tlist.execute(ds, new Date().toString());
    }

    private String rules;
    private TableList tlist;
}
