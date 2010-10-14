/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.tools.ui.config;

import javax.swing.JOptionPane;
import java.io.File;
import java.io.IOException;

import org.globus.common.CoGProperties;
import org.globus.tools.ui.util.UITools;

/** This class represents the Java CoG Kit configuration wizard.
 *
 */
public class ConfigurationWizard extends AbstractWizard {

    protected CoGProperties props = null;

    public ConfigurationWizard() {
	setTitle("Java CoG Kit Configuration Wizard");

	props = CoGProperties.getDefault();
	
    	initComponents();
    }

    private void initComponents() {
	addModule( new ConfigModule0(props) );
	addModule( new ConfigModule1(props) );
	
	pack();
	
	addModule( new ConfigModule2(props) );
	addModule( new ConfigModule3(props) );
    }

    public void saveSettings() {

	File cFile = new File(CoGProperties.configFile);
	File directory = cFile.getParentFile();
	
	try {
	    if (!directory.exists()) {
		if (!directory.mkdir()) 
		    throw new IOException("Unable to create directory : " + 
					  directory);
	    }
	    
	    props.remove("internal.usercert");
	    
	    props.save(CoGProperties.configFile);
	} catch(Exception e) {
	    JOptionPane.showMessageDialog(this,
	       "Failed to save the configuration file:\n" + e.getMessage(),
	       "Error Saving Configuration",
	       JOptionPane.ERROR_MESSAGE);
	    
	    return;
	}
	
	JOptionPane.showMessageDialog(this,
	  "The Java CoG Kit is now successfully configured.",
	  "Java CoG Kit Configuration Wizard",
	  JOptionPane.INFORMATION_MESSAGE);
	
	System.exit(0);
    }
    
    public static void main( String[] args ) {
	ConfigurationWizard configWizard = new ConfigurationWizard();
	UITools.center(null, configWizard);
	configWizard.setVisible( true );
    }
}












