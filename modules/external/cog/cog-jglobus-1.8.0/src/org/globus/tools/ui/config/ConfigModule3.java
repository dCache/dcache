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

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import javax.swing.JLabel;
import javax.swing.JOptionPane;

import org.globus.util.Util;
import org.globus.common.CoGProperties;
import org.globus.tools.ui.swing.MultiLineLabelUI;
import org.globus.tools.ui.util.FileBrowser;

public class ConfigModule3 extends BaseModule {

    private FileBrowser proxyfile;
    
    public ConfigModule3(CoGProperties props) {
	super(props);

	JLabel label = new JLabel("Configuring Proxy File");
	label.setFont(getFont(font, 1));
	label.setForeground(Color.black);
	
	add(label,
	    1, 1, 1, 1);
	
	label = new JLabel(" \nPlease make sure to set proper access rights\n" +
			   "to the proxy file.\n  ");
	label.setUI( new MultiLineLabelUI() );
	label.setFont(getFont(font, 1));

	add(label,
	    1, 2, 1, 1);
	
	proxyfile = new FileBrowser("Open Proxy File", 
				    "Proxy File: ",
				    "Select");
	proxyfile.setFile( props.getProxyFile() );

	add(proxyfile,
	    1, 3, 1, 1);
    }

    public void saveSettings() {
	props.setProxyFile( proxyfile.getFile() );
    }

    public boolean verifySettings() {
	
	String msg  = null;
	String file = proxyfile.getFile();
	
	if (file.length() == 0) {
	    msg = "Proxy file must be specified.";
	} else {
	    File f = new File(file);
	    if (f.exists()) {
		if (f.isDirectory()) {
		    msg = "Selected file is a directory.";
		} else if (!f.canWrite()) {
		    msg = "Cannot write to the selected file.";
		} else if (!f.canRead()) {
		    msg = "Cannot read from the selected file.";
		}
	    } else {
		try {
		    f.createNewFile();
		    Util.setOwnerAccessOnly(f.getAbsolutePath());
		} catch(IOException e) {
		    msg = "Unable to create selected file : " + e.getMessage();
		}
	    }
	}
	
	if (msg != null) {
	    JOptionPane.showMessageDialog(this, msg, "Proxy File Error",
					  JOptionPane.ERROR_MESSAGE);
	    return false;
	}
	
	return true;
    }

}

