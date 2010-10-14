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
import java.awt.Insets;
import java.awt.Font;
import java.io.File;

import org.globus.tools.ui.util.JJPanel;

public class EJPanel extends JJPanel {

    public static final String SIDE_TITLE = "Java CoG Kit";

    public boolean checkFile(String file, String name) {
	String msg = null;
	if (file.length() == 0) {
	    msg = name + " must be specified.";
	} else {
	    File f = new File(file);
	    if (!f.exists()) {
		msg = name + " file not found.";
	    }
	}
	
	if (msg != null) {
	    JOptionPane.showMessageDialog(this, msg, name + " Error",
					  JOptionPane.ERROR_MESSAGE);
	    return false;
	}
	
	return true;
    }

    public Insets getInsets() {
	return new Insets(5, 5, 5, 5);
    }
    
    protected Font getFont(Font srcFont, int sizeDiff) {
	return new Font(srcFont.getName(),
			srcFont.getStyle(),
			srcFont.getSize() + sizeDiff);
    }
}
