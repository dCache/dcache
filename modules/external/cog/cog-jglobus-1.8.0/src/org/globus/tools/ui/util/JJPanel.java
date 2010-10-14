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
package org.globus.tools.ui.util;

import java.awt.GridBagLayout;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import javax.swing.JPanel;
import javax.swing.JComponent;

public class JJPanel extends JPanel {
    
    protected GridBagLayout gbl;
    public GridBagConstraints gbc;
    
    public JJPanel() {
	gbl = new GridBagLayout();
	gbc = new GridBagConstraints();
	setLayout(gbl);
    }

    public void add(JComponent c, int x, int y, int w, int h) {
	gbc.gridx      = x;
	gbc.gridy      = y;
	gbc.gridwidth  = w;
	gbc.gridheight = h;
	
	gbl.setConstraints(c, gbc);
	
	add(c);
    }
    
    public void setAnchor(int anchor) {
	gbc.anchor = anchor;
    }
    
    public void setFill(int fill) {
	gbc.fill = fill;
    }
    
    public void setInsets(int a, int b, int c, int d) {
	gbc.insets = new Insets(a, b, c, d);
    }
    
}
