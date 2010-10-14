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
import java.awt.Font;
import javax.swing.JLabel;
import javax.swing.SwingConstants;

import org.globus.common.CoGProperties;
import org.globus.tools.ui.swing.VerticalLabelUI;

public abstract class BaseModule extends EJPanel 
    implements ConfigurationModule {
    
    protected CoGProperties props;
    
    protected Font font;
    
    public BaseModule(CoGProperties props) {
	this.props = props;
	
	setInsets(10, 10, 10, 10);
	
	JLabel label = new JLabel();
	font    = label.getFont();  // default font used
	
	JLabel l = new JLabel(SIDE_TITLE, SwingConstants.LEFT );
	l.setUI( new VerticalLabelUI(false) );
	l.setForeground(Color.black);
	l.setFont(getFont(font, 15));
	
	add(l, 
	    0, 0, 1, 5);
	
	setInsets(1, 1, 1, 1);
    }
    
    public abstract void saveSettings();
    
    public boolean verifySettings() {
	return true;
    }
    
}


