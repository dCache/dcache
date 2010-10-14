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
import javax.swing.JLabel;

import org.globus.common.CoGProperties;

public class ConfigModule0 extends BaseModule {
    
    public ConfigModule0(CoGProperties props) {
	super(props);
	
	setInsets(1, 1, 1, 1);
	
	JLabel label = 
	    new JLabel("Welcome to the Java CoG Kit installation wizard");
	label.setFont(getFont(font, 4));
	label.setForeground(Color.black);
	
	add(label,
	    1, 0, 1, 1);
	
	label = new JLabel("This wizard helps to configure Java CoG Kit installation.");
	label.setFont(getFont(font, 2));
	label.setForeground(Color.black);
	
	add(label,
	    1, 1, 1, 1);
    }
    
    public void saveSettings() {
    }
    
}


