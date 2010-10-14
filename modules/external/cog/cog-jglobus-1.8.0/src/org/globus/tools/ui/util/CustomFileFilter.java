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

import java.io.File;

public class CustomFileFilter extends javax.swing.filechooser.FileFilter {
    
    private String description;
    private String extension;

    public CustomFileFilter(String ext, String desc) {
	extension = ext;
	description = desc;
    }

    public boolean accept(File f) {

	if(f.isDirectory()) {
	    return true;
	}

	String s = f.getName().toLowerCase();
	if (s.endsWith(extension)) {
	    return true;
	} else {
	    return false;
	}

    }
  
    public String getDescription() {
	return description;
    }
}
