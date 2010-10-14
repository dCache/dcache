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
package org.globus.tools.proxy;

import java.awt.GridBagConstraints;
import javax.swing.JLabel;

import org.globus.common.CoGProperties;
import org.globus.tools.ui.util.FileBrowser;

public class DefaultProxyOptionsPanel extends ProxyOptionsPanel {

    protected FileBrowser certFileFB, keyFileFB;

    public DefaultProxyOptionsPanel() {
        super();

        setAnchor(GridBagConstraints.EAST);
        gbc.weightx = 0;
        setFill(GridBagConstraints.NONE);

        add(new JLabel("User Certificate: "), 0, 4, 1, 1);

        add(new JLabel("User Private Key: "), 0, 5, 1, 1);

        setAnchor(GridBagConstraints.WEST);

        // this makes the component to fill the entrie available space
        gbc.weightx = 1;

        setFill(GridBagConstraints.HORIZONTAL);

        certFileFB = new FileBrowser("Select User Certificate", "Select");

        add(certFileFB, 1, 4, 1, 1);

        keyFileFB = new FileBrowser("Select User Private Key", "Select");

        add(keyFileFB, 1, 5, 1, 1);
    }

    public boolean validateSettings() {
        if (!super.validateSettings())
            return false;

        if (certFileFB.getFile().equals("")) {
            return error("Please enter the certificate file location");
        } else if (keyFileFB.getFile().equals("")) {
            return error("Please enter the private key file location");
        }
        return true;
    }

    public void set(CoGProperties props) {
        super.set(props);

        certFileFB.setFile(props.getUserCertFile());
        keyFileFB.setFile(props.getUserKeyFile());
    }

    public void get(CoGProperties props) {
        super.get(props);

        props.setUserCertFile(certFileFB.getFile());
        props.setUserKeyFile(keyFileFB.getFile());
    }

}
