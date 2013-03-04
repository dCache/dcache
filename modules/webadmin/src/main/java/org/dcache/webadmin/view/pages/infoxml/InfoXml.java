package org.dcache.webadmin.view.pages.infoxml;

import org.apache.wicket.markup.html.basic.MultiLineLabel;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.request.cycle.RequestCycle;
import org.apache.wicket.request.handler.resource.ResourceStreamRequestHandler;
import org.apache.wicket.util.resource.StringResourceStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.webadmin.controller.InfoService;
import org.dcache.webadmin.controller.exceptions.InfoServiceException;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.util.DefaultFocusBehaviour;

/**
 * Page to do statepath requests to the infoservice in the webinterface and
 * provide a possibility to download the xml via this.
 * @author jans
 */
public class InfoXml extends BasePage {

    private static final Logger _log = LoggerFactory.getLogger(InfoXml.class);
    private static final long serialVersionUID = 8995842443972983951L;
    private TextField<String> _statepathField;
    private String _statepath;
    private String _xmlOutput;

    public InfoXml() {
        addMarkup();
    }

    private void addMarkup() {
        Form form = new Form("requestInfoXmlForm");
        form.add(new FeedbackPanel("feedback"));
        Button submitButton = new Button("submit") {

            private static final long serialVersionUID = -5872234705322961924L;

            @Override
            public void onSubmit() {
                try {
                    String statepath = getCorrectedStatepath(_statepath);
                    _log.debug(_statepath);
                    _xmlOutput = getInfoService().getXmlForStatepath(statepath);
                } catch (InfoServiceException ex) {
                    error(getStringResource("error.inforetrieval") + ex.getMessage());
                    _log.debug(getStringResource("error.inforetrieval"), ex);
                }
            }
        };
        form.add(submitButton);
        Button downloadButton = new Button("downloadXml") {

            private static final long serialVersionUID = 4865409603602146766L;

            @Override
            public void onSubmit() {
                if (_xmlOutput != null) {
                    ResourceStreamRequestHandler target =
                            new ResourceStreamRequestHandler(
                            new StringResourceStream(_xmlOutput, "text/xml"));
                    target.setFileName("info.xml");
                    RequestCycle.get().scheduleRequestHandlerAfterCurrent(target);
                }
            }
        };
        form.add(downloadButton);
        _statepathField = new TextField<>("statepath", new PropertyModel<String>(
                this, "_statepath"));
        _statepathField.add(new DefaultFocusBehaviour());
        form.add(_statepathField);
        MultiLineLabel label = new MultiLineLabel("xmlOutput",
                new PropertyModel<String>(this, "_xmlOutput"));
        form.add(label);
        add(form);
    }

    private InfoService getInfoService() {
        return getWebadminApplication().getInfoService();
    }

    private String getCorrectedStatepath(String statepath) {
        _log.debug("called with statepath: {}", statepath);
        String returnPath;
        if (statepath == null || statepath.equals("")) {
            returnPath = "/";
        } else if (statepath.length() > 1 && statepath.startsWith("/")) {
            returnPath = statepath.replaceFirst("/", "");
        } else {
            returnPath = statepath;
        }
        return returnPath;
    }
}
