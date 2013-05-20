package org.dcache.webadmin.view.pages.info;

import org.apache.wicket.request.cycle.RequestCycle;
import org.apache.wicket.request.handler.resource.ResourceStreamRequestHandler;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.util.resource.StringResourceStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.webadmin.controller.InfoService;
import org.dcache.webadmin.controller.exceptions.InfoServiceException;
import org.dcache.webadmin.view.pages.basepage.BasePage;

/**
 * A simple page that enables the download of the xml delivered by the
 * Info-Service by command-line (no need to be authenticated!). This works
 * e.g. by  wget hostname:port/webadmin/info?statepath=pools/poolname
 * the page is mounted as "info" in the ApplicationObject for this purpose
 * @author jans
 */
public class Info extends BasePage {

    private static final Logger _log = LoggerFactory.getLogger(Info.class);
    private static final long serialVersionUID = -2156502430133092912L;
    private String _statepath;

    public Info(PageParameters parameters) {
        try {
            setCorrectedStatepath(parameters.get("statepath").toString());
            _log.debug("calls Info with: {}", _statepath);
            CharSequence export = getInfoService().getXmlForStatepath(_statepath);
//            to get the legacy behaviour the filename is not set - this
//            way the firefox browser opens this directly with no download popup
            ResourceStreamRequestHandler target = new ResourceStreamRequestHandler(
                    new StringResourceStream(export, "text/xml"));
            RequestCycle.get().scheduleRequestHandlerAfterCurrent(target);
        } catch (InfoServiceException ex) {
            _log.error("Info-Service-Exception: {}", ex.getMessage());
        }
    }

    private void setCorrectedStatepath(String statepath) {
        _log.debug("called with statepath: {}", statepath);
        if (statepath == null || statepath.equals("")) {
            _statepath = "/";
        } else if (statepath.length() > 1 && statepath.startsWith("/")) {
            _statepath = statepath.replaceFirst("/", "");
        } else {
            _statepath = statepath;
        }
    }

    private InfoService getInfoService() {
        return getWebadminApplication().getInfoService();
    }
}
