package org.dcache.webadmin.model.dataaccess.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.webadmin.model.businessobjects.CellResponse;
import org.dcache.webadmin.model.dataaccess.DomainsDAO;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator;
import org.dcache.webadmin.model.dataaccess.communication.CellMessageGenerator.CellMessageRequest;
import org.dcache.webadmin.model.dataaccess.communication.CommandSender;
import org.dcache.webadmin.model.dataaccess.communication.CommandSenderFactory;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.model.dataaccess.communication.impl.PageInfoCache;
import org.dcache.webadmin.model.dataaccess.communication.impl.StringCommandMessageGenerator;
import org.dcache.webadmin.model.exceptions.DAOException;
import org.dcache.webadmin.model.exceptions.NoSuchContextException;

/**
 *
 * @author jans
 */
public class StandardDomainsDAO implements DomainsDAO {

    private static final String EMPTY_STRING = "";
    private static final String RESPONSE_FAILED = "failed";
    private static final Logger _log = LoggerFactory.getLogger(StandardDomainsDAO.class);
    private PageInfoCache _pageCache;
    private CommandSenderFactory _commandSenderFactory;

    public StandardDomainsDAO(PageInfoCache pageCache,
            CommandSenderFactory commandSenderFactory) {
        _commandSenderFactory = commandSenderFactory;
        _pageCache = pageCache;
    }

    @Override
    public Set<CellStatus> getCellStatuses()
    {
        _log.debug("getCellStatuses called");
        try {
            return (Set<CellStatus>) _pageCache.getCacheContent(ContextPaths.CELLINFO_LIST);
        } catch (NoSuchContextException e) {
            return Collections.emptySet();
        }
    }

    @Override
    public Set<CellResponse> sendCommand(Set<String> destinations, String command)
            throws DAOException {
        try {
            Set<CellResponse> responses = new HashSet<>();
            if (!destinations.isEmpty() && !EMPTY_STRING.equals(command)) {
                StringCommandMessageGenerator messageGenerator =
                        new StringCommandMessageGenerator(destinations, command);
                CommandSender commandSender =
                        _commandSenderFactory.createCommandSender(
                        messageGenerator);
                commandSender.sendAndWait();
                createResponses(responses, messageGenerator);
            }
            return responses;
        } catch (InterruptedException e) {
            throw new DAOException(e);
        }
    }

    private void createResponses(Set<CellResponse> responses,
            CellMessageGenerator<String> messageGenerator) {
        for (CellMessageRequest<String> request : messageGenerator) {
            CellResponse response = new CellResponse();
            response.setCellName(request.getDestination().getCellName());
            if (request.isSuccessful()) {
                response.setResponse(request.getAnswer());
            } else {
                response.setIsFailure(true);
                response.setResponse(RESPONSE_FAILED);
            }
            responses.add(response);
        }
    }

    @Override
    public Map<String, List<String>> getDomainsMap()
    {
        _log.debug("getDomainsMap called");
        try {
            Set<CellStatus> states = (Set<CellStatus>) _pageCache.getCacheContent(
                    ContextPaths.CELLINFO_LIST);
            Map<String, List<String>> domainsMap = Maps.newHashMap();
            for (CellStatus state : states) {
                List<String> cellsOfDomain = domainsMap.get(state.getDomainName());
                if (cellsOfDomain != null) {
                    cellsOfDomain.add(state.getCellName());
                } else {
                    cellsOfDomain = Lists.newArrayList();
                    cellsOfDomain.add(state.getCellName());
                    domainsMap.put(state.getDomainName(), cellsOfDomain);
                }
            }
            return domainsMap;
        } catch (NoSuchContextException e) {
            return Collections.emptyMap();
        }
    }
}
