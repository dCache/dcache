package org.dcache.webadmin.controller.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.dcache.webadmin.controller.TapeTransfersService;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.dataaccess.DAOFactory;
import org.dcache.webadmin.model.dataaccess.MoverDAO;
import org.dcache.webadmin.view.pages.tapetransferqueue.beans.RestoreBean;

/**
 *
 * @author jans
 */
public class StandardTapeTransfersService implements TapeTransfersService {

    private DAOFactory _daoFactory;

    public StandardTapeTransfersService(DAOFactory DAOFactory) {
        _daoFactory = DAOFactory;
    }

    @Override
    public List<RestoreBean> getRestores()
    {
        List<RestoreBean> beans = new ArrayList<>();
        Set<RestoreInfo> restores = getMoverDAO().getRestores();
        for (RestoreInfo currentRestore : restores) {
            beans.add(createRestoreBean(currentRestore));
        }
        return beans;
    }

    private MoverDAO getMoverDAO() {
        return _daoFactory.getMoverDAO();
    }

    public void setDAOFactory(DAOFactory daoFactory) {
        _daoFactory = daoFactory;
    }

    private RestoreBean createRestoreBean(RestoreInfo info) {
        return BeanDataMapper.restoreInfoModelToView(info);
    }
}
