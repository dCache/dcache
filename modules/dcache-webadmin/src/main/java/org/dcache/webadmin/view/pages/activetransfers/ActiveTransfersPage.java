package org.dcache.webadmin.view.pages.activetransfers;

import com.google.common.base.Strings;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import org.apache.wicket.authroles.authorization.strategies.role.Roles;
import org.apache.wicket.authroles.authorization.strategies.role.metadata.MetaDataRoleAuthorizationStrategy;
import org.apache.wicket.extensions.markup.html.repeater.data.table.DataTable;
import org.apache.wicket.extensions.markup.html.repeater.data.table.DefaultDataTable;
import org.apache.wicket.extensions.markup.html.repeater.data.table.IColumn;
import org.apache.wicket.extensions.markup.html.repeater.data.table.PropertyColumn;
import org.apache.wicket.extensions.markup.html.repeater.util.SortParam;
import org.apache.wicket.extensions.markup.html.repeater.util.SortableDataProvider;
import org.apache.wicket.extensions.model.AbstractCheckBoxModel;
import org.apache.wicket.markup.html.form.Button;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.markup.html.form.TextField;
import org.apache.wicket.markup.html.panel.FeedbackPanel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.PropertyModel;
import org.apache.wicket.model.ResourceModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.webadmin.controller.ActiveTransfersService;
import org.dcache.webadmin.controller.exceptions.ActiveTransfersServiceException;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;
import org.dcache.webadmin.view.pages.basepage.BasePage;
import org.dcache.webadmin.view.util.CheckBoxColumn;
import org.dcache.webadmin.view.util.Role;

import static java.util.stream.Collectors.toList;

@SuppressWarnings("serial")
public class ActiveTransfersPage extends BasePage
{
    private static final Logger _log = LoggerFactory.getLogger(ActiveTransfersPage.class);

    private final Set<ActiveTransfersBean.Key> selected = new HashSet<>();
    private final boolean isAdmin;
    private String filter;

    public ActiveTransfersPage()
    {
        isAdmin = getWebadminSession().hasAnyRole(new Roles(Role.ADMIN));

        add(new FeedbackPanel("feedback"));

        Form<?> filterForm = new Form<Void>("filterForm");
        filterForm.add(new TextField<>("filter", new PropertyModel<String>(this, "filter")));
        filterForm.add(new Button("clear") {
            @Override
            public void onSubmit()
            {
                filter = null;
            }
        });
        filterForm.setDefaultButton(new Button("update"));
        add(filterForm);

        List<IColumn<ActiveTransfersBean, ColumnComparator>> columns = new ArrayList<>();
        if (isAdmin) {
            columns.add(new SelectColumn());
        }
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("door"), ColumnComparator.DOOR, "cellName"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("domain"), ColumnComparator.DOMAIN, "cellDomainName"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("id"), ColumnComparator.ID, "serialId"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("protocol"), ColumnComparator.PROTOCOL, "protocol"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("owner"), ColumnComparator.OWNER, "owner"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("process"), ColumnComparator.PROCESS, "process"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("pnfsid"), ColumnComparator.PNFSID, "pnfsId"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("pool"), ColumnComparator.POOL, "pool"));
        if (isAdmin) {
            columns.add(
                    new PropertyColumn<ActiveTransfersBean, ColumnComparator>(
                            new ResourceModel("host"), ColumnComparator.HOST, "replyHost"));
        }
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("status"), ColumnComparator.STATUS, "status"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("time"), ColumnComparator.SINCE, "waitingSinceTime"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("state"), ColumnComparator.STATE, "state"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("transferred"), ColumnComparator.TRANSFERRED, "transferred"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("throughput"), ColumnComparator.RATE, "transferRate"));
        columns.add(new PropertyColumn<ActiveTransfersBean, ColumnComparator>(new ResourceModel("mover"), ColumnComparator.JOB, "jobId"));

        TransfersProvider provider = new TransfersProvider();
        DataTable<ActiveTransfersBean, ColumnComparator> transfers = new DefaultDataTable<>("transfers", columns, provider, 1000);

        Button submit = new Button("submit");
        MetaDataRoleAuthorizationStrategy.authorize(submit, RENDER, Role.ADMIN);

        Form<?> form = new Form<Void>("transfersForm") {
            @Override
            protected void onSubmit()
            {
                try {
                    getActiveTransfersService().kill(selected);
                    selected.clear();
                } catch (ActiveTransfersServiceException e) {
                    _log.info("Failed to kill some movers: {}",
                              e.getMessage());
                    error(getStringResource("error.notAllMoversKilled"));
                }
            }
        };
        form.add(new TextField<>("filter", new PropertyModel<String>(this, "filter")));
        form.add(transfers);
        form.add(submit);
        add(form);

        addAutoRefreshToForm(form, 30, TimeUnit.SECONDS);
    }

    private ActiveTransfersService getActiveTransfersService()
    {
        return getWebadminApplication().getActiveTransfersService();
    }

    private class TransfersProvider extends SortableDataProvider<ActiveTransfersBean, ColumnComparator>
    {
        private static final long serialVersionUID = -8155360113018832985L;

        private List<ActiveTransfersBean> transfers;

        private List<ActiveTransfersBean> getTransfers()
        {
            if (transfers == null) {
                transfers = new ArrayList<>(getActiveTransfersService().getTransfers());
            }
            if (!Strings.isNullOrEmpty(filter)) {
                String s = filter.toLowerCase();
                return transfers.stream()
                        .filter(transfer -> transfer.getCellName().toLowerCase().contains(s) ||
                                            transfer.getCellDomainName().toLowerCase().contains(s) ||
                                            transfer.getOwner().toLowerCase().contains(s) ||
                                            transfer.getPnfsId().toLowerCase().contains(s) ||
                                            transfer.getPool().toLowerCase().contains(s) ||
                                            transfer.getProcess().toLowerCase().contains(s) ||
                                            transfer.getProtocolFamily().toLowerCase().contains(s) ||
                                            isAdmin && transfer.getReplyHost().toLowerCase().contains(s) ||
                                            transfer.getState().toLowerCase().contains(s) ||
                                            transfer.getStatus().toLowerCase().contains(s))
                        .collect(toList());
            }
            return transfers;
        }

        @Override
        public void detach()
        {
            transfers = null;
        }

        @Override
        public Iterator<? extends ActiveTransfersBean> iterator(long first, long count)
        {
            List<ActiveTransfersBean> data = getTransfers();
            SortParam<ColumnComparator> sort = getSort();
            if (sort != null) {
                Comparator<ActiveTransfersBean> order = sort.getProperty();
                if (order != null) {
                    if (!sort.isAscending()) {
                        order = Ordering.from(order).reverse();
                    }
                    Collections.sort(data, order);
                }
            }
            return data.subList((int) first, (int) Math.min(first + count, data.size())).iterator();
        }

        @Override
        public long size()
        {
            return getTransfers().size();
        }

        @Override
        public IModel<ActiveTransfersBean> model(ActiveTransfersBean object)
        {
            return Model.of(object);
        }

    }

    private static enum ColumnComparator implements Comparator<ActiveTransfersBean>
    {
        DOOR {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getCellName().compareTo(o2.getCellName());
            }
        },
        DOMAIN {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getCellDomainName().compareTo(o2.getCellDomainName());
            }
        },
        ID {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return Longs.compare(o1.getSerialId(), o2.getSerialId());
            }
        },
        PROTOCOL {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getProtocolFamily().compareTo(o2.getProtocolFamily());
            }
        },
        OWNER {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getOwner().compareTo(o2.getOwner());
            }
        },
        PROCESS {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getProcess().compareTo(o2.getProcess());
            }
        },
        PNFSID {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getPnfsId().compareTo(o2.getPnfsId());
            }
        },
        POOL {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getPool().compareTo(o2.getPool());
            }
        },
        HOST {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getReplyHost().compareTo(o2.getReplyHost());
            }
        },
        STATUS {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getStatus().compareTo(o2.getStatus());
            }
        },
        SINCE {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return Longs.compare(o1.getWaitingSince(), o2.getWaitingSince());
            }
        },
        STATE {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return o1.getState().compareTo(o2.getState());
            }
        },
        TRANSFERRED {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return Longs.compare(o1.getTransferred(), o2.getTransferred());
            }
        },
        RATE {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return Longs.compare(o1.getTransferRate(), o2.getTransferRate());
            }
        },
        JOB {
            @Override
            public int compare(ActiveTransfersBean o1, ActiveTransfersBean o2)
            {
                return Longs.compare(o1.getJobId(), o2.getJobId());
            }
        }
    }

    private class SelectColumn extends CheckBoxColumn<ActiveTransfersBean, ColumnComparator>
    {
        public SelectColumn()
        {
            super(Model.of(""));
        }

        @Override
        protected IModel<Boolean> newCheckBoxModel(
                final IModel<ActiveTransfersBean> rowModel)
        {
            return new AbstractCheckBoxModel()
            {
                @Override
                public boolean isSelected()
                {
                    return selected.contains(rowModel.getObject().getKey());
                }

                @Override
                public void unselect()
                {
                    selected.remove(rowModel.getObject().getKey());
                }

                @Override
                public void select()
                {
                    selected.add(rowModel.getObject().getKey());
                }

                @Override
                public void detach()
                {
                    rowModel.detach();
                }
            };
        }
    }
}
