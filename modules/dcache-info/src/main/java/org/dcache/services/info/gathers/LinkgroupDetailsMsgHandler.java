package org.dcache.services.info.gathers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;

import diskCacheV111.services.space.LinkGroup;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.util.VOInfo;
import diskCacheV111.vehicles.Message;

import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StringStateValue;

public class LinkgroupDetailsMsgHandler implements MessageHandler {

	private static Logger _log = LoggerFactory.getLogger( LinkgroupDetailsMsgHandler.class);
	private static final StatePath LINKGROUPS_PATH = new StatePath("linkgroups");
	private static final StatePath SUMMARY_LINKGROUP_BLANKET_AUTH_ALL = StatePath.parsePath( "summary.linkgroup.blanket-auth.all");
	private static final StatePath SUMMARY_LINKGROUP_BLANKET_AUTH_BY_VO = StatePath.parsePath( "summary.linkgroup.blanket-auth.by-VO");
	private static final String WILDCARD_ROLE = "*";
	private static final String WILDCARD_VO = "*";

	final private StateUpdateManager _sum;

	public LinkgroupDetailsMsgHandler( StateUpdateManager sum) {
		_sum = sum;
	}

	@Override
        public boolean handleMessage(Message messagePayload, long metricLifetime) {

		if( !(messagePayload instanceof GetLinkGroupsMessage)) {
                    return false;
                }

		if( _log.isInfoEnabled()) {
                    _log.info("received linkgroup details msg.");
                }

		GetLinkGroupsMessage msg = (GetLinkGroupsMessage) messagePayload;

		Collection<LinkGroup> linkGroups = msg.getLinkGroups();

		if( linkGroups.isEmpty()) {
			_log.info( "received GetLinkGroupNamesMessage with no linkgroups listed");
			return true;
		}

		StateUpdate update = new StateUpdate();

		for( LinkGroup linkGroup : linkGroups) {

			String lgid = String.valueOf( linkGroup.getId());

			StatePath thisLinkGroupPath = LINKGROUPS_PATH.newChild( lgid);

			update.appendUpdate( thisLinkGroupPath.newChild("name"), new StringStateValue( linkGroup.getName(), metricLifetime));
			update.appendUpdate( thisLinkGroupPath.newChild("id"), new StringStateValue( lgid, metricLifetime));

			StatePath spacePath = thisLinkGroupPath.newChild( "space");
			update.appendUpdate( spacePath.newChild("free"), new IntegerStateValue( linkGroup.getFreeSpace(), metricLifetime));
			update.appendUpdate( spacePath.newChild("reserved"), new IntegerStateValue( linkGroup.getReservedSpace(), metricLifetime));
			update.appendUpdate( spacePath.newChild("available"), new IntegerStateValue( linkGroup.getAvailableSpace(), metricLifetime));

			StatePath accLatPath = thisLinkGroupPath.newChild("access-latency");
			update.appendUpdate( accLatPath.newChild("onlineAllowed"), new BooleanStateValue( linkGroup.isOnlineAllowed(), metricLifetime));
			update.appendUpdate( accLatPath.newChild("nearlineAllowed"), new BooleanStateValue( linkGroup.isNearlineAllowed(), metricLifetime));

			StatePath retPolPath = thisLinkGroupPath.newChild("retention-policy");
			update.appendUpdate( retPolPath.newChild("replicaAllowed"), new BooleanStateValue( linkGroup.isReplicaAllowed(), metricLifetime));
			update.appendUpdate( retPolPath.newChild("outputAllowed"), new BooleanStateValue( linkGroup.isOutputAllowed(), metricLifetime));
			update.appendUpdate( retPolPath.newChild("custodialAllowed"), new BooleanStateValue( linkGroup.isCustodialAllowed(), metricLifetime));

			Date lastUpdate = new Date( linkGroup.getUpdateTime());
			CellMessageHandlerSkel.addTimeMetrics( update, thisLinkGroupPath.newChild("lastUpdated"), lastUpdate, metricLifetime);

			StatePath vosPath = thisLinkGroupPath.newChild( "authorisation");
			VOInfo voInfo[] = linkGroup.getVOs();

			if( voInfo.length > 0) {
                            for (VOInfo thisVO : voInfo) {
                                addVoInfo(update, vosPath.newChild(thisVO
                                        .toString()), thisVO, metricLifetime, lgid);
                            }
			} else {
				// Ensure the VOs branch exists.
				update.appendUpdate( vosPath, new StateComposite(metricLifetime));
			}
		}

		_sum.enqueueUpdate( update);

		return true;
	}

	/**
	 * Add VO-specific information about a linkgroup underneath the correct branch.
	 * @param update the StateUpdate to append new metrics underneath
	 * @param vosPath the branch that metrics will be added underneath.
	 * @param vo the VOInfo of this VO.
	 */
	private void addVoInfo( StateUpdate update, StatePath voPath, VOInfo vo, long metricLifetime, String lgid) {
		String group = vo.getVoGroup();
		String role = vo.getVoRole();

		if( group != null) {
                    update.appendUpdate(voPath
                            .newChild("group"), new StringStateValue(group, metricLifetime));
                }

		if( role != null) {
                    update.appendUpdate(voPath
                            .newChild("role"), new StringStateValue(role, metricLifetime));
                }

		if( group != null) {
			StringBuilder fqan = new StringBuilder();

			fqan.append( vo.getVoGroup());

			if( role != null && !role.equals( WILDCARD_ROLE)) {
				fqan.append( "/Role=");
				fqan.append( role);
			}

			update.appendUpdate( voPath.newChild( "FQAN"), new StringStateValue( fqan.toString(), metricLifetime));

			// If this entry authorises a whole VO (or all VOs), make a special note of this.
			if( (role == null || role.equals( WILDCARD_ROLE)) && group.indexOf('/', 1) == -1) {
				String voName = group.startsWith("/") ? group.substring(1) : group;

				StatePath rootPath = voName.equals( WILDCARD_VO) ? SUMMARY_LINKGROUP_BLANKET_AUTH_ALL :  SUMMARY_LINKGROUP_BLANKET_AUTH_BY_VO.newChild( voName);
				update.appendUpdate( rootPath.newChild("linkgroups").newChild(lgid), new StateComposite( metricLifetime));
			}
		}
	}

}
