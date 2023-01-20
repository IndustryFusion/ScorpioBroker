package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionSyncManager;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;

@Singleton
@UnlessBuildProfile("in-memory")
public class RegistrySubscriptionSyncService extends BaseSubscriptionSyncManager {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	@ConfigProperty(name = "scorpio.messaging.duplicate", defaultValue = "false")
	boolean duplicate;

	@Inject
	@Channel(AppConstants.REG_SUB_ALIVE_CHANNEL)
	MutinyEmitter<AliveAnnouncement> aliveEmitter;

	@Inject
	RegistrySubscriptionService subService;

	@Incoming(AppConstants.REG_SUB_SYNC_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	Uni<Void> listenForSubs(SyncMessage message) {
//		Message<SubscriptionRequest> tmp;
//		if (duplicate) {
//			tmp = MicroServiceUtils.deepCopySubscriptionMessage(message);
//		} else {
//			tmp = message;
//		}
		// listenForSubscriptionUpdates(tmp.getPayload(), tmp.getPayload().getId());
		listenForSubscriptionUpdates(message.getRequest(), message.getSyncId());
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.REG_SUB_ALIVE_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	Uni<Void> listenForAlive(AliveAnnouncement message) {
		listenForAnnouncements(message, message.getId());
		return Uni.createFrom().voidItem();
	}

	@Override
	protected void setSyncId() {
		this.syncId = RegistrySubscriptionSyncService.SYNC_ID;
	}

	@Override
	protected MutinyEmitter<AliveAnnouncement> getAliveEmitter() {
		return aliveEmitter;

	}

	@Override
	protected BaseSubscriptionService getSubscriptionService() {
		return subService;
	}

}
