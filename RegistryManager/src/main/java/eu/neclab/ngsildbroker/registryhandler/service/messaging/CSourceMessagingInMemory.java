package eu.neclab.ngsildbroker.registryhandler.service.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile("in-memory")
public class CSourceMessagingInMemory extends CSourceMessagingBase {
	@Incoming(AppConstants.ENTITY_CHANNEL)
	public Uni<Void> handleEntity(Message<BaseRequest> mutinyMessage) {
		return baseHandleEntity(mutinyMessage);

	}
}
