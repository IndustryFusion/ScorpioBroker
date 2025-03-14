package eu.neclab.ngsildbroker.commons.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;



public class NotificationBuffer {
    private final Map<Long, List<Map<String, Object>>> timestamp2Entities;
    private final SubscriptionRequest subscription;
    private long lastUpdateTime;
    private static final long TIME_WINDOW = 100; // 100ms window

    public NotificationBuffer(SubscriptionRequest subscription) {
        this.subscription = subscription;
        this.timestamp2Entities = new HashMap<>();
        this.lastUpdateTime = System.currentTimeMillis();
    }

    public void addEntity(Map<String, Object> entity) {
        long modifiedAt = extractModifiedAtMillis(entity);
        // Round down to nearest window
        long windowKey = (modifiedAt / TIME_WINDOW) * TIME_WINDOW;
        timestamp2Entities.computeIfAbsent(windowKey, k -> new ArrayList<>()).add(entity);
        this.lastUpdateTime = System.currentTimeMillis();
    }

    @SuppressWarnings("unchecked")
    private long extractModifiedAtMillis(Map<String, Object> entity) {
        List<Map<String, Object>> modifiedAt = (List<Map<String, Object>>) 
            entity.get("https://uri.etsi.org/ngsi-ld/modifiedAt");
        if (modifiedAt != null && !modifiedAt.isEmpty()) {
            String timestamp = (String) modifiedAt.get(0).get("@value");
            return SerializationTools.date2Long(timestamp);
        }
        return System.currentTimeMillis();
    }

    public Map<Long, List<Map<String, Object>>> getTimestamp2Entities() {
        return timestamp2Entities;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public SubscriptionRequest getSubscription() {
        return subscription;
    }
}
