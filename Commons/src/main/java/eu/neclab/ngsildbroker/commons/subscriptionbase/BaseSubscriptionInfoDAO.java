package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

public abstract class BaseSubscriptionInfoDAO extends StorageDAO implements SubscriptionInfoDAOInterface {
	private final static Logger logger = LoggerFactory.getLogger(BaseSubscriptionInfoDAO.class);

	private String dbname = getDBName();

	public Uni<Table<String, String, Set<String>>> getIds2Type() {
		String sql = getSQLForTypes();
		List<Uni<Tuple2<RowSet<Row>, String>>> unis = Lists.newArrayList();
		for (Entry<String, Uni<PgPool>> entry : clientManager.getAllClients().entrySet()) {
			Uni<PgPool> clientUni = entry.getValue();
			unis.add(clientUni.onItem().transformToUni(client -> client.preparedQuery(sql).execute().onItem()
					.transform(t -> Tuple2.of(t, entry.getKey()))));
		}
		return Uni.combine().all().unis(unis).combinedWith(t -> {
			Table<String, String, Set<String>> result = HashBasedTable.create();
			for (Object obj : t) {
				Tuple2<RowSet<Row>, String> tuple = (Tuple2<RowSet<Row>, String>) obj;
				RowSet<Row> rowSet = tuple.getItem1();
				String tenant = tuple.getItem2();
				rowSet.forEach(row -> {
					addToResult(result, tenant, row.getString("id"), row.getString("type"));
				});
			}
			return result;
		});
	}

	protected abstract String getDBName();

	protected abstract String getSQLForTypes();

	private void addToResult(Table<String, String, Set<String>> result, String key, String id, String type) {
		Set<String> value = result.get(key, id);

		if (value == null) {
			value = new HashSet<String>();
			result.put(key, id, value);
		}
		value.add(type);
	}

	public Uni<List<String>> getStoredSubscriptions() {
		List<Uni<RowSet<Row>>> unis = Lists.newArrayList();
		for (Entry<String, Uni<PgPool>> entry : clientManager.getAllClients().entrySet()) {
			Uni<PgPool> clientUni = entry.getValue();
			unis.add(clientUni.onItem().transformToUni(
					client -> client.preparedQuery("SELECT subscription_request FROM " + dbname).execute()));
		}
		return Uni.combine().all().unis(unis).combinedWith(t -> {
			List<String> result = Lists.newArrayList();
			for (Object obj : t) {
				RowSet<Row> rowSet = (RowSet<Row>) obj;
				rowSet.forEach(row -> {
					result.add(row.getString(0));
				});
			}
			return result;
		});
	}

	@Override
	public Uni<List<String>> getEntriesFromSub(SubscriptionRequest subscriptionRequest) {
		String tenant = subscriptionRequest.getTenant();
		Subscription subscription = subscriptionRequest.getSubscription();
		List<QueryParams> qps = ParamsResolver.getQueryParamsFromSubscription(subscription);
		return query(qps, tenant);
	}

	private Uni<List<String>> query(List<QueryParams> qps, String tenant) {
		List<Uni<QueryResult>> unis = Lists.newArrayList();
		for (QueryParams qp : qps) {
			qp.setTenant(tenant);
			Uni<QueryResult> qr = query(qp);
			unis.add(qr);
		}
		return Uni.combine().all().unis(unis).combinedWith(t -> {
			List<String> result = Lists.newArrayList();
			for (Object entry : t) {
				QueryResult qr = (QueryResult) entry;
				List<String> resultString = qr.getActualDataString();
				if (resultString != null) {
					result.addAll(resultString);
				}
			}
			return result;
		});
	}

	@Override
	public Uni<Void> storeSubscription(SubscriptionRequest sub) {
		String tenant = sub.getTenant();
		return clientManager.getClient(tenant, false).onItem()
				.transformToUni(client -> client.preparedQuery("INSERT INTO " + dbname
						+ " (subscription_id, subscription_request) VALUES (?, ?) ON CONFLICT(subscription_id) DO UPDATE SET subscription_request = EXCLUDED.subscription_request")
						.execute(Tuple.of(sub.getId(), DataSerializer.toJson(sub))).onItem().ignore()
						.andContinueWithNull());
	}

	@Override
	public Uni<Void> deleteSubscription(SubscriptionRequest sub) {
		String tenant = sub.getTenant();

		return clientManager.getClient(tenant, false).onItem()
				.transformToUni(client -> client.preparedQuery("DELETE FROM " + dbname + " WHERE subscription_id=?")
						.execute(Tuple.of(sub.getId())).onItem().ignore().andContinueWithNull());
	}
}
