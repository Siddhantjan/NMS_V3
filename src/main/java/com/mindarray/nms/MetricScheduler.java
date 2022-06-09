package com.mindarray.nms;

import com.mindarray.Bootstrap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static com.mindarray.Constant.*;

public class MetricScheduler extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(MetricScheduler.class.getName());
    private final HashMap<Integer, Integer> originalMetricTime = new HashMap<>();
    private final HashMap<Integer, Integer> iteratorMetricTime = new HashMap<>();

    @Override
    public void start(Promise<Void> startPromise) {

        var eventBus = vertx.eventBus();

        initialPolling();

        eventBus.<JsonObject>localConsumer(METRIC_SCHEDULER_EVENTBUS_DELETE_ADDRESS, eventBusHandler -> {
            try {

                var metricIdArray = eventBusHandler.body().getJsonArray(DATA);

                for (int metricIdIndex = 0; metricIdIndex < metricIdArray.size(); metricIdIndex++) {

                    var value = metricIdArray.getJsonObject(metricIdIndex);

                    originalMetricTime.remove(value.getInteger(ID));

                    iteratorMetricTime.remove(value.getInteger(ID));

                }
            } catch (Exception exception) {

                LOG.error(EXCEPTION, exception);


            }
        });

        eventBus.<Integer>localConsumer(METRIC_SCHEDULER_EVENTBUS_ADDRESS, eventBusHandler -> {

            try {

                var id = eventBusHandler.body();

                if (id != null) {

                    String getQuery = "select id,monitor_id,credential_profile,time from metric where credential_profile="
                            + id + ";";

                    eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                                    .put(QUERY, getQuery).put(METHOD_TYPE, GET_DATA),
                            eventBusGetDataHandler -> {
                                try {

                                    if (eventBusGetDataHandler.succeeded()
                                            && !eventBusGetDataHandler.result().body().isEmpty()) {

                                        var result = eventBusGetDataHandler.result().body()
                                                .getJsonArray(DATA);

                                        if (!result.isEmpty()) {

                                            for (int index = 0; index < result.size(); index++) {

                                                var value = result.getJsonObject(index);

                                                originalMetricTime.put(value.getInteger(ID),
                                                        value.getInteger("time"));

                                                iteratorMetricTime.put(value.getInteger(ID),
                                                        value.getInteger("time"));

                                            }

                                        } else {

                                            LOG.error("data is empty in metric");

                                        }

                                    } else {

                                        LOG.error(eventBusGetDataHandler.cause().getMessage());

                                    }

                                } catch (Exception exception) {
                                    LOG.error(EXCEPTION, exception);
                                }
                            });
                } else {
                    LOG.error("Data mismatched");
                }
            } catch (Exception exception) {
                LOG.error(EXCEPTION, exception);
            }
        });


        vertx.setPeriodic(10000, periodicHandler -> {
            if (iteratorMetricTime.size() > 0) {
                var timestamp = System.currentTimeMillis();

                iteratorMetricTime.forEach((key, value) -> {
                    var remainingTime = value - 10000;

                    if (remainingTime <= 0) {

                        var originalTime = originalMetricTime.get(key);
                        iteratorMetricTime.put(key, originalTime);
                        String getQuery = "select id,metric.monitor_id,ip,type,username,password,community,version,metric_group,time,port from metric,credential,monitor where metric.credential_profile=credential_id and metric.monitor_id=monitor.monitor_id and id=" + key + ";";

                        eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                                        .put(QUERY, getQuery)
                                        .put(METHOD_TYPE, GET_DATA)
                                , eventBusHandler -> {
                                    try {
                                        if (eventBusHandler.succeeded() && eventBusHandler.result().body() != null) {
                                            eventBus.send(POLLING_EVENTBUS_ADDRESS,
                                                    eventBusHandler.result().body().getJsonArray(DATA)
                                                            .getJsonObject(0).put("category", "polling")
                                                            .put("timestamp", timestamp));
                                        } else {
                                            LOG.error(eventBusHandler.cause().getMessage());
                                        }
                                    } catch (Exception exception) {
                                        LOG.error(EXCEPTION, exception);
                                    }
                                });

                    } else {

                        iteratorMetricTime.put(key, remainingTime);

                    }
                });
            }
        });

        startPromise.complete();

    }

    private void initialPolling() {

        LOG.info("initial polling fn() called");

        String getQuery = "Select id,time from metric;";

        Bootstrap.vertx.eventBus().<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                new JsonObject().put(QUERY, getQuery)
                        .put(METHOD_TYPE, GET_DATA), eventBusHandler -> {

                    try {

                        if (eventBusHandler.succeeded() && !eventBusHandler.result().body().isEmpty()) {

                            var result = eventBusHandler.result().body().getJsonArray(DATA);

                            if (!result.isEmpty()) {

                                for (int index = 0; index < result.size(); index++) {

                                    var value = result.getJsonObject(index);

                                    originalMetricTime.put(value.getInteger(ID), value.getInteger("time"));

                                    iteratorMetricTime.put(value.getInteger(ID), value.getInteger("time"));

                                }

                            } else {

                                LOG.debug("data is empty in metric");

                            }
                        } else {

                            LOG.error(eventBusHandler.cause().getMessage());

                        }
                    } catch (Exception exception) {
                        LOG.error(EXCEPTION, exception);
                    }
                });
    }
}
