package com.mindarray.api;

import com.mindarray.Bootstrap;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.mindarray.Constant.*;

public class Metric {

    private static final Logger LOG = LoggerFactory.getLogger(Metric.class.getName());
    private final EventBus eventBus = Bootstrap.vertx.eventBus();

    public void init(Router router) {

        LOG.info("metric init fn() called");

        router.route().method(HttpMethod.PUT).path(METRIC_POINT + "/:id")
                .handler(this::validate).handler(this::update);

        router.route().method(HttpMethod.GET).path(METRIC_POINT).handler(this::get);


        router.route().method(HttpMethod.GET).path(METRIC_POINT + "/:id")
                .handler(this::validate).handler(this::getByID);

        router.route().method(HttpMethod.GET).path(METRIC_POINT + "/limit/:id")
                .handler(this::validate).handler(this::getPollingData);

    }

    private void getByID(RoutingContext context) {

        LOG.info("metric get by id fn() called");
        try {

            String query;
            var type = context.queryParam("type");

            if (!type.isEmpty() && type.get(0).equals("monitor")) {

                query = "select * from metric where monitor_id=" + context.pathParam(ID) + ";";

            } else {

                query = "select * from metric where id=" + context.pathParam(ID) + ";";
            }

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                            .put(METHOD_TYPE, GET_DATA).put(QUERY, query),
                    eventBusHandler -> {

                        try {

                            if (eventBusHandler.succeeded()) {
                                var result = eventBusHandler.result().body().getJsonArray(DATA);

                                if (!result.isEmpty()) {

                                    response(context, 200, new JsonObject()
                                            .put(STATUS, SUCCESS)
                                            .put(RESULT, result));

                                    LOG.info("context ->{}, status ->{}", context.pathParam(ID), "success");

                                } else {

                                    response(context, 200, new JsonObject()
                                            .put(STATUS, FAIL)
                                            .put(ERROR, "id not found in database"));
                                }
                            } else {

                                response(context, 400, new JsonObject().put(ERROR,
                                                eventBusHandler.cause().getMessage())
                                        .put(STATUS, FAIL));

                            }
                        } catch (Exception exception) {
                            LOG.error(EXCEPTION,exception);

                            response(context, 500, new JsonObject().put(STATUS, FAIL)
                                    .put(ERROR, exception.getMessage()));
                        }

                    });
        } catch (Exception exception) {
            LOG.error(EXCEPTION,exception);

            response(context, 500, new JsonObject().put(STATUS, FAIL)
                    .put(ERROR, exception.getMessage()));
        }
    }

    private void getPollingData(RoutingContext context) {

        LOG.info("metric get polling data fn() called");
        Promise<String> promise = Promise.promise();
        Future<String> future = promise.future();

        try {
            int limitVal;
            String query = "select * from polling where monitor_id= idValue and metric_group= \"groupValue\" order by id desc limit limitValue";

            var groups = context.queryParam("group");

            var limit = context.queryParam("limit");

            if (limit.isEmpty() || Integer.parseInt(limit.get(0)) > 100
                    || Integer.parseInt(limit.get(0)) < 0) {

                limitVal = 10;

            } else {

                limitVal = Integer.parseInt(context.queryParam("limit").get(0));

            }

            if (!groups.isEmpty()) {

                query = query.replace("idValue", context.pathParam(ID))
                        .replace("groupValue", groups.get(0))
                        .replace("limitValue", String.valueOf(limitVal)) + ";";

                promise.complete(query);

            } else {
                String finalQuery = query;

                String getMetricGroup = "select metric_group from default_metric where type =(select type from monitor where monitor_id="
                        + context.pathParam(ID) + ");";

                eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                        .put(QUERY, getMetricGroup)
                        .put(METHOD_TYPE, GET_DATA), eventBusHandler -> {

                    try {

                        if (eventBusHandler.succeeded()) {

                            var result = eventBusHandler.result().body().getJsonArray(DATA);

                            if (!result.isEmpty()) {

                                var queryBuilder = new StringBuilder();

                                for (int i = 0; i < result.size(); i++) {

                                    var tempQuery = finalQuery.replace("idValue",
                                                    context.pathParam("id")).replace("groupValue",
                                                    result.getJsonObject(i).getString("metric.group"))
                                            .replace("limitValue", String.valueOf(limitVal));

                                    queryBuilder.append("( ").append(tempQuery).append(") ").append("union");

                                }

                                queryBuilder.setLength(queryBuilder.length() - 5);

                                queryBuilder.append(";");

                                promise.complete(queryBuilder.toString());

                            } else {

                                promise.fail("id not found in database");

                            }

                        } else {

                            promise.fail(eventBusHandler.cause().getMessage());

                        }

                    } catch (Exception exception) {
                        LOG.error(EXCEPTION,exception);

                        promise.fail(exception.getMessage());
                    }
                });
            }


            future.onComplete(futureCompleteHandler -> {
                try {
                    if (futureCompleteHandler.succeeded()) {
                        eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                                .put("query", futureCompleteHandler.result())
                                .put(METHOD_TYPE, GET_DATA), eventBusHandler -> {
                            try {

                                if (eventBusHandler.succeeded()) {

                                    var result = eventBusHandler.result().body()
                                            .getJsonArray(DATA);
                                    if (!result.isEmpty()) {

                                        response(context, 200, new JsonObject()
                                                .put(STATUS, SUCCESS)
                                                .put(RESULT, result));
                                    } else {

                                        response(context, 400, new JsonObject()
                                                .put(STATUS, FAIL)
                                                .put(ERROR, "id not found in database or wrong request"));

                                    }
                                } else {

                                    response(context, 400, new JsonObject().put(STATUS, FAIL)
                                            .put(ERROR, eventBusHandler.cause().getMessage()));

                                }
                            } catch (Exception exception) {
                                LOG.error(EXCEPTION,exception);
                                response(context, 500, new JsonObject()
                                        .put(STATUS, FAIL)
                                        .put(ERROR, exception.getMessage()));

                            }
                        });
                    } else {

                        response(context, 400, new JsonObject().put(STATUS, FAIL)
                                .put(ERROR, futureCompleteHandler.cause().getMessage()));

                    }
                } catch (Exception exception) {
                    LOG.error(EXCEPTION,exception);

                    response(context, 500, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(ERROR, exception.getMessage()));
                }
            });

        } catch (Exception exception) {
            LOG.error(EXCEPTION,exception);
            response(context, 500, new JsonObject().put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));
        }

    }

    private void get(RoutingContext context) {

        LOG.info("metric get all fn() called");

        try {

            var query = "Select * from metric;";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put("query", query)
                    .put(METHOD_TYPE, GET_DATA), eventBusHandler -> {
                try {

                    if (eventBusHandler.succeeded()) {

                        var result = eventBusHandler.result().body().getValue("data");

                        response(context, 200, new JsonObject()
                                .put(STATUS, SUCCESS)
                                .put("result", result));

                    } else {

                        response(context, 400, new JsonObject()
                                .put(STATUS, FAIL)
                                .put(ERROR, eventBusHandler.cause().getMessage()));
                    }
                }catch (Exception exception){

                    LOG.error(EXCEPTION,exception);

                    response(context, 500, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(ERROR, exception.getMessage()));
                }
            });
        } catch (Exception exception) {
            LOG.error(EXCEPTION,exception);

            response(context, 500, new JsonObject().put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }

    }

    private void update(RoutingContext context) {

        LOG.info("metric update fn() called");

        String metricTimeUpdateQuery = "update  metric set time=" + context.getBodyAsJson().getInteger("time")
                + " where id=" + context.getBodyAsJson().getInteger("id") + ";";

        eventBus.request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put("query", metricTimeUpdateQuery)
                .put(METHOD_TYPE, UPDATE_DATA), eventBusHandler -> {

            if (eventBusHandler.succeeded()) {

                response(context, 200, new JsonObject()
                        .put(STATUS, SUCCESS)
                        .put(MESSAGE, "metric time updated successfully"));

            } else {

                response(context, 400, new JsonObject()
                        .put(STATUS, FAIL)
                        .put(ERROR, eventBusHandler.cause().getMessage()));

            }
        });

    }

    private void validate(RoutingContext context) {

        LOG.info("metric validate fn() called");

        try {
            if ((context.request().method() != HttpMethod.DELETE)
                    && (context.request().method() != HttpMethod.GET)) {

                if (context.getBodyAsJson() == null) {

                    response(context, 400, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(MESSAGE, "wrong json format"));

                }

                context.getBodyAsJson().stream().forEach(value -> {

                    if (context.getBodyAsJson().getValue(value.getKey()) instanceof String) {

                        context.getBodyAsJson().put(value.getKey(),
                                context.getBodyAsJson().getString(value.getKey()).trim());

                    }
                });

                context.setBody(context.getBodyAsJson().toBuffer());
            }


            if (Objects.equals(context.request().method().toString(), "PUT")) {

                validateUpdate(context);

            } else if (Objects.equals(context.request().method().toString(), "GET")) {

                if (context.pathParam("id") == null) {

                    response(context, 400, new JsonObject().put(STATUS, FAIL)
                            .put(ERROR, "id is null"));

                    LOG.error("id is null");
                } else {

                    context.next();

                }

            } else {

                response(context, 400, new JsonObject()
                        .put(STATUS, FAIL)
                        .put(ERROR, "wrong request"));
            }

        } catch (Exception exception) {
            LOG.error(EXCEPTION,exception);

            response(context, 500, new JsonObject().put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }

    }

    private void validateUpdate(RoutingContext context) {

        LOG.info("validate update fn() called");

        if (context.pathParam(ID) == null) {
            response(context, 400, new JsonObject()
                    .put(MESSAGE, "id is null")
                    .put(STATUS, FAIL));
        } else {

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                    new JsonObject().put(ID,
                                    Integer.parseInt(context.pathParam(ID)))
                            .put(REQUEST_POINT, METRIC)
                            .put(METHOD_TYPE, CHECK_ID),
                    eventBusHandler -> {
                        if (eventBusHandler.succeeded()
                                && eventBusHandler.result().body() != null) {

                            if (eventBusHandler.result().body().getString(STATUS)
                                    .equals(FAIL)) {

                                response(context, 400, new JsonObject().put(MESSAGE,
                                                eventBusHandler.result().body().getString(ERROR))
                                        .put(STATUS, FAIL));

                            } else {
                                try {
                                    if (!context.getBodyAsJson().containsKey("time")
                                            || !(context.getBodyAsJson().getValue("time") instanceof Integer)
                                            || context.getBodyAsJson().getInteger("time") == null) {
                                        response(context, 400, new JsonObject()
                                                .put(STATUS, FAIL)
                                                .put(ERROR, "time Required (int 10^9 )"));
                                    }
                                    if ((context.getBodyAsJson().getInteger("time")  < 60 )
                                            || (context.getBodyAsJson().getInteger("time")  > 86400 )
                                            || context.getBodyAsJson().getInteger("time") % 10 != 0) {
                                        response(context, 400, new JsonObject()
                                                .put(STATUS, FAIL).put(ERROR
                                                        , "time out of range or multiple of 10 ( range[ 60s,86400s])"));
                                    } else {

                                        context.setBody(context.getBodyAsJson().put("id",
                                                        Integer.parseInt(context.pathParam(ID)))
                                                        .put("time",
                                                                context.getBodyAsJson().getInteger("time")*1000)
                                                .toBuffer());

                                        context.next();
                                    }
                                } catch (Exception exception) {
                                   LOG.error(EXCEPTION,exception);
                                    response(context, 500, new JsonObject()
                                            .put(STATUS, FAIL)
                                            .put(MESSAGE, "wrong json format")
                                            .put(ERROR, exception.getMessage()));
                                }
                            }
                        } else {
                            response(context, 400, new JsonObject()
                                    .put(STATUS, FAIL)
                                    .put(ERROR, eventBusHandler.cause().getMessage()));
                        }
                    });
        }
    }

    private void response(RoutingContext context, int statusCode, JsonObject reply) {

        var httpResponse = context.response();

        httpResponse.setStatusCode(statusCode).putHeader(HEADER_TYPE, HEADER_VALUE)
                .end(reply.encodePrettily());

    }
}
