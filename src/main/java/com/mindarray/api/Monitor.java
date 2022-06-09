package com.mindarray.api;

import com.mindarray.Bootstrap;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.mindarray.Constant.*;

public class Monitor {
    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class.getName());
    private final EventBus eventBus = Bootstrap.vertx.eventBus();

    public void init(Router router) {
        LOG.info("start -> {}", " Monitor Init Called .....");

        router.route().method(HttpMethod.POST).path(MONITOR_POINT +
                PROVISION_POINT).handler(this::validate).handler(this::create);

        router.route().method(HttpMethod.DELETE).path(MONITOR_POINT + "/:id/")
                .handler(this::validate).handler(this::delete);

        router.route().method(HttpMethod.GET).path(MONITOR_POINT).handler(this::get);

        router.route().method(HttpMethod.GET).path(MONITOR_POINT + "/:id/")
                .handler(this::validate).handler(this::getById);

        router.route().method(HttpMethod.PUT).path(MONITOR_POINT + "/:id/")
                .handler(this::validate).handler(this::update);
    }

    private void update(RoutingContext context) {

        String portUpdateQuery = "update  monitor set port=" + context.getBodyAsJson().getInteger(PORT)
                + " where monitor_id=" + context.getBodyAsJson().getInteger(MONITOR_ID) + ";";

        eventBus.request(DATABASE_EVENTBUS_ADDRESS,
                new JsonObject().put(QUERY, portUpdateQuery)
                        .put(METHOD_TYPE, UPDATE_DATA), eventBusHandler -> {

                    try {

                        if (eventBusHandler.succeeded()) {

                            response(context, 200, new JsonObject()
                                    .put(STATUS, SUCCESS)
                                    .put(MESSAGE, "monitor updated successfully"));

                        } else {

                            response(context, 400, new JsonObject()
                                    .put(STATUS, FAIL)
                                    .put(ERROR, eventBusHandler.cause().getMessage()));

                        }

                    } catch (Exception exception) {
                       LOG.error(EXCEPTION,exception);

                        response(context, 500, new JsonObject()
                                .put(STATUS, FAIL)
                                .put(ERROR, exception.getMessage()));
                    }
                });
    }

    private void getById(RoutingContext context) {

        LOG.info("monitor get by id fn() called");
        try {

            var query = "Select * from monitor where monitor_id ="
                    + context.pathParam(ID) + ";";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                            .put(QUERY, query).put(METHOD_TYPE, GET_DATA),
                    eventBusHandler -> {
                        try {
                            if (eventBusHandler.succeeded()) {

                                var result = eventBusHandler.result().body().getValue(DATA);

                                response(context, 200, new JsonObject()
                                        .put(STATUS, SUCCESS)
                                        .put(RESULT, result));

                            } else {

                                response(context, 200, new JsonObject()
                                        .put(STATUS, FAIL)
                                        .put(ERROR, eventBusHandler.cause().getMessage()));
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

            response(context, 400, new JsonObject().put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));
        }
    }

    private void get(RoutingContext context) {
        LOG.info("monitor get all fn() called");

        try {

            var query = "Select * from monitor;";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,

                    new JsonObject().put(QUERY, query)
                            .put(METHOD_TYPE, GET_DATA), eventBusHandler -> {

                        try {

                            if (eventBusHandler.succeeded()) {

                                var result = eventBusHandler.result().body().getValue(DATA);

                                response(context, 200, new JsonObject()
                                        .put(STATUS, SUCCESS)
                                        .put(RESULT, result));

                            } else {

                                response(context, 200,
                                        new JsonObject().put(STATUS, FAIL)
                                                .put(ERROR, eventBusHandler.cause().getMessage()));
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

    private void delete(RoutingContext context) {

        LOG.info("monitor delete fn() called");

        try {
            Promise<JsonObject> promise = Promise.promise();

            Future<JsonObject> future = promise.future();

            String getMetricIDQuery = "select id from metric where monitor_id="
                    + context.pathParam(ID) + ";";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                    .put(QUERY, getMetricIDQuery)
                    .put(METHOD_TYPE, GET_DATA), eventBusHandler -> {

                try {

                    if (eventBusHandler.succeeded()) {

                        promise.complete(eventBusHandler.result().body());

                    } else {

                        promise.fail(eventBusHandler.cause().getMessage());

                    }
                } catch (Exception exception) {
                   LOG.error(EXCEPTION,exception);

                    promise.fail(exception.getMessage());

                }
            });

            future.onComplete(futureCompleteHandler -> {

                if (futureCompleteHandler.succeeded()) {

                    var query = "delete from monitor where monitor_id ="
                            + context.pathParam(ID) + ";";

                    eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                            new JsonObject().put(QUERY, query)
                                    .put(METHOD_TYPE, MONITOR_DELETE)
                                    .put(REQUEST_POINT, MONITOR)
                                    .put(MONITOR_ID, context.pathParam(ID)),
                            eventBusHandler -> {
                                try {

                                    var deleteResult = eventBusHandler.result().body();

                                    if (deleteResult.getString(STATUS).equals(SUCCESS)) {

                                        response(context, 200, new JsonObject()
                                                .put(MESSAGE, "id " + context.pathParam(ID)
                                                        + " deleted")
                                                .put(STATUS, SUCCESS));

                                        eventBus.send(METRIC_SCHEDULER_EVENTBUS_DELETE_ADDRESS
                                                , futureCompleteHandler.result());

                                    } else {

                                        response(context, 400, new JsonObject()
                                                .put(ERROR, eventBusHandler.result().body())
                                                .put(STATUS, FAIL));
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
            });
        } catch (Exception exception) {
           LOG.error(EXCEPTION,exception);

            response(context, 400, new JsonObject().put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }
    }

    private void create(RoutingContext context) {

        LOG.info("monitor create fn() called");

        var monitorData = context.getBodyAsJson();

        String query = "insert into monitor (ip,host,type,port) values("
                + "'" + monitorData.getString(IP) + "','"
                + monitorData.getString(HOST) + "','"
                + monitorData.getString(TYPE) + "',"
                + monitorData.getInteger(PORT) + ");";

        eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put(QUERY, query)
                .put(METHOD_TYPE, CREATE_DATA)
                .put(REQUEST_POINT, MONITOR), eventBusHandler -> {

            try {

                if (eventBusHandler.succeeded()) {

                    var result = eventBusHandler.result().body().getJsonArray(DATA);
                    var id = result.getJsonObject(0).getInteger(ID);

                    response(context, 200, new JsonObject()
                            .put(STATUS, SUCCESS)
                            .put(MESSAGE, "monitor created successfully")
                            .put(ID, id));

                    metricGroupCreate(context.getBodyAsJson().put(MONITOR_ID, id));

                } else {
                    response(context, 400, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(ERROR, eventBusHandler.cause().getMessage()));

                }

            } catch (Exception exception) {
               LOG.error(EXCEPTION,exception);

                response(context, 500, new JsonObject()
                        .put(STATUS, FAIL)
                        .put(ERROR, exception.getMessage()));
            }

        });

    }

    private void metricGroupCreate(JsonObject monitorData) {

        LOG.info("metric group create fn() called");

        Promise<JsonObject> promiseAddMetric = Promise.promise();
        Future<JsonObject> futureAddMetric = promiseAddMetric.future();

        StringBuilder values = new StringBuilder();

        String query = "select metric_group,time,monitor_id from default_metric,monitor where default_metric.type=monitor.type and monitor_id="
                + monitorData.getInteger(MONITOR_ID) + ";";

        eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                        .put(QUERY, query)
                        .put(METHOD_TYPE, GET_DATA),
                eventBusHandler -> {
                    try {

                        if (eventBusHandler.succeeded()) {
                            var resultJsonArrayData = eventBusHandler.result().body()
                                    .getJsonArray(DATA);

                            for (int index = 0; index < resultJsonArrayData.size(); index++) {

                                if (!monitorData.getString(TYPE).equals(NETWORK)) {

                                    resultJsonArrayData.getJsonObject(index).mergeIn(new JsonObject()
                                            .put(CREDENTIAL_PROFILE,
                                                    monitorData.getInteger(CREDENTIAL_ID)));

                                } else {

                                    var getObjects = monitorData.getJsonObject("objects")
                                            .getJsonArray("interfaces");

                                    var upObjects = new JsonArray();

                                    for (int objectIndex = 0; objectIndex <= getObjects.size() - 1; objectIndex++) {

                                        var value = getObjects.getJsonObject(objectIndex);

                                        if (value.getString("interface.operational.status").equals("up")) {

                                            upObjects.add(value);

                                        }
                                    }

                                    resultJsonArrayData.getJsonObject(index).mergeIn(new JsonObject()
                                            .put(CREDENTIAL_PROFILE,
                                                    monitorData.getInteger(CREDENTIAL_ID))
                                            .put("objects", new JsonObject().put("interfaces", upObjects)));
                                }
                            }

                            if (!resultJsonArrayData.isEmpty()) {

                                String addMetricQuery = "";

                                for (int metricDataIndex = 0;
                                     metricDataIndex < resultJsonArrayData.size();
                                     metricDataIndex++) {

                                    var value = resultJsonArrayData.getJsonObject(metricDataIndex);

                                    if (monitorData.getString(TYPE).equals(NETWORK)) {

                                        values.append("(")
                                                .append(value.getInteger(MONITOR_ID))
                                                .append(",").append(monitorData.getInteger(CREDENTIAL_ID))
                                                .append(",'").append(value.getString(METRIC_GROUP))
                                                .append("',").append(value.getInteger("time"))
                                                .append(",'").append(value.getJsonObject("objects"))
                                                .append("'").append("),");

                                    } else {

                                        values.append("(")
                                                .append(value.getInteger(MONITOR_ID))
                                                .append(",").append(monitorData.getInteger(CREDENTIAL_ID))
                                                .append(",'").append(value.getString(METRIC_GROUP))
                                                .append("',").append(value.getInteger("time")).append("),");
                                    }
                                }

                                if (monitorData.getString(TYPE).equals(NETWORK)) {

                                    addMetricQuery = "insert into metric(monitor_id,credential_profile,metric_group,time,objects)values"
                                            + values.substring(0, values.toString().length() - 1) + ";";

                                    promiseAddMetric.complete(new JsonObject()
                                            .put(QUERY, addMetricQuery)
                                            .put(METHOD_TYPE, METRIC_CREATE));
                                } else {

                                    addMetricQuery = "insert into metric(monitor_id,credential_profile,metric_group,time)values"
                                            + values.substring(0, values.toString().length() - 1) + ";";

                                    promiseAddMetric.complete(new JsonObject()
                                            .put(QUERY, addMetricQuery)
                                            .put(METHOD_TYPE, METRIC_CREATE));
                                }

                            } else {

                                promiseAddMetric.fail("result Json array data is empty");

                            }
                        } else {

                            LOG.error(eventBusHandler.cause().getMessage());

                        }
                    } catch (Exception exception) {
                       LOG.error(EXCEPTION,exception);
                    }
                });

        futureAddMetric.onComplete(futureCompleteHandler -> {
            if (futureCompleteHandler.succeeded()) {
                eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                        futureCompleteHandler.result(), eventBusHandler -> {

                            if (eventBusHandler.succeeded()) {
                                LOG.info("metrics added");

                                eventBus.send(METRIC_SCHEDULER_EVENTBUS_ADDRESS,
                                        monitorData.getInteger(CREDENTIAL_ID));

                            } else {

                                LOG.error(eventBusHandler.cause().getMessage());
                            }

                        });
            } else {

                LOG.error(futureCompleteHandler.cause().getMessage());

            }
        });

    }

    private void validate(RoutingContext context) {

        LOG.info(" monitor validate fn() called");

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

            switch (context.request().method().toString()) {
                case "POST" -> validateCreate(context);

                case "PUT" -> validateUpdate(context);

                case "DELETE", "GET" -> {

                    if (context.pathParam(ID) == null) {

                        response(context, 400, new JsonObject().put(MESSAGE, "id is null")
                                .put(STATUS, FAIL));

                    } else {

                        eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                                new JsonObject()
                                        .put(ID, Integer.parseInt(context.pathParam(ID)))
                                        .put(REQUEST_POINT, MONITOR)
                                        .put(METHOD_TYPE, CHECK_ID), eventBusHandler -> {

                                    if (eventBusHandler.succeeded() && eventBusHandler.result().body() != null) {

                                        if (eventBusHandler.result().body().getString(STATUS)
                                                .equals(FAIL)) {

                                            response(context, 400, new JsonObject()
                                                    .put(ERROR,
                                                            eventBusHandler.result().body().getString(ERROR))
                                                    .put(STATUS, FAIL));

                                        } else {

                                            context.next();

                                        }
                                    } else {

                                        response(context, 400, new JsonObject()
                                                .put(STATUS, FAIL)
                                                .put(ERROR, eventBusHandler.cause().getMessage()));

                                    }
                                });
                    }
                }
                default -> response(context, 400, new JsonObject()
                        .put(STATUS, FAIL)
                        .put(MESSAGE, "wrong method"));
            }
        } catch (Exception exception) {
           LOG.error(EXCEPTION,exception);

            response(context, 500, new JsonObject().put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));
        }


    }

    private void validateUpdate(RoutingContext context) {

        if (context.pathParam(ID) == null) {

            response(context, 400, new JsonObject()
                    .put(MESSAGE, "id is null")
                    .put(STATUS, FAIL));

        } else {

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put(ID,
                                    Integer.parseInt(context.pathParam(ID)))
                            .put(REQUEST_POINT, MONITOR)
                            .put(METHOD_TYPE, CHECK_ID),
                    eventBusHandler -> {


                        if (eventBusHandler.succeeded() && eventBusHandler.result().body() != null) {

                            if (eventBusHandler.result().body().getString(STATUS)
                                    .equals(FAIL)) {

                                response(context, 400, new JsonObject().put(MESSAGE,
                                                eventBusHandler.result().body().getString(ERROR))
                                        .put(STATUS, FAIL));

                            } else {
                                try {

                                    if (!context.getBodyAsJson().containsKey(PORT)
                                            || !(context.getBodyAsJson().getValue(PORT) instanceof Integer)
                                            || context.getBodyAsJson().getInteger(PORT) == null) {

                                        response(context, 400, new JsonObject()
                                                .put(STATUS, FAIL)
                                                .put(ERROR, "port is required or wrong type entered "));

                                    }
                                    else if (context.getBodyAsJson().containsKey(PORT)
                                            &&( context.getBodyAsJson().getInteger(PORT) < 0
                                            || context.getBodyAsJson().getInteger(PORT) > 65535)) {
                                        response(context, 400, new JsonObject()
                                                .put(STATUS, FAIL)
                                                .put(ERROR, "wrong port range"));
                                    }
                                    else {

                                        context.setBody(context.getBodyAsJson().put(MONITOR_ID,
                                                        Integer.parseInt(context.pathParam(ID)))
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

    private void validateCreate(RoutingContext context) {

        LOG.info("monitor validate create fn () called");

        var errors = new ArrayList<String>();

        if (!context.getBodyAsJson().containsKey(IP)
                || !(context.getBodyAsJson().getValue(IP) instanceof String)
                || context.getBodyAsJson().getString(IP).isEmpty()) {

            errors.add("ip is required");
            LOG.error("ip is required");

        }

        if (!context.getBodyAsJson().containsKey(TYPE)
                || !(context.getBodyAsJson().getValue(TYPE) instanceof String)
                || context.getBodyAsJson().getString(TYPE).isEmpty()) {

            errors.add("type is required");
            LOG.error("type is required");

        }

        if (!context.getBodyAsJson().containsKey(CREDENTIAL_ID)
                || !(context.getBodyAsJson().getValue(CREDENTIAL_ID) instanceof Integer)
                || context.getBodyAsJson().getInteger(CREDENTIAL_ID) == null) {

            errors.add("credential.id is required (int)");
            LOG.error("credential.id is required");

        }
        if (!context.getBodyAsJson().containsKey(PORT)
                || !(context.getBodyAsJson().getValue(PORT) instanceof Integer)
                || context.getBodyAsJson().getInteger(PORT) == null) {

            errors.add("port is required (int)");
            LOG.error("port is required");

        }
        if (!context.getBodyAsJson().containsKey(HOST)
                || !(context.getBodyAsJson().getValue(HOST) instanceof String)
                || context.getBodyAsJson().getString(HOST).isEmpty()) {

            errors.add("hostname required");
            LOG.error("hostname required");

        }

        if (context.getBodyAsJson().containsKey(TYPE)
                && context.getBodyAsJson().getString(TYPE).equals(NETWORK)
                && (!context.getBodyAsJson().containsKey("objects")
                || context.getBodyAsJson().isEmpty())) {

            errors.add("objects are required for snmp devices");

            LOG.error("objects are required for snmp devices");

        }

        if (errors.isEmpty()) {

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, context.getBodyAsJson()
                    .put(REQUEST_POINT, MONITOR)
                    .put(METHOD_TYPE, MONITOR_CHECK), eventBusHandler -> {

                try {

                    if (eventBusHandler.succeeded()) {

                        var responseData = eventBusHandler.result().body();

                        if (responseData.getString(STATUS).equals(SUCCESS)) {

                            context.next();

                        } else {

                            response(context, 400, new JsonObject()
                                    .put(ERROR, responseData.getString(ERROR))
                                    .put(STATUS, FAIL));
                        }

                    } else {

                        response(context, 400, new JsonObject()
                                .put(ERROR, eventBusHandler.cause().getMessage())
                                .put(STATUS, FAIL));

                    }
                } catch (Exception exception) {
                   LOG.error(EXCEPTION,exception);

                    response(context, 500, new JsonObject()
                            .put(ERROR, exception.getMessage())
                            .put(STATUS, FAIL));
                }
            });
        } else {

            response(context, 400, new JsonObject().put(ERROR, errors)
                    .put(STATUS, FAIL));
        }


    }

    private void response(RoutingContext context, int statusCode, JsonObject reply) {

        var httpResponse = context.response();

        httpResponse.setStatusCode(statusCode).putHeader(HEADER_TYPE, HEADER_VALUE)
                .end(reply.encodePrettily());

    }
}
