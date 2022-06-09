package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Utils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.mindarray.Constant.*;

public class Discovery {
    private static final Logger LOG = LoggerFactory.getLogger(Discovery.class.getName());

    private final EventBus eventBus = Bootstrap.vertx.eventBus();

    public void init(Router router) {
        LOG.info("Discovery Init fn () Called .....");

        router.route().method(HttpMethod.POST).path(DISCOVERY_POINT)
                .handler(this::validate).handler(this::create);

        router.route().method(HttpMethod.PUT).path(DISCOVERY_POINT + "/:id/")
                .handler(this::inputValidate).handler(this::validate).handler(this::update);

        router.route().method(HttpMethod.DELETE).path(DISCOVERY_POINT + "/:id/")
                .handler(this::validate).handler(this::delete);

        router.route().method(HttpMethod.GET).path(DISCOVERY_POINT).handler(this::get);

        router.route().method(HttpMethod.GET).path(DISCOVERY_POINT + "/:id/")
                .handler(this::validate).handler(this::getById);

        router.route().method(HttpMethod.POST).path(DISCOVERY_POINT + "/run" + "/:id/")
                .handler(this::run);
    }

    private void inputValidate(RoutingContext context) {

        LOG.info("discovery Input validate fn() called");


        try {

            if (context.pathParam(ID) == null) {

                response(context, 400, new JsonObject()
                        .put(MESSAGE, "id is null")
                        .put(STATUS, FAIL));

            }
            else {
                var query = "select type from discovery where discovery_id=" + Integer.parseInt(context.pathParam(ID));
                eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put(QUERY, query)
                                .put(METHOD_TYPE, GET_DATA),
                        eventBusHandler -> {
                            if (eventBusHandler.succeeded()) {
                                var data = eventBusHandler.result().body()
                                        .getJsonArray(DATA).getJsonObject(0);
                                if (!data.isEmpty()) {
                                        try {
                                            var contextData = context.getBodyAsJson();
                                            var validateInputArray = Utils.inputValidation();
                                            if (contextData.containsKey(TYPE)) {
                                                contextData.remove(TYPE);
                                            }
                                            var type = data.getString(TYPE);
                                            var keys = contextData.fieldNames();
                                            keys.removeIf(key -> !validateInputArray.get(type).contains(key));
                                            context.setBody(contextData.put(DISCOVERY_ID,
                                                            Integer.parseInt(context.pathParam(ID)))
                                                    .toBuffer());

                                            context.next();

                                        } catch (Exception exception) {
                                            LOG.error(EXCEPTION, exception);
                                            response(context, 500, new JsonObject()
                                                    .put(STATUS, FAIL)
                                                    .put(MESSAGE, "wrong json format")
                                                    .put(ERROR, exception.getMessage()));
                                        }
                                } else {
                                    response(context, 400, new JsonObject().put(MESSAGE,
                                                    "discovery.id not found in database")
                                            .put(STATUS, FAIL));
                                }
                            } else {

                                response(context, 400, new JsonObject()
                                        .put(STATUS, FAIL)
                                        .put(ERROR, eventBusHandler.cause().getMessage()));
                            }

                        });
            }


        } catch (Exception exception) {
            LOG.error(EXCEPTION, exception);

            response(context, 400, new JsonObject()
                    .put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }
    }

    private void run(RoutingContext context) {

        Promise<JsonObject> promise = Promise.promise();
        Promise<JsonObject> promiseDiscovery = Promise.promise();
        Future<JsonObject> future = promise.future();
        Future<JsonObject> futureDiscovery = promiseDiscovery.future();

        if (context.pathParam(ID) == null) {

            response(context, 400, new JsonObject()
                    .put(MESSAGE, "id is null")
                    .put(STATUS, FAIL));

        } else {
            String runDiscoveryQuery = "select c.username,c.password,c.community,c.version,d.ip,d.type,d.port from discovery as d JOIN credential as c on d.credential_profile=c.credential_id where d.discovery_id="
                    + Integer.parseInt(context.pathParam(ID)) + ";";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                            .put(QUERY, runDiscoveryQuery)
                            .put(REQUEST_POINT, DISCOVERY)
                            .put(METHOD_TYPE, RUN_CHECK_DATA),
                    eventBusHandler -> {
                        try {

                            if (eventBusHandler.succeeded()) {

                                var result = eventBusHandler.result().body().getJsonArray(DATA);

                                if (!result.isEmpty()) {

                                    promise.complete(result.getJsonObject(0));

                                } else {

                                    promise.fail("id not exists in discovery");

                                }

                            } else {

                                promise.fail(eventBusHandler.cause().getMessage());

                            }
                        } catch (Exception exception) {
                            LOG.error(EXCEPTION, exception);
                            promise.fail(exception.getMessage());

                        }
                    });

            future.onComplete(futureCompletedHandler -> {
                try {
                    if (futureCompletedHandler.succeeded()) {

                        var discoveryData = futureCompletedHandler.result();

                        eventBus.<JsonObject>request(DISCOVERY_EVENTBUS_ADDRESS,
                                discoveryData, eventBusHandler -> {
                                    try {

                                        if (eventBusHandler.succeeded()) {

                                            var discoveryResult = eventBusHandler.result().body();

                                            promiseDiscovery.complete(discoveryResult);

                                        } else {

                                            promiseDiscovery.fail(eventBusHandler.cause().getMessage());

                                        }
                                    } catch (Exception exception) {

                                        promiseDiscovery.fail(exception.getMessage());

                                    }
                                });

                    } else {

                        promiseDiscovery.fail(futureCompletedHandler.cause().getMessage());

                    }
                } catch (Exception exception) {
                    LOG.error(EXCEPTION, exception);
                    promiseDiscovery.fail(exception.getMessage());
                }
            });

            futureDiscovery.onComplete(futureCompleteHandler -> {
                try {

                    if (futureCompleteHandler.succeeded()) {

                        var discoveryData = futureCompleteHandler.result();
                        if (discoveryData.containsKey(TYPE) && !(discoveryData.getValue(TYPE).equals("network"))) {
                            discoveryData.remove(USERNAME);
                            discoveryData.remove(PASSWORD);
                        } else {
                            discoveryData.remove(COMMUNITY);
                            discoveryData.remove(VERSION);
                        }
                        discoveryData.remove(IP);
                        discoveryData.remove(PORT);
                        discoveryData.remove(TYPE);
                        discoveryData.remove("category");

                        String insertDiscoveryData = "update discovery set result =" + "'"
                                + discoveryData + "'" + " where discovery_id ="
                                + Integer.parseInt(context.pathParam(ID)) + ";";

                        eventBus.request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                                .put(QUERY, insertDiscoveryData)
                                .put(METHOD_TYPE, DISCOVERY_RESULT_INSERT), eventBusHandler -> {
                            try {

                                if (eventBusHandler.succeeded() && eventBusHandler.result().body() != null) {

                                    response(context, 200, new JsonObject()
                                            .put(STATUS, SUCCESS)
                                            .put(MESSAGE, "discovery successful"));
                                } else {

                                    response(context, 400, new JsonObject()
                                            .put(STATUS, FAIL)
                                            .put(MESSAGE, "discovery failed")
                                            .put(ERROR, eventBusHandler.cause().getMessage()));
                                }
                            } catch (Exception exception) {
                                LOG.error(EXCEPTION, exception);

                                response(context, 500, new JsonObject().put(STATUS, FAIL)
                                        .put(MESSAGE, "discovery failed")
                                        .put(ERROR, exception.getMessage()));
                            }
                        });
                    } else {

                        response(context, 400, new JsonObject()
                                .put(STATUS, FAIL)
                                .put(MESSAGE, "discovery failed")
                                .put(ERROR, futureCompleteHandler.cause().getMessage()));
                    }
                } catch (Exception exception) {
                    LOG.error(EXCEPTION, exception);
                    response(context, 500, new JsonObject().put(STATUS, FAIL)
                            .put(ERROR, exception.getMessage()));
                }
            });
        }
    }

    private void get(RoutingContext context) {
        LOG.info("discovery get all fn() called ...");

        try {

            var query = "Select * from discovery;";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put(QUERY, query)
                            .put(METHOD_TYPE, GET_DATA),
                    eventBusHandler -> {
                        try {

                            if (eventBusHandler.succeeded()) {

                                var result = eventBusHandler.result().body().getValue(DATA);

                                response(context, 200, new JsonObject()
                                        .put(STATUS, SUCCESS)
                                        .put(RESULT, result));

                            } else {

                                response(context, 400, new JsonObject()
                                        .put(STATUS, FAIL)
                                        .put(ERROR, eventBusHandler.cause().getMessage()));

                            }
                        } catch (Exception exception) {
                            LOG.error(EXCEPTION, exception);

                            response(context, 500, new JsonObject().put(STATUS, FAIL)
                                    .put(ERROR, exception.getMessage()));
                        }

                    });
        } catch (Exception exception) {
            LOG.error(EXCEPTION, exception);

            response(context, 400, new JsonObject().put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }
    }

    private void getById(RoutingContext context) {

        LOG.info("discovery get by id fn() called ");

        try {

            var query = "Select * from discovery where discovery_id ="
                    + context.pathParam(ID) + ";";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                    new JsonObject().put(QUERY, query)
                            .put(METHOD_TYPE, GET_DATA)
                            .put(REQUEST_POINT, DISCOVERY), eventBusHandler -> {
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
                            LOG.error(EXCEPTION, exception);

                            response(context, 500, new JsonObject().put(STATUS, FAIL)
                                    .put(ERROR, exception.getMessage()));
                        }

                    });

        } catch (Exception exception) {
            LOG.error(EXCEPTION, exception);

            response(context, 400, new JsonObject().put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));
        }
    }

    private void delete(RoutingContext context) {

        LOG.info("discovery delete fn() called ");

        try {

            var query = "delete from discovery where discovery_id =" + context.pathParam(ID) + ";";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put(QUERY, query)
                    .put(METHOD_TYPE, DELETE_DATA), eventBusHandler -> {
                try {

                    if (eventBusHandler.succeeded()) {

                        var deleteResult = eventBusHandler.result().body();

                        if (deleteResult.getString(STATUS).equals(SUCCESS)) {

                            response(context, 200, new JsonObject()
                                    .put(MESSAGE, "id " + context.pathParam(ID) + " deleted")
                                    .put(STATUS, SUCCESS));

                        } else {

                            response(context, 200, new JsonObject()
                                    .put(ERROR, eventBusHandler.result().body())
                                    .put(STATUS, FAIL));
                        }

                    } else {

                        response(context, 400, new JsonObject().put(STATUS, FAIL)
                                .put(ERROR, eventBusHandler.cause().getMessage()));
                    }
                } catch (Exception exception) {
                    LOG.error(EXCEPTION, exception);

                    response(context, 500, new JsonObject().put(STATUS, FAIL)
                            .put(ERROR, exception.getMessage()));
                }
            });

        } catch (Exception exception) {
            LOG.error(EXCEPTION, exception);

            response(context, 400, new JsonObject().put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));
        }
    }

    private void update(RoutingContext context) {

        LOG.info("discovery update fn() called ");

        try {

            var userData = context.getBodyAsJson();
            var id = userData.getInteger(DISCOVERY_ID);
            var queryStatement = new StringBuilder();
            userData.remove(DISCOVERY_ID);

            userData.forEach(value -> {

                var key = value.getKey().replace(".", "_");
                queryStatement.append(key).append(" = ").append("'")
                        .append(value.getValue()).append("'").append(",");

            });
            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                    new JsonObject().put(QUERY, "select * from discovery where discovery_id=" + id + ";")
                            .put(METHOD_TYPE, GET_DATA)
                            .put(REQUEST_POINT, DISCOVERY), eventBusHandler -> {
                        try {

                            if (eventBusHandler.succeeded() && eventBusHandler.result().body() != null) {

                                var result = eventBusHandler.result().body()
                                        .getJsonArray(DATA).getJsonObject(0);
                                var flag = new JsonObject().put(STATUS, FAIL);
                                userData.forEach(value -> {
                                    var key = value.getKey();
                                    if (!result.getValue(key).equals(value.getValue())) {
                                        flag.put(STATUS, SUCCESS);
                                    }
                                });

                                if (flag.getString(STATUS).equals(SUCCESS)) {
                                    queryStatement.append("result").append(" = ").append("'")
                                            .append("{}").append("'").append(",");
                                }
                            } else {
                                LOG.error(eventBusHandler.cause().getMessage());
                            }

                            if (queryStatement.length() > 0) {
                                var query = "update discovery set "
                                        + queryStatement.substring(0, queryStatement.length() - 1)
                                        + " where discovery_id = " + id + ";";


                                eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                                        new JsonObject().put(QUERY, query)
                                                .put(METHOD_TYPE, CREATE_DATA)
                                                .put(REQUEST_POINT, DISCOVERY), updateEventBusHandler -> {

                                            try {

                                                var result = updateEventBusHandler.result().body();

                                                if (updateEventBusHandler.succeeded() || result != null) {

                                                    response(context, 200, new JsonObject()
                                                            .put(STATUS, SUCCESS)
                                                            .put(MESSAGE, "id " + id + " updated"));

                                                } else {

                                                    response(context, 400, new JsonObject()
                                                            .put(STATUS, FAIL)
                                                            .put(ERROR, updateEventBusHandler.cause().getMessage()));

                                                }
                                            } catch (Exception exception) {
                                                LOG.error(EXCEPTION, exception);

                                                response(context, 500, new JsonObject()
                                                        .put(STATUS, FAIL)
                                                        .put(ERROR, exception.getMessage()));

                                            }
                                        });
                            } else {
                                response(context, 200, new JsonObject()
                                        .put(STATUS, SUCCESS).put(MESSAGE, "nothing changed"));
                            }
                        } catch (Exception exception) {
                            LOG.error(EXCEPTION, exception);

                            response(context, 400, new JsonObject()
                                    .put(MESSAGE, "wrong json format")
                                    .put(ERROR, exception.getMessage()));
                        }

                    });


        } catch (Exception exception) {
            LOG.error(EXCEPTION, exception);

            response(context, 400, new JsonObject()
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }
    }

    private void create(RoutingContext context) {
        LOG.info("discovery create fn() called ");
        try {
            var userData = context.getBodyAsJson();

            String query = "insert into discovery(discovery_name,ip,type,credential_profile,port)values('"
                    + userData.getString(DISCOVERY_NAME).trim() + "','"
                    + userData.getString(IP).trim() + "','"
                    + userData.getString(TYPE).trim() + "','"
                    + userData.getInteger(CREDENTIAL_PROFILE)
                    + "','" + userData.getInteger(PORT) + "');";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put(QUERY, query)
                    .put(METHOD_TYPE, CREATE_DATA)
                    .put(REQUEST_POINT, DISCOVERY), eventBusHandler -> {
                try {
                    if (eventBusHandler.succeeded()) {
                        var handlerResult = eventBusHandler.result().body().getJsonArray(DATA);
                        var id = handlerResult.getJsonObject(0).getInteger(ID);

                        response(context, 200, new JsonObject()
                                .put(MESSAGE, "discovery created successfully")
                                .put(STATUS, SUCCESS).put(ID, id));

                    } else {

                        response(context, 400, new JsonObject()
                                .put(STATUS, FAIL)
                                .put(ERROR, eventBusHandler.cause().getMessage()));
                    }
                } catch (Exception exception) {
                    LOG.error(EXCEPTION, exception);

                    response(context, 500, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(ERROR, exception.getMessage()));

                }

            });
        } catch (Exception exception) {
            LOG.error(EXCEPTION, exception);

            response(context, 400, new JsonObject().put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));


        }

    }

    private void validate(RoutingContext context) {

        LOG.info("Discovery Validate fn() called...");

        try {

            if ((context.request().method() != HttpMethod.DELETE)
                    && (context.request().method() != HttpMethod.GET)) {

                if (context.getBodyAsJson() == null) {

                    response(context, 400, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(MESSAGE, "wrong json format"));

                }

                var credentials = context.getBodyAsJson();

                credentials.forEach(value -> {

                    if (credentials.getValue(value.getKey()) instanceof String) {

                        credentials.put(value.getKey(), credentials.getString(value.getKey()).trim());

                    }

                });

                context.setBody(credentials.toBuffer());

            }

            switch (context.request().method().toString()) {

                case "POST" -> validateCreate(context);

                case "PUT" -> validateUpdate(context);

                case "DELETE", "GET" -> {

                    if (context.pathParam(ID) == null) {

                        response(context, 400, new JsonObject()
                                .put(MESSAGE, "id is null")
                                .put(STATUS, FAIL));

                    } else {

                        eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject().put(ID,
                                                Integer.parseInt(context.pathParam(ID)))
                                        .put(REQUEST_POINT, DISCOVERY)
                                        .put(METHOD_TYPE, CHECK_ID),
                                eventBusHandler -> {
                                    try {

                                        if (eventBusHandler.succeeded()
                                                && eventBusHandler.result().body() != null) {

                                            if (eventBusHandler.result().body().getString(STATUS)
                                                    .equals(FAIL)) {

                                                response(context, 400, new JsonObject().put(MESSAGE,
                                                                eventBusHandler.result().body()
                                                                        .getString(ERROR))
                                                        .put(STATUS, FAIL));

                                            } else {

                                                context.next();

                                            }

                                        } else {

                                            response(context, 400, new JsonObject()
                                                    .put(STATUS, FAIL)
                                                    .put(ERROR, eventBusHandler.cause().getMessage()));

                                        }
                                    } catch (Exception exception) {
                                        LOG.error(EXCEPTION, exception);

                                        response(context, 500, new JsonObject()
                                                .put(STATUS, FAIL)
                                                .put(ERROR, exception.getMessage()));

                                    }

                                });
                    }
                }

                default -> response(context, 400, new JsonObject()
                        .put(STATUS, FAIL)
                        .put(MESSAGE, "wrong method request"));
            }


        } catch (Exception exception) {
            LOG.error(EXCEPTION, exception);

            response(context, 500, new JsonObject().put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }

    }

    private void validateUpdate(RoutingContext context) {

        var errors = new ArrayList<String>();

        if (!context.getBodyAsJson().containsKey(DISCOVERY_ID)
                || !(context.getBodyAsJson().getValue(DISCOVERY_ID) instanceof Integer)) {

            errors.add("discovery.id is required");

        }
        if (context.getBodyAsJson().containsKey(DISCOVERY_NAME)
                && (!(context.getBodyAsJson().getValue(DISCOVERY_NAME) instanceof String)
                || context.getBodyAsJson().getString(DISCOVERY_NAME).isEmpty())) {
            errors.add("discovery.name is required");

        }
        if (context.getBodyAsJson().containsKey(IP)
                && (!(context.getBodyAsJson().getValue(IP) instanceof String)
                || context.getBodyAsJson().getString(IP).isEmpty())) {
            errors.add("IP is required");
        }
        if (context.getBodyAsJson().containsKey(CREDENTIAL_PROFILE)
                && (!(context.getBodyAsJson().getValue(CREDENTIAL_PROFILE) instanceof Integer)
                || context.getBodyAsJson().getString(CREDENTIAL_PROFILE).isEmpty())) {
            errors.add("credential.profile is required");
        }
        if (context.getBodyAsJson().containsKey(PORT)
                && (!(context.getBodyAsJson().getValue(PORT) instanceof Integer)
                || context.getBodyAsJson().getString(PORT).isEmpty())) {
            errors.add("port is required");
        } else if (context.getBodyAsJson().containsKey(PORT)
                && (context.getBodyAsJson().getInteger(PORT) < 0
                || context.getBodyAsJson().getInteger(PORT) > 65535)) {
            errors.add("wrong port range");
        }
        if (errors.isEmpty()) {

            eventBus.request(DATABASE_EVENTBUS_ADDRESS, context.getBodyAsJson()
                    .put(METHOD_TYPE, CHECK_DISCOVERY_UPDATES)
                    .put(REQUEST_POINT, DISCOVERY), eventBusHandler -> {
                try {

                    if (eventBusHandler.succeeded()) {

                        context.next();

                    } else {

                        response(context, 400, new JsonObject().put(STATUS, FAIL)
                                .put(ERROR, eventBusHandler.cause().getMessage()));

                    }
                } catch (Exception exception) {
                    LOG.error(EXCEPTION, exception);

                    response(context, 500, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(ERROR, exception.getMessage()));

                }
            });

        } else {

            response(context, 400, new JsonObject().put(ERROR, errors)
                    .put(STATUS, FAIL));

        }
    }

    private void validateCreate(RoutingContext context) {

        var errors = new ArrayList<String>();

        if (!context.getBodyAsJson().containsKey(DISCOVERY_NAME)
                || !(context.getBodyAsJson().getValue(DISCOVERY_NAME) instanceof String)
                || context.getBodyAsJson().getString(DISCOVERY_NAME).isEmpty()) {

            errors.add("discovery.name is required");
            LOG.error("discovery.name is required");

        }

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

        if (!context.getBodyAsJson().containsKey(CREDENTIAL_PROFILE)
                || !(context.getBodyAsJson().getValue(CREDENTIAL_PROFILE) instanceof Integer)
                || context.getBodyAsJson().getInteger(CREDENTIAL_PROFILE) == null) {

            errors.add("credential.profile is required (int)");
            LOG.error("credential.profile is required");

        }

        if (!context.getBodyAsJson().containsKey(PORT)
                || !(context.getBodyAsJson().getValue(PORT) instanceof Integer)
                || context.getBodyAsJson().getInteger(PORT) == null) {

            errors.add("port is required (int)");
            LOG.error("port is required");

        } else if (context.getBodyAsJson().containsKey(PORT)
                && (context.getBodyAsJson().getInteger(PORT) < 0
                || context.getBodyAsJson().getInteger(PORT) > 65535)) {
            errors.add("wrong port range");
        }

        if (errors.isEmpty()) {

            eventBus.request(DATABASE_EVENTBUS_ADDRESS, context.getBodyAsJson()
                    .put(METHOD_TYPE, CHECK_MULTI_FIELDS)
                    .put(REQUEST_POINT, DISCOVERY), eventBusHandler -> {
                try {

                    if (eventBusHandler.succeeded()) {

                        context.next();

                    } else {

                        response(context, 400, new JsonObject()
                                .put(STATUS, FAIL)
                                .put(ERROR, eventBusHandler.cause().getMessage()));

                    }

                } catch (Exception exception) {
                    LOG.error(EXCEPTION, exception);

                    response(context, 500, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(ERROR, exception.getMessage()));

                }

            });
        } else {

            response(context, 400, new JsonObject()
                    .put(ERROR, errors).put(STATUS, FAIL));

        }
    }

    private void response(RoutingContext context, int statusCode, JsonObject reply) {

        var httpResponse = context.response();

        httpResponse.setStatusCode(statusCode)
                .putHeader(HEADER_TYPE, HEADER_VALUE)
                .end(reply.encodePrettily());

    }
}
