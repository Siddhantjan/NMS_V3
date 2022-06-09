package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.Utils;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import static com.mindarray.Constant.*;

public class Credential {
    private static final Logger LOG = LoggerFactory.getLogger(Credential.class.getName());
    private final EventBus eventBus = Bootstrap.vertx.eventBus();

    public void init(Router router) {

        LOG.info("Credential init () called...");

        router.route().method(HttpMethod.POST).path(CREDENTIAL_POINT)
                .handler(this::validate).handler(this::create);

        router.route().method(HttpMethod.PUT).path(CREDENTIAL_POINT + "/:id/")
                .handler(this::inputValidate).handler(this::validate).handler(this::update);

        router.route().method(HttpMethod.DELETE).path(CREDENTIAL_POINT + "/:id/")
                .handler(this::validate).handler(this::delete);

        router.route().method(HttpMethod.GET).path(CREDENTIAL_POINT).handler(this::get);

        router.route().method(HttpMethod.GET).path(CREDENTIAL_POINT + "/:id/")
                .handler(this::validate).handler(this::getById);

    }

    private void inputValidate(RoutingContext context) {

        LOG.info("remove unwanted parameters fn() called");
        try {
            if (context.pathParam(ID) == null) {

                response(context, 400, new JsonObject()
                        .put(MESSAGE, "id is null")
                        .put(STATUS, FAIL));

            }
            else {
                var query = "select protocol from credential where credential_id=" + Integer.parseInt(context.pathParam(ID));
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

                                        if (contextData.containsKey(PROTOCOL)) {
                                            contextData.remove(PROTOCOL);
                                        }

                                        var type = data.getString(PROTOCOL);

                                        var keys = contextData.fieldNames();
                                        keys.removeIf(key -> !validateInputArray.get(type).contains(key));
                                        context.setBody(contextData.put(CREDENTIAL_ID,
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
          LOG.error(EXCEPTION,exception);
            response(context, 500, new JsonObject()
                    .put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }
    }

    private void getById(RoutingContext context) {

        LOG.info("credential get by id fn() called");

        try {

            var query = "Select * from credential where credential_id =" + context.pathParam(ID) + ";";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                    .put(QUERY, query)
                    .put(METHOD_TYPE, GET_DATA)
                    .put(REQUEST_POINT, CREDENTIAL), eventBusHandler -> {

                try {

                    if (eventBusHandler.succeeded()) {

                        var result = eventBusHandler.result().body().getValue(DATA);

                        response(context, 200, new JsonObject()
                                .put(STATUS, SUCCESS).put(RESULT, result));

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

            response(context, 400, new JsonObject()
                    .put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }
    }

    private void get(RoutingContext context) {

        LOG.info("credential get all fn() called");

        try {
            var query = "Select * from credential;";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                    new JsonObject().put(QUERY, query)
                            .put(METHOD_TYPE, GET_DATA),
                    eventBusHandler -> {

                        try {

                            if (eventBusHandler.succeeded()) {

                                var result = eventBusHandler.result().body()
                                        .getJsonArray(DATA);

                                response(context, 200, new JsonObject()
                                        .put(STATUS, SUCCESS)
                                        .put(RESULT, result));

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

        } catch (Exception exception) {
           LOG.error(EXCEPTION,exception);

            response(context, 400, new JsonObject()
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));
        }
    }

    private void delete(RoutingContext context) {

        LOG.info("credential delete function called");
        try {

            var query = "delete from credential where credential_id ="
                    + context.pathParam(ID) + ";";

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                            .put(QUERY, query)
                            .put(METHOD_TYPE, DELETE_DATA),
                    eventBusHandler -> {
                        try {


                            if (eventBusHandler.succeeded()) {
                                var deleteResult = eventBusHandler.result().body();

                                if (deleteResult != null) {
                                    response(context, 200, new JsonObject()
                                            .put(MESSAGE, "id "
                                                    + context.pathParam(ID) + " deleted")
                                            .put(STATUS, SUCCESS));
                                }

                            } else {

                                response(context, 400, new JsonObject()
                                        .put(STATUS, FAIL)
                                        .put(ERROR, eventBusHandler.result().body()));

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

            response(context, 400, new JsonObject()
                    .put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));
        }
    }

    private void update(RoutingContext context) {

        LOG.info("credential update fn () called ");

        try {

            var userData = context.getBodyAsJson();

            var id = userData.getInteger(CREDENTIAL_ID);

            userData.remove(CREDENTIAL_ID);

            var queryStatement = new StringBuilder();

            userData.forEach(value -> {

                var key = value.getKey().replace(".", "_");
                queryStatement.append(key).append(" = ").append("\"")
                        .append(value.getValue()).append("\"").append(",");

            });
            if (queryStatement.length() > 0) {
                var query = "update credential set "
                        + queryStatement.substring(0, queryStatement.length() - 1)
                        + " where credential_id = " + id + ";";

                eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                                .put(QUERY, query)
                                .put(METHOD_TYPE, UPDATE_DATA)
                                .put(REQUEST_POINT, CREDENTIAL),
                        eventBusHandler -> {
                            try {

                                var result = eventBusHandler.result().body();

                                if (eventBusHandler.succeeded() || result != null) {

                                    response(context, 200, new JsonObject()
                                            .put(MESSAGE, id + " updated")
                                            .put(STATUS, SUCCESS));

                                } else {

                                    response(context, 400, new JsonObject()
                                            .put(STATUS, FAIL)
                                            .put(ERROR, eventBusHandler.result().body()));

                                }

                            } catch (Exception exception) {
                               LOG.error(EXCEPTION,exception);

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
           LOG.error(EXCEPTION,exception);

            response(context, 400, new JsonObject()
                    .put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }

    }

    private void create(RoutingContext context) {

        LOG.info("Credential create called....");

        try {
            StringBuilder values = new StringBuilder();
            var userData = context.getBodyAsJson();

            if (userData.getString(PROTOCOL).equals(SSH)
                    || userData.getString(PROTOCOL).equals(POWERSHELL)) {

                values
                        .append("insert into credential(credential_name,protocol,username,password)values(")
                        .append("'").append(userData.getString(CREDENTIAL_NAME).trim())
                        .append("','").append(userData.getString(PROTOCOL).trim()).append("','")
                        .append(userData.getString(USERNAME).trim()).append("','")
                        .append(userData.getString(PASSWORD).trim()).append("');");


            } else if (userData.getString(PROTOCOL).equals(SNMP)) {

                values
                        .append("insert into credential(credential_name,protocol,community,version)values(")
                        .append("'").append(userData.getString(CREDENTIAL_NAME).trim())
                        .append("','").append(userData.getString(PROTOCOL).trim()).append("','")
                        .append(userData.getString(COMMUNITY).trim()).append("','")
                        .append(userData.getString(VERSION).trim()).append("');");

            }

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                    new JsonObject().put(QUERY, values)
                            .put(METHOD_TYPE, CREATE_DATA)
                            .put(REQUEST_POINT, CREDENTIAL),

                    eventBusHandler -> {

                        try {

                            if (eventBusHandler.succeeded()) {

                                var handlerResult = eventBusHandler.result().body()
                                        .getJsonArray(DATA);

                                var id = handlerResult.getJsonObject(0).getInteger(ID);

                                response(context, 200, new JsonObject()
                                        .put(MESSAGE, "credential created successfully")
                                        .put(STATUS, SUCCESS).put(ID, id));

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

        } catch (Exception exception) {
           LOG.error(EXCEPTION,exception);

            response(context, 400, new JsonObject()
                    .put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }
    }

    private void validate(RoutingContext context) {

        LOG.info("credential validate fn() called");

        try {

            if ((context.request().method() != HttpMethod.DELETE)
                    && (context.request().method() != HttpMethod.GET)) {

                if (context.getBodyAsJson() == null) {

                    response(context, 400, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(MESSAGE, "json is required"));

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

                case "DELETE" -> validateDelete(context);

                case "GET" -> validateGet(context);

                default -> response(context, 400, new JsonObject()
                        .put(STATUS, FAIL)
                        .put(MESSAGE, "wrong method"));

            }

        } catch (Exception exception) {
           LOG.error(EXCEPTION,exception);

            response(context, 400, new JsonObject()
                    .put(STATUS, FAIL)
                    .put(MESSAGE, "wrong json format")
                    .put(ERROR, exception.getMessage()));

        }

    }

    private void validateGet(RoutingContext context) {


        if (context.pathParam(ID) == null) {
            response(context, 400, new JsonObject()
                    .put(MESSAGE, "id is null").put(STATUS, FAIL));

        } else {

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                    .put(ID, Integer.parseInt(context.pathParam(ID)))
                    .put(REQUEST_POINT, CREDENTIAL)
                    .put(METHOD_TYPE, CHECK_ID), eventBusHandler -> {

                try {

                    if (eventBusHandler.succeeded() && eventBusHandler.result().body() != null) {

                        if (eventBusHandler.succeeded()) {

                            if (eventBusHandler.result().body().getString(STATUS).equals(FAIL)) {

                                response(context, 400, new JsonObject()
                                        .put(ERROR, eventBusHandler.result().body().getString(ERROR))
                                        .put(STATUS, FAIL));

                            } else {

                                context.next();

                            }

                        }

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
    }

    private void validateDelete(RoutingContext context) {

        if (context.pathParam(ID) == null) {

            response(context, 400, new JsonObject()
                    .put(MESSAGE, "id is null").put(STATUS, FAIL));

        } else {

            eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS, new JsonObject()
                    .put(ID, Integer.parseInt(context.pathParam(ID)))
                    .put(REQUEST_POINT, CREDENTIAL)
                    .put(METHOD_TYPE, CHECK_CREDENTIAL_DELETE), eventBusHandler -> {
                try {

                    if (eventBusHandler.succeeded()) {
                        if (eventBusHandler.result().body().getString(STATUS).equals(FAIL)) {

                            response(context, 400, new JsonObject()
                                    .put(ERROR, eventBusHandler.result().body().getString(ERROR))
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
                   LOG.error(EXCEPTION,exception);

                    response(context, 500, new JsonObject()
                            .put(STATUS, FAIL)
                            .put(ERROR, exception.getMessage()));
                }
            });
        }
    }

    private void validateUpdate(RoutingContext context) {
        var errors = new ArrayList<String>();

        eventBus.<JsonObject>request(DATABASE_EVENTBUS_ADDRESS,
                context.getBodyAsJson().put(METHOD_TYPE, CHECK_CREDENTIAL_UPDATES)
                        .put(REQUEST_POINT, CREDENTIAL), eventBusHandler -> {
                    try {

                        if (eventBusHandler.succeeded() && eventBusHandler.result().body() != null) {

                            if (eventBusHandler.result().body().getString(STATUS).equals(FAIL)) {

                                response(context, 400, new JsonObject().put(MESSAGE,
                                                eventBusHandler.result().body().getString(ERROR))
                                        .put(STATUS, FAIL));

                            } else {

                                var validationData = eventBusHandler.result().body().getJsonArray(DATA);

                                if (validationData.getJsonObject(0).getString(PROTOCOL).equals(SSH)
                                        || validationData.getJsonObject(0).getString(PROTOCOL)
                                        .equals(POWERSHELL)) {

                                    if (context.getBodyAsJson().containsKey(USERNAME)
                                            && (context.getBodyAsJson().getString(USERNAME).isEmpty()
                                            || !(context.getBodyAsJson().getValue(USERNAME) instanceof String))) {

                                        errors.add("username is required");
                                    }
                                    if (context.getBodyAsJson().containsKey(PASSWORD)
                                            && (context.getBodyAsJson().getString(PASSWORD).isEmpty()
                                            || !(context.getBodyAsJson().getValue(PASSWORD) instanceof String))) {

                                        errors.add("password is required");
                                    }
                                }
                                if (validationData.getJsonObject(0).getString(PROTOCOL)
                                        .equals(SNMP)) {

                                    if (context.getBodyAsJson().containsKey(COMMUNITY)
                                            && (context.getBodyAsJson().getString(COMMUNITY).isEmpty()
                                            || !(context.getBodyAsJson().getValue(COMMUNITY) instanceof String))) {

                                        errors.add("community is required");
                                    }

                                    if (context.getBodyAsJson().containsKey(VERSION)
                                            && (context.getBodyAsJson().getString(VERSION).isEmpty()
                                            || !(context.getBodyAsJson().getValue(VERSION) instanceof String))) {

                                        errors.add("version is required");
                                    }
                                }

                                if (errors.isEmpty()) {

                                    context.next();

                                } else {

                                    response(context, 400, new JsonObject()
                                            .put(MESSAGE, errors).put(STATUS, FAIL));

                                }
                            }
                        } else {

                            response(context, 500, new JsonObject()
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

    private void validateCreate(RoutingContext context) {
        var errors = new ArrayList<String>();

        if (!context.getBodyAsJson().containsKey(CREDENTIAL_NAME)
                || !(context.getBodyAsJson().getValue(CREDENTIAL_NAME) instanceof String)
                || context.getBodyAsJson().getString(CREDENTIAL_NAME).isEmpty()) {

            errors.add("credential.name is required");
            LOG.error("credential.name is required");
        }

        if (!context.getBodyAsJson().containsKey(PROTOCOL)
                || context.getBodyAsJson().getString(PROTOCOL).isEmpty()
                || !(context.getBodyAsJson().getValue(PROTOCOL) instanceof String)) {

            errors.add("protocol is required");
            LOG.error("protocol is required");

        } else if (!context.getBodyAsJson().getString(PROTOCOL).equals(SSH)
                && !context.getBodyAsJson().getString(PROTOCOL).equals(POWERSHELL)
                && !context.getBodyAsJson().getString(PROTOCOL).equals(SNMP)) {

            errors.add("wrong protocol");

        } else if (context.getBodyAsJson().getString(PROTOCOL).equals(SSH)
                || context.getBodyAsJson().getString(PROTOCOL).equals(POWERSHELL)) {

            if (!context.getBodyAsJson().containsKey(USERNAME)
                    || context.getBodyAsJson().getString(USERNAME).isEmpty()
                    || !(context.getBodyAsJson().getValue(USERNAME) instanceof String)) {

                errors.add("username is required");

            }

            if (!context.getBodyAsJson().containsKey(PASSWORD)
                    || context.getBodyAsJson().getString(PASSWORD).isEmpty()
                    || !(context.getBodyAsJson().getValue(PASSWORD) instanceof String)) {

                errors.add("password is required");

            }

        } else if (context.getBodyAsJson().getString(PROTOCOL).equals(SNMP)) {


            if (!context.getBodyAsJson().containsKey(COMMUNITY)
                    || context.getBodyAsJson().getString(COMMUNITY).isEmpty()
                    || !(context.getBodyAsJson().getValue(COMMUNITY) instanceof String)) {

                errors.add("community is required");

            }

            if (!context.getBodyAsJson().containsKey(VERSION)
                    || context.getBodyAsJson().getString(VERSION).isEmpty()
                    || !(context.getBodyAsJson().getValue(VERSION) instanceof String)) {

                errors.add("version is required");

            }
        }

        if (errors.isEmpty()) {

            eventBus.request(DATABASE_EVENTBUS_ADDRESS, context.getBodyAsJson()
                    .put(METHOD_TYPE, CHECK_MULTI_FIELDS)
                    .put(REQUEST_POINT, CREDENTIAL), eventBusHandler -> {
                try {

                    if (eventBusHandler.succeeded()) {


                        context.next();

                    } else {

                        response(context, 400, new JsonObject()
                                .put(STATUS, FAIL)
                                .put(ERROR, eventBusHandler.cause().getMessage()));

                    }
                } catch (Exception exception) {
                   LOG.error(EXCEPTION,exception);

                    response(context, 500, new JsonObject().put(STATUS, FAIL)
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
