package com.mindarray;

import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.mindarray.Constant.*;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class.getName());

    public static Future<JsonObject> checkSystemStatus(JsonObject credential) {

        Promise<JsonObject> promise = Promise.promise();

        if ((!credential.containsKey(IP)) || credential.getString(IP) == null) {

            promise.fail("IP address is null in check system status");

        } else {

            Bootstrap.vertx.executeBlocking(blockingHandler -> {
                NuProcess process = null;
                try {
                    var processBuilder = new NuProcessBuilder(Arrays
                            .asList("fping", "-c", "3", "-t", "1000", "-q",
                                    credential.getString(IP)));

                    var handler = new ProcessHandler();

                    processBuilder.setProcessListener(handler);

                     process = processBuilder.start();


                    process.waitFor(4000, TimeUnit.MILLISECONDS);


                    var result = handler.output();
                    if (result != null && !result.trim().isEmpty()) {
                        blockingHandler.complete(result);
                    } else {

                        blockingHandler.fail("ping fail..." + credential.getString(IP));
                    }
                } catch (Exception exception) {
                   LOG.error(EXCEPTION,exception);
                    blockingHandler.fail(exception.getMessage());
                }
                finally {

                    if (process != null) {

                        process.destroy(true);

                    }
                }

            }).onComplete(completeHandler -> {
                try {
                    if (completeHandler.succeeded()) {

                        var result = completeHandler.result().toString();

                        if (result == null) {

                            promise.fail(" Request time out occurred");
                            LOG.error("Request time out occurred");

                        } else {

                            var pattern = Pattern
                                    .compile("\\d+.\\d+.\\d+.\\d+\\s*:\\s*\\w+\\/\\w+\\/%\\w+\\s*=\\s*\\d+\\/\\d+\\/(\\d+)%");
                            var matcher = pattern.matcher(result);
                            if ((matcher.find()) && (!matcher.group(1).equals("0"))) {
                                promise.fail("ping fail and loss percentage is :" + matcher.group(1));
                            } else {
                                promise.complete(credential.put(STATUS, SUCCESS));

                            }
                        }

                    } else {

                        promise.fail(completeHandler.cause().getMessage());

                    }
                }catch (Exception exception) {
                    promise.fail(exception.getCause().getMessage());
                   LOG.error(EXCEPTION,exception);
                }
            });
        }

        return promise.future();
    }

    public static Future<JsonObject> checkPort(@NotNull JsonObject credential) {

        LOG.info("check port called...");

        Promise<JsonObject> promise = Promise.promise();

        if (credential.getString(TYPE).equals(NETWORK)) {

            promise.complete(new JsonObject().put(STATUS, SUCCESS));


        } else {

            String ip = credential.getString(IP);

            int port = credential.getInteger(PORT);

            try (var s = new Socket(ip, port)) {
                promise.complete(new JsonObject().put(STATUS, SUCCESS));
            } catch (Exception exception) {
               LOG.error(EXCEPTION,exception);
                promise.fail(exception.getMessage());
            }
        }

        return promise.future();
    }

    public static Future<JsonObject> spawnProcess(JsonObject credential) {

        LOG.info("spawn process called...");

        Promise<JsonObject> promise = Promise.promise();

        if (credential == null) {

            LOG.error("credential is null");
            promise.fail("credential is null");

        } else {
            credential.remove(STATUS);
            Bootstrap.vertx.<JsonObject>executeBlocking(blockingHandler -> {

                NuProcess process = null;

                try {
                    String encoder = (Base64.getEncoder()
                            .encodeToString((credential).toString().getBytes(StandardCharsets.UTF_8)));


                    var processBuilder = new NuProcessBuilder(Arrays.asList("./nms", encoder));
                    var handler = new ProcessHandler();

                    processBuilder.setProcessListener(handler);

                    process = processBuilder.start();

                    process.waitFor(20, TimeUnit.SECONDS);


                    var handlerResult = handler.output();

                    if (handlerResult != null && !handlerResult.trim().isEmpty()) {

                        blockingHandler.complete(new JsonObject(handlerResult));

                    } else {

                        blockingHandler.fail("request timeout occurred");

                    }
                } catch (Exception exception) {
                   LOG.error(EXCEPTION,exception);

                    blockingHandler.fail(exception.getMessage());

                } finally {

                    if (process != null) {

                        process.destroy(true);

                    }
                }

            }).onComplete(completeHandler -> {
                try {
                    if (completeHandler.succeeded()) {
                        if (completeHandler.result() != null
                                && completeHandler.result().getString(STATUS).equals(SUCCESS)) {
                            promise.complete(completeHandler.result());
                        } else {
                            promise.fail(completeHandler.result().getString(ERROR));
                        }

                    } else {
                        promise.fail(completeHandler.cause().getMessage());
                    }
                } catch (Exception exception) {
                   LOG.error(EXCEPTION,exception);

                }
            });


        }

        return promise.future();
    }

    public static Map<String, JsonArray> inputValidation() {

        HashMap<String, JsonArray> inputValidation = new HashMap<>();

        inputValidation.put(LINUX, new JsonArray().add(DISCOVERY_ID)
                .add(DISCOVERY_NAME).add(CREDENTIAL_PROFILE).add(TYPE)
                .add(PORT).add(IP));

        inputValidation.put(WINDOWS, new JsonArray().add(DISCOVERY_ID)
                .add(DISCOVERY_NAME).add(CREDENTIAL_PROFILE).add(TYPE)
                .add(PORT).add(IP));

        inputValidation.put(NETWORK, new JsonArray().add(DISCOVERY_ID)
                .add(DISCOVERY_NAME).add(CREDENTIAL_PROFILE).add(TYPE)
                .add(PORT).add(IP));

        inputValidation.put(SNMP, new JsonArray().add(PROTOCOL).add(CREDENTIAL_NAME)
                .add(CREDENTIAL_ID).add(COMMUNITY).add(VERSION));

        inputValidation.put(SSH, new JsonArray().add(PROTOCOL).add(CREDENTIAL_NAME)
                .add(CREDENTIAL_ID).add(USERNAME).add(PASSWORD));

        inputValidation.put(POWERSHELL, new JsonArray().add(PROTOCOL)
                .add(CREDENTIAL_NAME).add(CREDENTIAL_ID)
                .add(USERNAME).add(PASSWORD));

        return inputValidation;
    }
}
