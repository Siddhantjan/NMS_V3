package com.mindarray.nms;

import com.mindarray.Utils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.mindarray.Constant.*;

public class DiscoveryEngine extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(DiscoveryEngine.class.getName());

    @Override
    public void start(Promise<Void> startPromise) {

        var eventBus = vertx.eventBus();

        eventBus.<JsonObject>localConsumer(DISCOVERY_EVENTBUS_ADDRESS, discoveryEventBusHandler -> {
            try {

                var discoveryData = discoveryEventBusHandler.body()
                        .put("category", DISCOVERY);

                Utils.checkSystemStatus(discoveryData)
                        .compose(future -> Utils.checkPort(discoveryData))
                        .compose(future -> Utils.spawnProcess(discoveryData))
                        .onComplete(futureCompleteHandler -> {
                            try {
                                if (futureCompleteHandler.succeeded()) {

                                    if (futureCompleteHandler.result().containsKey(STATUS)
                                            && futureCompleteHandler.result().getString(STATUS)
                                            .equals(SUCCESS)) {

                                        LOG.info("Discovery successful");
                                        discoveryEventBusHandler.reply(futureCompleteHandler.result());

                                    } else {


                                        if (futureCompleteHandler.result().containsKey(ERROR)) {
                                            discoveryEventBusHandler.fail(-1, futureCompleteHandler.result()
                                                    .getValue(ERROR).toString());
                                            LOG.error(futureCompleteHandler.result().getValue(ERROR).toString());
                                        } else {
                                            discoveryEventBusHandler.fail(-1, "discovery failed");
                                        }
                                    }

                                } else {

                                    LOG.error(futureCompleteHandler.cause().getMessage());

                                    discoveryEventBusHandler.fail(-1, futureCompleteHandler.cause().getMessage());
                                }
                            }catch (Exception exception){
                              LOG.error(EXCEPTION,exception);
                                discoveryEventBusHandler.fail(-1,exception.getCause().getMessage());
                            }
                        });
            } catch (Exception exception) {
              LOG.error(EXCEPTION,exception);

                discoveryEventBusHandler.fail(-1, exception.getCause().getMessage());

            }
        });

        startPromise.complete();
    }
}
