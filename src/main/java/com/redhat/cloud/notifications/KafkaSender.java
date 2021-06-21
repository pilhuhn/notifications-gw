package com.redhat.cloud.notifications;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;

import javax.inject.Inject;

/**
 * Kafka sending in its own class
 * This is to only late inject the emitter if it is
 * really used.
 */
public class KafkaSender {

    @Inject
    @Channel("egress")
    Emitter<String> emitter;

    public void send(String serializedAction) {
        emitter.send(serializedAction);
    }
}
