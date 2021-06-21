package com.redhat.cloud.notifications;

import com.redhat.cloud.notifications.ingress.Action;
import com.redhat.cloud.notifications.ingress.Event;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.metrics.Counter;
import org.eclipse.microprofile.metrics.annotation.Metric;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


@ApplicationScoped
@Path("/notifications")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class GwResource {

    @ConfigProperty(name = "mp.messaging.outgoing.egress.enabled", defaultValue = "true")
    boolean kafkaEnabled;

    @ConfigProperty(name = "k.sink")
    Optional<String> knativeSink;

    @Inject
    @Metric(name = "notifications.gw.received")
    Counter receivedActions;

    @Inject
    @Metric(name = "notifications.gw.forwarded")
    Counter forwardedActions;

    @POST
    @Operation(summary = "Forward one message to the notification system")
    @APIResponses({
        @APIResponse(responseCode = "200", description = "Message forwarded"),
        @APIResponse(responseCode = "403", description = "No permission"),
        @APIResponse(responseCode = "401"),
        @APIResponse(responseCode = "400", description = "Incoming message was not valid")
    })
    public Response forward(@NotNull @Valid RestAction ra) {
        receivedActions.inc();

        Action.Builder builder = Action.newBuilder();
        LocalDateTime parsedTime = LocalDateTime.parse(ra.timestamp, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        builder.setTimestamp(parsedTime);
        builder.setContext(new HashMap());
        List<Event> eventList = new ArrayList<>(1);
        com.redhat.cloud.notifications.ingress.Metadata meta = new com.redhat.cloud.notifications.ingress.Metadata();
        Event event = new Event(meta, ra.getPayload());
        eventList.add(event);
        builder.setEvents(eventList);
        builder.setEventType(ra.eventType);
        builder.setApplication(ra.application);
        builder.setBundle(ra.bundle);
        builder.setAccountId(ra.accountId);

        Action message = builder.build();

        try {
            String serializedAction = serializeAction(message);
            if (kafkaEnabled) {
                KafkaSender ks = new KafkaSender();
                ks.send(serializedAction);
            } else {
                sendToKnative(serializedAction, knativeSink, message.getAccountId());
            }
            forwardedActions.inc();
        } catch (Exception e) {
            e.printStackTrace();  // TODO: Customise this generated block
            return Response.serverError().entity(e.getMessage()).build();
        }

        return Response.ok().build();
    }

    private void sendToKnative(String ce, Optional<String> sink, String account) throws IOException, InterruptedException {

        if (sink.isEmpty()) {
            throw new IllegalStateException("No K_SINK provided");
        }

        HttpClient client = HttpClient.newBuilder().build();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(sink.get()))
                .header("Ce-Id","notification-gw-" + account + "-" + UUID.randomUUID()) // This has to be unique
                .header("Ce-Specversion", "1.0")
                .header("Ce-Type", "com.redhat.cloud.notification")
                .header("Ce-Source", "notifications-gw")
                .header("Content-Type","application/json")
                .header("Ce-rhaccount", account) // custom property, must be [a-z0-9]+
                .POST(HttpRequest.BodyPublishers.ofString(ce))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (!((response.statusCode() / 100 ) == 2)) {
            throw new IOException("Status was " + response.statusCode());
        }

    }

    @GET
    @Path("/sample")
    public Response getSample() {
        RestAction a = new RestAction();
        a.setAccountId("123");
        a.setBundle("my-bundle");
        a.setApplication("my-app");
        a.setEventType("a type");
        Map<String, Object> payload = new HashMap<>();
        payload.put("key1","value1");
        payload.put("key2","value2");
        a.setPayload(payload);
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        a.setTimestamp(LocalDateTime.now().format(formatter));

        return Response.ok().entity(a).build();
    }

    public static String serializeAction(Action action) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(Action.getClassSchema(), baos);
        DatumWriter<Action> writer = new SpecificDatumWriter<>(Action.class);
        writer.write(action, jsonEncoder);
        jsonEncoder.flush();

        return baos.toString(StandardCharsets.UTF_8);
    }

}
