package org.dcache.telemetry;

import static java.time.temporal.ChronoUnit.SECONDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import dmg.util.command.Command;
import org.dcache.util.CDCScheduledExecutorServiceDecorator;
import org.dcache.util.FireAndForgetTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * This is a cell, that collects information about the dCache-instance and sends it once per hour to
 * a central database. It captures the version and the storage from dCache directly. The location
 * and a siteid are read from collectdata.properties. The location consists of latitude and
 * longitude. It's possible to set the values to 0 to omit the real location. Sending the
 * information regularly is implemented with a ScheduledExecutor, which executes sendData() at an
 * interval of one hour.
 */

public class SendData implements CellCommandListener, CellLifeCycleAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendData.class);

    private ScheduledExecutorService sendDataExecutor;
    private InstanceData instanceData;
    private URI uri;
    private HttpClient httpClient;
    private boolean enable;
    private static final int HTTP_TIMEOUT = 60;

    @Required
    public void setUrlStr(String url) {
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            LOGGER.error("Failed to create URL. Reason: {}", e.toString());
            throw new RuntimeException();
        }
    }

    @Required
    public void setInstanceData(InstanceData instanceData) {
        this.instanceData = instanceData;
    }

    @Required
    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public SendData() {
        sendDataExecutor = new CDCScheduledExecutorServiceDecorator(
              Executors.newScheduledThreadPool(1));
    }

    @Override
    public void beforeStop() {
        sendDataExecutor.shutdown();
    }

    @Override
    public void afterStart() {
        if (!enable) {
            throw new RuntimeException(
                  "Telemetry cell is configured but not enabled. Configure the enable setting " +
                        "to run telemetry cell.");
        }
        LOGGER.warn("Sending information about dCache-instance to {} is activated.", uri);
        httpClient = HttpClient.newHttpClient();
        sendDataExecutor.scheduleAtFixedRate(new FireAndForgetTask(this::sendData),
              0, 1, TimeUnit.HOURS);
    }

    /**
     * sendData() sends the data. The information are updated and converted to a JSON-formatted
     * string first.
     */

    private void sendData() {
        instanceData.updateData();

        ObjectMapper jackson = new ObjectMapper();

        try {
            HttpRequest httpRequest = HttpRequest.newBuilder()
                  .uri(uri)
                  .version(HttpClient.Version.HTTP_2)
                  .header("Content-Type", "application/json")
                  .timeout(Duration.of(HTTP_TIMEOUT, SECONDS))
                  .POST(HttpRequest.BodyPublishers.ofString(
                        jackson.writeValueAsString(instanceData)))
                  .build();

            HttpResponse<String> response = httpClient.send(httpRequest,
                  HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() != 201 && response.statusCode() != 200) {
                LOGGER.error("Error sending data to {}. Response: {}", uri, response);
            } else {
                LOGGER.info("Information successfully sent to {}", uri);
            }
        } catch (InterruptedException | IOException e) {
            LOGGER.error("Sending data to {} failed, caused by: {}", uri, e.toString());
        }
    }

    @Command(name = "print data",
            hint = "prints data sent by telemetry cell",
            description = "The telemetry cell sends data to a collector. Use print data to print the data that " +
                    "are being sent. The unit for storage is byte.")
    public class PrintTelemetryCommand implements Callable<String> {
        @Override
        public String call() throws JsonProcessingException {
            ObjectMapper jackson = new ObjectMapper();
            return jackson.writerWithDefaultPrettyPrinter().writeValueAsString(instanceData);
        }
    }

}
