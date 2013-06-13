package org.apache.hadoop.metrics.sinks;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics2.MetricsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Enumeration;
import java.util.Properties;


/**
 * Created with IntelliJ IDEA.
 * User: chris
 * Date: 13/06/2013
 * Time: 15:13
 * To change this template use File | Settings | File Templates.
 */
public class CarbonMetricsContext extends AbstractMetricsContext {


    private static final Logger logger = LoggerFactory.getLogger(CarbonMetricsContext.class);


    private static final String HOSTNAME_KEY = "amqp.host";
    private static final String PORT_KEY = "amqp.port";
    private static final String EXCHANGE_NAME_KEY = "amqp.exchange.name";
    private static final String EXCHANGE_DURABLE_KEY = "amqp.exchange.durable";
    private static final String USERNAME_KEY = "amqp.username";
    private static final String PASSWORD_KEY = "amqp.password";
    private static final String ROUTING_KEY = "amqp.routing.key";
    private static final String VHOST_KEY = "amqp.vhost";
    private static final String PREFIX_KEY = "prefix";



    private ConnectionFactory factory = new ConnectionFactory();
    private Connection connection;
    private Channel channel;
    private String routingKey;
    private String exchangeName;
    private String prefix;
    private String mainClass;

    public CarbonMetricsContext() {
        super();
    }

    protected String getAttribute(String key, String def) {
        if(getAttribute(key) != null) {
            return getAttribute(key);
        } else {
            return def;
        }
    }
    //This is horrific, but we need to differentiate between processes when they
    //share the same hadoop-metrics.properties
    private String getMainClassName() {
        StackTraceElement[] stack = Thread.currentThread ().getStackTrace ();
        StackTraceElement main = stack[stack.length - 1];
        String mainClass = main.getClassName ();
        String[] mainClassSplit = mainClass.split("\\.");

        mainClass = mainClassSplit[mainClassSplit.length - 1];

        return mainClass;
    }

    @Override
    public void init(String contextName, ContextFactory contextfactory) {
        super.init(contextName, contextfactory);

        mainClass = getMainClassName();

        try {
            String addr = getAttribute(HOSTNAME_KEY);

            int port = Integer.parseInt(getAttribute(PORT_KEY, "5672"));
            String username = getAttribute(USERNAME_KEY, null);
            String password = getAttribute(PASSWORD_KEY, null);
            exchangeName = getAttribute(EXCHANGE_NAME_KEY, "metrics");
            boolean exchangeDurable = Boolean.parseBoolean(getAttribute(EXCHANGE_DURABLE_KEY, "true"));
            routingKey = getAttribute(ROUTING_KEY, "#");
            String vhost = getAttribute(VHOST_KEY, "/");
            prefix = getAttribute(PREFIX_KEY, "hadoop");

            factory.setHost(addr);
            factory.setPort(port);
            factory.setVirtualHost(vhost);
            if (username != null) {
                factory.setUsername(username);
                if (username != null) {
                    factory.setPassword(password);
                }
            }
            connection = factory.newConnection();
            channel = connection.createChannel();

            channel.exchangeDeclare(exchangeName, "topic", exchangeDurable);

            logger.info("AMQP Connection to "+addr+" established");

        } catch (IOException io) {
            throw new MetricsException("Failed to start sink", io);
        }
    }

    @Override
    protected void emitRecord(String context, String record, OutputRecord outputRecord) throws IOException {

        logger.debug("context: " + context + " record: "+ record);

        long timestamp = System.currentTimeMillis() / 1000;

        StringBuffer message = new StringBuffer();


        for (String metricName : outputRecord.getMetricNames()) {
            float value = outputRecord.getMetric(metricName).floatValue();
            value = Math.round(value * 1000) / 1000;

            message.append(prefix + "." +mainClass+ "." + context + "." + record + "." + metricName + " " + value + " " + timestamp + "\n");
            logger.debug(metricName + " value: " + outputRecord.getMetric(metricName).toString());
        }

        logger.debug("Publishing metric " + message);
        channel.basicPublish(exchangeName, routingKey, null, message.toString().getBytes());

    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (IOException e) {
             logger.error("Could not close AMQP connection",e);
        }
    }
}
