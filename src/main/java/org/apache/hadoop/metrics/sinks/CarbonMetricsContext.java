package org.apache.hadoop.metrics.sinks;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.hadoop.metrics.tools.ExponentialBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


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
	private static final String ZEROS_AS_NULL_KEY = "zeros.as.null";
	private static final String BACKOFF_BASE_SLEEP_TIME = "base.sleep.time";
	private static final String RETRIES = "amqp.retries";


	private ConnectionFactory factory = new ConnectionFactory();
	private Connection connection;
	private Channel channel;
	private String routingKey;
	private String exchangeName;
	private String prefix;
	private String mainClass;
	private Boolean zerosAsNull;
	private ExponentialBackoff exponentialBackoff;
	private int retries;

	public CarbonMetricsContext() {
		super();
	}

	protected String getAttribute(String key, String def) {
		if (getAttribute(key) != null) {
			return getAttribute(key);
		} else {
			return def;
		}
	}

	//This is horrific, but we need to differentiate between processes when they
	//share the same hadoop-metrics.properties
	private String getMainClassName() {

		String mainClass = System.getProperty("sun.java.command");
		String[] mainClassSplit = mainClass.split("\\.");
		mainClassSplit = mainClassSplit[mainClassSplit.length - 1].split(" ");

		mainClass = mainClassSplit[0];

		return mainClass;
	}

	@Override
	public void init(String contextName, ContextFactory contextfactory) {
		super.init(contextName, contextfactory);

		mainClass = getMainClassName();

		int baseSleepTime = Integer.parseInt(getAttribute(BACKOFF_BASE_SLEEP_TIME, "500"));
		exponentialBackoff = new ExponentialBackoff(baseSleepTime);
		retries = Integer.parseInt(getAttribute(RETRIES, "15"));

		connectAmqp();

	}

	private void connectAmqp(){
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
			zerosAsNull = Boolean.parseBoolean(getAttribute(ZEROS_AS_NULL_KEY, "true"));

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

			logger.info("AMQP Connection to " + addr + " established");

		} catch (IOException io) {
			logger.warn("Could not establish AMQP connection, will retry", io);
		}
	}

	private void disconnectAmqp() {
		try {
			connection.close();
		} catch (Exception e) {
			logger.error("Could not close AMQP connection", e);
		}
	}

	@Override
	protected void emitRecord(String context, String record, OutputRecord outputRecord) throws IOException {

		logger.debug("context: " + context + " record: " + record);

		long timestamp = System.currentTimeMillis() / 1000;

		StringBuffer message = new StringBuffer();

		for (String metricName : outputRecord.getMetricNames()) {
			float value = outputRecord.getMetric(metricName).floatValue();

			if (value != 0 && zerosAsNull) {

				value = Math.round(value * 1000) / 1000;

				message.append(prefix + "." + mainClass + "." + context + "." + record + "." + metricName + " " + value + " " + timestamp + "\n");
				logger.debug(metricName + " value: " + outputRecord.getMetric(metricName).toString());
			}
		}

		for (int i = 0; i < retries; i ++) {
			try {
				logger.debug("Publishing metric " + message);
				channel.basicPublish(exchangeName, routingKey, null, message.toString().getBytes());
			} catch (Exception e) {
				long sleep = exponentialBackoff.getSleepTimeMs();
				logger.warn("Could not publish metric, attempting to restart AMQP connection, " +
						"backing off for " + sleep + "ms, retries remaining: "+(retries - i), e);
				disconnectAmqp();
				try {
					Thread.sleep(sleep);
				} catch (InterruptedException ie) {
					logger.error("Could not sleep thread", ie);
				}
				connectAmqp();
			}
		}

	}

	@Override
	public void close() {
	   disconnectAmqp();
	}
}
