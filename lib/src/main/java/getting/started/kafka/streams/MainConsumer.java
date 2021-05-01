package getting.started.kafka.streams;

import static java.lang.String.format;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class MainConsumer {
	private static final String CONSUMER_NAME = System.getenv("CONSUMER_NAME");

	public static void main(String[] args) {
		new MainConsumer().subscribe();
	}

	void subscribe() {
		final var consumer = new KafkaConsumer<>(properties());
		final var topic = "GETTING_STARTED_KAFKA";
		consumer.subscribe(List.of(topic));

		try {
			while (true) {
				final var records = consumer.poll(Duration.ofMillis(300));
				records.forEach(record -> {
					final var messageContent = format("[%S]Topic: %s, offset: %s, partition: %s, key: %s, value: %s",
							CONSUMER_NAME, record.topic(), record.offset(), record.partition(), record.key(),
							record.value());

					System.out.println(messageContent);
				});

				Thread.sleep(4500);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Properties properties() {
		final var properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, getClass().getSimpleName());
		properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_NAME);
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");

		return properties;
	}

}
