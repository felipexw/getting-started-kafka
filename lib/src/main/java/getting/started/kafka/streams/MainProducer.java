package getting.started.kafka.streams;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import static java.lang.String.format;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MainProducer {

	public static void main(String[] args) {
		new MainProducer().publish();
	}

	void publish() {
		final var producer = new KafkaProducer<String, String>(properties());
		final var topic = "GETTING_STARTED_KAFKA";
		final var producerName = System.getenv("PRODUCER_NAME");
		try {
			while (true) {
				producer.send(new ProducerRecord<>(topic, producerName, String.valueOf(UUID.randomUUID())),
						(data, ex) -> {
							if (ex != null) {

								System.out.println("[" + producerName + "]: Erro ao publicar mensagem. Mensagem: "
										+ ex.getMessage());
							}

							final var messageContent = format(
									"[" + producerName + "]: Topic: %s, offset: %s, partition: %s", data.topic(),
									data.offset(), data.partition());

							System.out.println(messageContent);
						}).get();

				Thread.sleep(500);
			}
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Properties properties() {
		final var properties = new Properties();

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;
	}

}
