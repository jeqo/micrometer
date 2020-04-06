package io.micrometer.core.instrument.binder.kafka;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@Tag("docker")
public class KafkaStreamsMetricsIntegrationTest {
    @Container KafkaContainer kafkaContainer = new KafkaContainer();

    @Test void shouldReportStreamsMetrics() throws ExecutionException, InterruptedException {
        //Given
        // Topics created
        Properties adminConfig = new Properties();
        adminConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        AdminClient adminClient = KafkaAdminClient.create(adminConfig);
        Collection<NewTopic> newTopics = new ArrayList<>();
        newTopics.add(new NewTopic("input",  1, (short)1));
        newTopics.add(new NewTopic("output",  1, (short)1));
        adminClient.createTopics(newTopics).all().get();
        // Streams defined
        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream("input").peek((key, value) -> System.out.println(value)).to("output");
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        // And metrics binder defined
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        KafkaStreamsMetrics kafkaStreamsMetrics = new KafkaStreamsMetrics(streams);
        kafkaStreamsMetrics.bindTo(registry);
        //When
        streams.start();
        //Then
        // Meters are gathered
        List<Meter> meters = new ArrayList<>(registry.getMeters());
        assertThat(meters).hasSizeGreaterThan(0);
        assertThat(kafkaStreamsMetrics.reloaded).isTrue();
        //Given
        // Producer to test input
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfig, new ByteArraySerializer(), new ByteArraySerializer());
        // And consumer to test output
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(Collections.singletonList("output"));
        //When
        // Sending new input message
        producer.send(new ProducerRecord<>("input", "foo".getBytes()));
        producer.flush();
        // And wait for stream to process
        Thread.sleep(1000);
        // And validate for output to be produced
        ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(10000));
        assertThat(records).hasSize(1);
        //Then
        // Metrics are reloaded to gather new metrics
        kafkaStreamsMetrics.checkAndBindMetrics(registry);
        assertThat(kafkaStreamsMetrics.reloaded).isTrue();
        //When
        // Another message is sent
        producer.send(new ProducerRecord<>("input", "bar".getBytes()));
        producer.flush();
        // And wait for stream to process
        Thread.sleep(1000);
        // And validate for output to be produced
        records = consumer.poll(Duration.ofMillis(10000));
        assertThat(records).hasSize(1);
        //Then
        // Metrics are not reloaded any more
        kafkaStreamsMetrics.checkAndBindMetrics(registry);
        assertThat(kafkaStreamsMetrics.reloaded).isFalse();
        //Clean
        streams.close();
    }
}
