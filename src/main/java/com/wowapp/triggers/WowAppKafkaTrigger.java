package com.wowapp.triggers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WowAppKafkaTrigger implements ITrigger {

    private static final Logger LOGGER = LoggerFactory.getLogger("[WOWAPP_TRIGGER]");

    private ExecutorService executorService = Executors.newFixedThreadPool(1, r -> new Thread(r, "KafkaThread"));
    private Properties properties;

    private String topic;
    private Producer<String, String> producer;

    private ObjectMapper MAPPER = new ObjectMapper();

    public WowAppKafkaTrigger() throws IOException {
        String configPath = Optional.ofNullable(System.getenv("WOWAPP_TRIGGERS_CONFIG_PATH")).orElse("/etc/triggers/triggers.conf");

        try (final FileInputStream in = new FileInputStream(new File(configPath))) {
            properties = new Properties();
            properties.load(in);

            producer = createProducer(properties);

            topic = properties.getProperty("kafka.topic");
        }
    }

    protected Producer<String, String> createProducer(Properties prop) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", props.getProperty(prop.getProperty("kafka.brokers")));
        props.put("key.serializer", StringSerializer.class.getCanonicalName());
        props.put("value.serializer", StringSerializer.class.getCanonicalName());
        props.put("retries", "3");
        return new KafkaProducer<>(properties);
    }

    @Override
    public Collection<Mutation> augment(Partition partition) {
        executorService.submit(() -> sendChange(partition));
        return Collections.emptyList();
    }

    protected boolean partitionIsDeleted(Partition partition) {
        return partition.partitionLevelDeletion().markedForDeleteAt() > Long.MIN_VALUE;
    }

    protected boolean rowIsDeleted(Row row) {
        return row.deletion().time().markedForDeleteAt() > Long.MIN_VALUE;
    }

    protected String getKey(Partition partition) {
        return partition.metadata().getKeyValidator().getString(partition.partitionKey().getKey());
    }

    public <K, V> Future<RecordMetadata> send(String key, String value) {
        return producer.send(new ProducerRecord<>(topic, key, value), (RecordMetadata metadata, Exception exception) -> {
            Optional.ofNullable(exception).ifPresent(e -> LOGGER.error("Failed to send to topic: {}, value: {}", topic, value, e));
        });
    }

    private void sendChange(Partition partition) {
        try {

            final String key = getKey(partition);
            final Map<String, Object> triggerObject = new HashMap<>();

            triggerObject.put("key", key);

            if (partitionIsDeleted(partition)) {
                triggerObject.put("partitionDeleted", true);
            } else {
                handleModification(partition, triggerObject);
            }

            LOGGER.info("Value: {}", MAPPER.writeValueAsString(triggerObject));

        } catch (Exception e) {
            LOGGER.error("Failed to serialize row.", e);
        }
    }

    private void handleModification(Partition partition, Map<String, Object> triggerObject) {
        final UnfilteredRowIterator it = partition.unfilteredIterator();
        while (it.hasNext()) {

            final Unfiltered un = it.next();
            if (un.isRow()) {
                handleRow(un, partition, triggerObject);
            } else if (un.isRangeTombstoneMarker()) {

                triggerObject.put("rowRangeDeleted", true);
                ClusteringBound bound = (ClusteringBound) un.clustering();
                final List<Map<String, Object>> bounds = new ArrayList<>();

                for (int i = 0; i < bound.size(); i++) {
                    String clusteringBound = partition.metadata().comparator.subtype(i).getString(bound.get(i));
                    final Map<String, Object> boundObject = new HashMap<>();
                    boundObject.put("clusteringKey", clusteringBound);
                    if (i == bound.size() - 1) {
                        if (bound.kind().isStart()) {
                            boundObject.put("inclusive", bound.kind() == ClusteringPrefix.Kind.INCL_START_BOUND ? true : false);
                        }
                        if (bound.kind().isEnd()) {
                            boundObject.put("inclusive", bound.kind() == ClusteringPrefix.Kind.INCL_END_BOUND ? true : false);
                        }
                    }
                    bounds.add(boundObject);
                }

                triggerObject.put((bound.kind().isStart() ? "start" : "end"), bounds);

            }
        }
    }

    private void handleRow(Unfiltered un, Partition partition, Map<String, Object> triggerObject) {

        final Map<String, Object> mapRow = new HashMap<>();
        final Clustering clustering = (Clustering) un.clustering();
        final String clusterKey = clustering.toCQLString(partition.metadata());

        mapRow.put("clusterKey", clusterKey);
        final Row row = partition.getRow(clustering);

        if (rowIsDeleted(row)) {
            mapRow.put("rowDeleted", true);
        } else {

            final List<Object> colls = new ArrayList<>();
            for (final Cell cell : row.cells()) {
                final Map<String, Object> cellMap = new HashMap<>();
                cellMap.put("name", cell.column().name.toString());
                if (cell.isTombstone()) {
                    cellMap.put("deleted", true);
                } else {
                    String data = cell.column().type.getString(cell.value());
                    cellMap.put("value", data);
                }

                colls.add(cellMap);
            }

            mapRow.put("row", colls);
        }

        triggerObject.put(clusterKey, mapRow);
    }

}
