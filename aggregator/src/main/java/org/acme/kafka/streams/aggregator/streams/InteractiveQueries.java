package org.acme.kafka.streams.aggregator.streams;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.WeatherStationData;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.StoreQueryParameters;

import java.util.List;
import java.util.stream.Collectors;

import org.jboss.logging.Logger;
import org.wildfly.common.net.HostName;

@ApplicationScoped
public class InteractiveQueries {

    private static final int MAX_ATTEMPTS = 10;
    private static final long DELAY_BETWEEN_ATTEMPTS_MS = 1000;
    private static final Logger LOG = Logger.getLogger(InteractiveQueries.class);

    @Inject
    KafkaStreams streams;

    public List<PipelineMetadata> getMetaData() {
        return streams.allMetadataForStore(TopologyProducer.WEATHER_STATIONS_STORE)
                .stream()
                .map(m -> new PipelineMetadata(
                        m.hostInfo().host() + ":" + m.hostInfo().port(),
                        m.topicPartitions()
                                .stream()
                                .map(TopicPartition::toString)
                                .collect(Collectors.toSet())))
                .collect(Collectors.toList());
    }

    public GetWeatherStationDataResult getWeatherStationData(int id) {
        KeyQueryMetadata metadata = streams.queryMetadataForKey(
                TopologyProducer.WEATHER_STATIONS_STORE,
                id,
                Serdes.Integer().serializer());

        if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE) {
            LOG.warnv("Found no metadata for key {0}", id);
            return GetWeatherStationDataResult.notFound();
        } else if (metadata.activeHost().host().equals(HostName.getQualifiedHostName())) {
            LOG.infov("Found data for key {0} locally", id);
            Aggregation result = getWeatherStationStore().get(id);

            if (result != null) {
                return GetWeatherStationDataResult.found(WeatherStationData.from(result));
            } else {
                return GetWeatherStationDataResult.notFound();
            }
        } else {
            LOG.infov("Found data for key {0} on remote host {1}:{2}", id, metadata.activeHost().host(), metadata.activeHost().port());
            return GetWeatherStationDataResult.foundRemotely(metadata.activeHost().host(), metadata.activeHost().port());
        }
    }

    // ChatGPT Version
   /* private ReadOnlyKeyValueStore<Integer, Aggregation> getWeatherStationStore() {
        int attempts = 0;
        while (attempts < MAX_ATTEMPTS) {
            try {
                StoreQueryParameters<ReadOnlyKeyValueStore<Integer, Aggregation>> storeQueryParameters =
                        StoreQueryParameters.fromNameAndType(
                                TopologyProducer.WEATHER_STATIONS_STORE,
                                QueryableStoreTypes.keyValueStore()
                        );

                return streams.store(storeQueryParameters);
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
                attempts++;
                try {
                    Thread.sleep(DELAY_BETWEEN_ATTEMPTS_MS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new IllegalStateException("Unable to obtain the weather station store after multiple attempts.");
    } */

    private ReadOnlyKeyValueStore<Integer, Aggregation> getWeatherStationStore() {
        while (true) {
            try {
                return streams.store(StoreQueryParameters.fromNameAndType(TopologyProducer.WEATHER_STATIONS_STORE, QueryableStoreTypes.keyValueStore()));
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }
}
