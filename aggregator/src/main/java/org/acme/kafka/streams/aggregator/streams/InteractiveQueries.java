package org.acme.kafka.streams.aggregator.streams;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.acme.kafka.streams.aggregator.model.Aggregation;
import org.acme.kafka.streams.aggregator.model.WeatherStationData;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.StoreQueryParameters;

@ApplicationScoped
public class InteractiveQueries {

    private static final int MAX_ATTEMPTS = 10;
    private static final long DELAY_BETWEEN_ATTEMPTS_MS = 1000;

    @Inject
    KafkaStreams streams;

    public GetWeatherStationDataResult getWeatherStationData(int id) {
        Aggregation result = getWeatherStationStore().get(id);

        if (result != null) {
            return GetWeatherStationDataResult.found(WeatherStationData.from(result));
        } else {
            return GetWeatherStationDataResult.notFound();
        }
    }

    private ReadOnlyKeyValueStore<Integer, Aggregation> getWeatherStationStore() {
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
    }
}
