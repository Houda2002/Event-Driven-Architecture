package com.aittaarabt.kafkacloud;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration

public class KafkaStreamsProcessor {

    @Bean
    public KStream<String, Long> process(StreamsBuilder builder) {
        KStream<String, String> source = builder.stream("supplierTopic");
        KTable<String, Long> counts = source
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word)
                .count();

        counts.toStream().to("analyticsTopic", Produced.with(Serdes.String(), Serdes.Long()));

        return counts.toStream();
    }
}
