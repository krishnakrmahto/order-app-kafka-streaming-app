package com.course.kafka.broker.stream.web;

import com.course.kafka.broker.message.WebColorVoteMessage;
import com.course.kafka.broker.message.WebDesignVoteMessage;
import com.course.kafka.broker.message.WebLayoutVoteMessage;
import com.course.kafka.util.WebColorVoteTimestampExtractor;
import com.course.kafka.util.WebLayoutVoteTimestampExtractor;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class WebDesignJoinStreamThree
{
    @Bean
    public KStream<String, WebColorVoteMessage> kStreamWebDesign(StreamsBuilder streamsBuilder,
                                                                 WebColorVoteTimestampExtractor colorVoteTimestampExtractor,
                                                                 WebLayoutVoteTimestampExtractor layoutVoteTimestampExtractor)
    {
        JsonSerde<WebColorVoteMessage> colorVoteSerde = new JsonSerde<>(WebColorVoteMessage.class);
        JsonSerde<WebLayoutVoteMessage> layoutVoteSerde = new JsonSerde<>(WebLayoutVoteMessage.class);
        Serde<String> stringSerde = Serdes.String();

        KStream<String, WebColorVoteMessage> colorVoteSourceStream = streamsBuilder.stream("t.commodity.web.vote-color",
                                                                            Consumed.with(stringSerde,
                                                                                          colorVoteSerde,
                                                                                          colorVoteTimestampExtractor,
                                                                                          null));
        colorVoteSourceStream.mapValues(WebColorVoteMessage::getColor)
                             .to("t.commodity.web.vote-three-username-color");

        KTable<String, String> colorTable = streamsBuilder.table("t.commodity.web.vote-three-username-color",
                                                            Consumed.with(stringSerde, stringSerde));

        streamsBuilder.stream("t.commodity.web.vote-layout", Consumed.with(stringSerde,
                                                                           layoutVoteSerde,
                                                                           layoutVoteTimestampExtractor,
                                                                           null))
                      .mapValues(WebLayoutVoteMessage::getLayout)
                      .to("t.commodity.web.vote-three-username-layout");

        KTable<String, String> layoutTable = streamsBuilder.table("t.commodity.web.vote-three-username-layout",
                                                                     Consumed.with(stringSerde, stringSerde));


        // table-table join
        KTable<String, WebDesignVoteMessage> colorLayoutVoteJoin = colorTable.outerJoin(layoutTable, this::voteJoiner,
                                                                                   Materialized.with(stringSerde, new JsonSerde<>(WebDesignVoteMessage.class)));
        colorLayoutVoteJoin.toStream().to("t.commodity.web.vote-three-result");

        // print vote counts in console (we could put it in another stream as well)
        colorLayoutVoteJoin.groupBy((userName, votedDesign) -> KeyValue.pair(votedDesign.getColor(), votedDesign.getLayout()))
                           .count().toStream().print(Printed.toSysOut());

        return colorVoteSourceStream;
    }

    private WebDesignVoteMessage voteJoiner(String color, String layout)
    {
        // we might have to consider and put conditions for possible null values of color and layout (since
        // this is an outer join), but in our case this makes no difference.
        return new WebDesignVoteMessage(color, layout);
    }
}
