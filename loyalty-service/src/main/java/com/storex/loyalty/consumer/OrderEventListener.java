package com.storex.loyalty.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class OrderEventListener {
    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    @KafkaListener(topics = "storex-order-events", groupId = "loyalty-group")
    public void consumeOrderCreated(@Payload String message, 
                                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                    @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("[LOYALTY] Received order event from partition: {} with offset: {}. Adding loyalty reward points for: {}", 
                 partition, offset, message);
        try {
            // Giả lập logic xử lý tích điểm thành viên
            Thread.sleep(30); 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}