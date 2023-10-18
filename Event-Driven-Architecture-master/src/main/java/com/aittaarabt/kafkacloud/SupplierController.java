package com.aittaarabt.kafkacloud;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka-supplier")
public class SupplierController {

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    public SupplierController(KafkaTemplate<Integer, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping("/publish/{message}")
    public String sendMessageToKafkaTopic(@PathVariable String message) {
        kafkaTemplate.send("supplierTopic", 1, message);
        return "Message envoyé avec succès au service fournisseur Kafka !";
    }
}
