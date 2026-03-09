package com.estudo.redis.controller;

import java.util.Map;

import com.estudo.redis.dto.PixDebitEvent;
import com.estudo.redis.dto.PublishRequest;
import com.estudo.redis.service.PubSubService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/pubsub")
public class PubSubController {

    private final PubSubService pubSubService;

    public PubSubController(PubSubService pubSubService) {
        this.pubSubService = pubSubService;
    }

    @GetMapping("/sub/{id}")
    public ResponseEntity<Map<String, Object>> subscribe(@PathVariable String id) {
        PixDebitEvent message = pubSubService.subscribeAndWait(id, 20000);

        return ResponseEntity.ok(Map.of(
                "id", id,
                "resultChannel", PubSubService.RESULT_CHANNEL,
                "message", message.toString()
        ));
    }

    @PostMapping("/pub")
    public ResponseEntity<Map<String, Object>> publish(@RequestBody String request) {

        Long listeners = pubSubService.publish(request);
        return ResponseEntity.ok(Map.of(
                "resultChannel", PubSubService.RESULT_CHANNEL,
                "message", request,
                "receivers", listeners == null ? 0 : listeners
        ));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}
