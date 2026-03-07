package com.estudo.redis.controller;

import java.util.Map;

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
        String message = pubSubService.subscribeAndWait(id);

        return ResponseEntity.ok(Map.of(
                "id", id,
                "resultChannel", PubSubService.RESULT_CHANNEL,
                "message", message
        ));
    }

    @PostMapping("/pub/{id}")
    public ResponseEntity<Map<String, Object>> publish(@PathVariable String id, @RequestBody PublishRequest request) {
        if (request == null || request.message() == null || request.message().isBlank()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "campo 'message' e obrigatorio"));
        }

        Long listeners = pubSubService.publish(id, request.message());
        return ResponseEntity.ok(Map.of(
                "id", id,
                "resultChannel", PubSubService.RESULT_CHANNEL,
                "message", request.message(),
                "receivers", listeners == null ? 0 : listeners
        ));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}
