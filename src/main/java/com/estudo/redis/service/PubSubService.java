package com.estudo.redis.service;

import com.estudo.redis.dto.ResultEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;

@Service
public class PubSubService {

    public static final String RESULT_CHANNEL = "result";
    private static final long WAIT_TIMEOUT_MS = 800;

    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisMessageListenerContainer listenerContainer;
    private final ObjectMapper objectMapper;

    private final Map<String, CompletableFuture<String>> waiters = new ConcurrentHashMap<>();

    public PubSubService(
            RedisTemplate<String, Object> redisTemplate,
            RedisMessageListenerContainer listenerContainer,
            ObjectMapper objectMapper
    ) {
        this.redisTemplate = redisTemplate;
        this.listenerContainer = listenerContainer;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    public void startListener() {
        listenerContainer.addMessageListener(this::handleResultMessage, ChannelTopic.of(RESULT_CHANNEL));
    }

    public String subscribeAndWait(String id) {
        String normalizedId = normalizeId(id);

        CompletableFuture<String> future = new CompletableFuture<>();
        waiters.put(normalizedId, future);

        try {
            return future.get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("thread interrompida aguardando resultado", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("falha aguardando resultado", e);
        } finally {
            waiters.remove(normalizedId);
        }
    }

    public Long publish(String id, String message) {
        String normalizedId = normalizeId(id);

        ResultEvent event = new ResultEvent(normalizedId, message);
        String payload = toPayload(event);

        return redisTemplate.convertAndSend(RESULT_CHANNEL, payload);
    }

    private void handleResultMessage(Message message, byte[] pattern) {

        String payload = new String(message.getBody(), StandardCharsets.UTF_8);

        ResultEvent event = fromPayload(payload);
        if (event == null) {
            return;
        }

        CompletableFuture<String> future = waiters.remove(event.id());

        if (future != null) {
            future.complete(event.message());
        }
    }

    private String normalizeId(String id) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("id do resultado nao pode ser vazio");
        }
        return id.trim();
    }

    private String toPayload(ResultEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("falha ao serializar evento", e);
        }
    }

    private ResultEvent fromPayload(String payload) {
        try {
            return objectMapper.readValue(payload, ResultEvent.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}