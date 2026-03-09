package com.estudo.redis.service;

import com.estudo.redis.dto.PixDebitEvent;
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
    private static final long WAIT_TIMEOUT_MS = 20000;

    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisMessageListenerContainer listenerContainer;
    private final ObjectMapper objectMapper;

    private final Map<String, CompletableFuture<PixDebitEvent>> waiters = new ConcurrentHashMap<>();

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

    public PixDebitEvent subscribeAndWait(String id, long timeout) {

        CompletableFuture<PixDebitEvent> future = new CompletableFuture<>();
        waiters.put(id, future);

        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("thread interrompida aguardando resultado", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("falha aguardando resultado", e);
        } finally {
            waiters.remove(id);
        }
    }

    public Long publish(String message) {
        return redisTemplate.convertAndSend(RESULT_CHANNEL, message);
    }

    private void handleResultMessage(Message message, byte[] pattern) {

        String payload = new String(message.getBody(), StandardCharsets.UTF_8);

        PixDebitEvent event = fromPayload(payload);
        if (event == null) {
            return;
        }

        CompletableFuture<PixDebitEvent> future = waiters.remove(event.instantPaymentId());

        if (future != null) {
            future.complete(event);
        }
    }

    private PixDebitEvent fromPayload(String payload) {
        try {
            return objectMapper.readValue(payload, PixDebitEvent.class);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}