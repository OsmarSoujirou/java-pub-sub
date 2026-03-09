package com.estudo.redis.dto;

import com.fasterxml.jackson.annotation.JsonProperty;


public record PixDebitEvent(

        @JsonProperty("instantPaymentId")
        String instantPaymentId

) {

}
