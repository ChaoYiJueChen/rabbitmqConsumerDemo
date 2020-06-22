package com.everjiankang.dependency.springboot;

import java.util.Map;

import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.everjiankang.dependency.model.Order;
import com.rabbitmq.client.Channel;

@Component
public class RabbitReceiver {
    
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(
                        name = "queue-1",
                        durable = "true"
                    ),
            exchange = @Exchange(
                        name = "exchange-1",
                        durable = "true",
                        type = "topic",
                        ignoreDeclarationExceptions = "true"
                    ),
            key = "springboot.*"
            )
    )
    @RabbitHandler
    public void onMessage(Message message, Channel  channel) throws Exception {
        System.err.println("------------------------------------");
        System.err.println("消费端Payload:" + message.getPayload());
        Long deliverytag = (Long)message.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        //手工ACK
        channel.basicAck(deliverytag, false);
    }
    
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(
                        name = "${spring.rabbitmq.listener.order.queue.name}",
                        durable = "${spring.rabbitmq.listener.order.queue.durable}"
                    ),
            exchange = @Exchange(
                        name = "${spring.rabbitmq.listener.order.exchange.name}",
                        durable = "${spring.rabbitmq.listener.order.exchange.durable}",
                        type = "${spring.rabbitmq.listener.order.exchange.type}",
                        ignoreDeclarationExceptions = "${spring.rabbitmq.listener.order.exchange.ignoreDeclarationExceptions}"
                    ),
            key = "${spring.rabbitmq.listener.order.key}"
            )
    )

    @RabbitHandler
    //将Message用@Payload和@Headers拆分
    //@Payload表示body里面的信息，@Headers表示properties的信息
    public void onOrderMessage(@Payload Order order, Channel  channel,
            @Headers Map<String, Object> headers) throws Exception {
        System.err.println("------------------------------------");
        System.err.println("消费端Order:" + order.getId());
        Long deliverytag = (Long)headers.get(AmqpHeaders.DELIVERY_TAG);
        //手工ACK
        channel.basicAck(deliverytag, false);
    }
}