package input

import (
	"context"
	"fmt"
	"simple_stash/config"
	"simple_stash/logger"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type kafka struct {
	kafkaGroup []sarama.ConsumerGroup
	topic      []string
}
type consumerGroupHandler struct {
	consumeFunc func(data interface{})
}

const KafkaInputer = "kafka"

var kafkaHandler = &kafka{}

func init() {
	register(KafkaInputer, kafkaHandler)
}

func (kafka kafka) new(config config.ClientInput) Input {
	kafkaConf := config.KafkaConf
	kafkaHandler.topic = config.KafkaConf.Topic
	for i := 0; i < config.KafkaConf.Consumers; i++ {
		consumer, err := newClient(config.KafkaConf.Broker[i], kafkaConf.Group, kafkaConf.MaxWaitTime)
		if err != nil {
			logger.Runtime.Error(err.Error())
		}
		kafkaHandler.kafkaGroup = append(kafkaHandler.kafkaGroup, consumer)
	}
	return kafkaHandler
}
func (kafka kafka) run(ctx context.Context, consumeFunc func(data interface{})) error {
	var wg sync.WaitGroup
	for _, consumer := range kafkaHandler.kafkaGroup {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumerGroup(ctx, consumer, kafkaHandler.topic, consumeFunc)
		}()
	}
	wg.Wait()
	return nil
}

//kafka组消费
func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		h.consumeFunc(string(msg.Value))
		sess.MarkMessage(msg, "")
	}
	return nil
}

func consumerGroup(ctx context.Context, cg sarama.ConsumerGroup, topic []string, consumeFunc func(data interface{})) {
	var err error
	defer cg.Close()
	handler := consumerGroupHandler{consumeFunc: consumeFunc}
	for {
		err = cg.Consume(ctx, topic, handler)
		if err != nil {
			logger.Runtime.Error(err.Error())
		}
		if ctx.Err() != nil {
			logger.Runtime.Info("kafka consumer close!")
			return
		}
	}
}

func newClient(configBase config.KafKaBroker, group string, maxWaitTime int) (sarama.ConsumerGroup, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Net.SASL.Password = configBase.Pwd
	saramaConfig.Net.SASL.User = configBase.User
	saramaConfig.Consumer.MaxWaitTime = time.Duration(maxWaitTime) * time.Second
	return sarama.NewConsumerGroup([]string{fmt.Sprintf("%s:%s", configBase.Host, configBase.Port)}, group, saramaConfig)

}
