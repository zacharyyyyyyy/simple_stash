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
	var wg sync.WaitGroup
	for i := 0; i < config.KafkaConf.Consumers; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			consumer, err := newClient(config.KafkaConf.Broker[k], kafkaConf.Group, kafkaConf.MaxWaitTime)
			if err != nil {
				logger.Runtime.Error(err.Error())
				return
			}

			kafkaHandler.kafkaGroup = append(kafkaHandler.kafkaGroup, consumer)
		}(i)
	}
	wg.Wait()
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
// Setup 执行在 获得新 session 后 的第一步, 在 ConsumeClaim() 之前
func (consumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error { return nil }

// Cleanup 执行在 session 结束前, 当所有 ConsumeClaim goroutines 都退出时
func (consumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

// ConsumeClaim 具体的消费逻辑
func (h consumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		//消费信息
		h.consumeFunc(string(msg.Value))
		// 标记消息已被消费 内部会更新 consumer offset
		sess.MarkMessage(msg, "")
	}
	return nil
}

func consumerGroup(ctx context.Context, cg sarama.ConsumerGroup, topic []string, consumeFunc func(data interface{})) {
	var err error
	defer cg.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
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
	}()
	wg.Wait()
}

func newClient(configBase config.KafKaBroker, group string, maxWaitTime int) (sarama.ConsumerGroup, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Net.SASL.Password = configBase.Pwd
	saramaConfig.Net.SASL.User = configBase.User
	saramaConfig.Consumer.MaxWaitTime = time.Duration(maxWaitTime) * time.Second
	return sarama.NewConsumerGroup([]string{fmt.Sprintf("%s:%s", configBase.Host, configBase.Port)}, group, saramaConfig)

}
