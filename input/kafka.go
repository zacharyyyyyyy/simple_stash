package input

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"simple_stash/config"
	"simple_stash/logger"
	"sync"
	"time"
)

type kafka struct {
	kafkaGroup []sarama.ConsumerGroup
	topic      []string
}
type consumerGroupHandler struct {
	consumeFunc func(data string)
}

const KafkaInputer = "kafka"

var KafkaHandler = &kafka{}

func init() {
	register(KafkaInputer, KafkaHandler)
}

func (kafka kafka) new(config config.ClientInput) Input {
	kafkaConf := config.KafkaConf
	KafkaHandler.topic = config.KafkaConf.Topic
	var wg sync.WaitGroup
	for i := 0; i < config.KafkaConf.Consumers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer, err := newClient(config.KafkaConf.Broker[i], kafkaConf.Group, kafkaConf.MaxWaitTime)
			if err != nil {
				logger.Runtime.Error(err.Error())
				return
			}
			KafkaHandler.kafkaGroup = append(KafkaHandler.kafkaGroup, consumer)
		}()
	}
	wg.Wait()
	return KafkaHandler
}
func (kafka kafka) run(ctx context.Context, consumeFunc func(data string)) error {
	var wg sync.WaitGroup
	for _, consumer := range KafkaHandler.kafkaGroup {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ConsumerGroup(consumer, KafkaHandler.topic, consumeFunc)
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

func ConsumerGroup(cg sarama.ConsumerGroup, topic []string, consumeFunc func(data string)) {
	var err error
	ctx, _ := context.WithCancel(context.Background())
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
	return sarama.NewConsumerGroup([]string{fmt.Sprintf("%s,%s", configBase.Host, configBase.Port)}, group, saramaConfig)

}
