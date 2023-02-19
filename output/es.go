package output

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/semaphore"
	"log"
	"simple_stash/config"
	"simple_stash/logger"
	"time"

	"github.com/olivere/elastic/v7"
)

type elasticSearch struct {
	client       *elastic.Client
	bulkRequest  *elastic.BulkService
	dataSlice    []interface{}
	bulkMaxCount int
	index        string
}

const EsOutputer = "es"

var (
	ElasticHandler              = &elasticSearch{}
	goroutineLimit        int64 = 100
	goroutineWeight       int64 = 1
	sema                        = semaphore.NewWeighted(goroutineLimit)
	goroutineNotEnoughErr       = errors.New("es goroutine not enough")
)

func init() {
	register(EsOutputer, ElasticHandler)
}

func (es elasticSearch) new(config config.ClientOutput) Output {
	esConfig := config.EsConf
	errorLog := log.New(logger.Runtime, "", log.LstdFlags)
	client, err := elastic.NewClient(
		elastic.SetErrorLog(errorLog),
		elastic.SetURL(fmt.Sprintf("%s:%s", esConfig.Host, esConfig.Port)),
		elastic.SetSniff(false),
		elastic.SetBasicAuth(esConfig.Username, esConfig.Password), // 账号密码
	)
	if err != nil {
		panic(err)
	}
	ElasticHandler.client = client
	ElasticHandler.dataSlice = make([]interface{}, 0, 20)
	ElasticHandler.bulkRequest = client.Bulk()
	ElasticHandler.bulkMaxCount = config.EsConf.BulkMaxCount
	ElasticHandler.index = config.EsConf.Index
	return ElasticHandler
}

func (es elasticSearch) run(ctx context.Context) error {
	defer es.client.Stop()
	for {
		select {
		case data := <-read():
			ElasticHandler.dataSlice = append(ElasticHandler.dataSlice, data)
			if ctx.Err() != nil {
				es.bulkCreate(ElasticHandler.dataSlice)
				logger.Runtime.Info("elasticsearch client close!")
				return nil
			}
			if len(ElasticHandler.dataSlice) >= es.bulkMaxCount {
				if ok := sema.TryAcquire(goroutineWeight); ok != true {
					logger.Runtime.Error(goroutineNotEnoughErr.Error())
					continue
				}
				bulkData := ElasticHandler.dataSlice[:es.bulkMaxCount]
				ElasticHandler.dataSlice = ElasticHandler.dataSlice[es.bulkMaxCount:]
				go func(bulkData []interface{}) {
					defer sema.Release(goroutineWeight)
					es.bulkCreate(bulkData)
				}(bulkData)
			}
		case <-ctx.Done():
			//清空剩余data
			es.bulkCreate(ElasticHandler.dataSlice)
			logger.Runtime.Info("elasticsearch client close!")
			return nil
		}
	}
}

func (es elasticSearch) bulkCreate(bulkData []interface{}) {
	if len(bulkData) > 0 {
		for _, data := range bulkData {
			req := elastic.NewBulkIndexRequest().Index(es.index).Type("_doc").Doc(data)
			es.bulkRequest.Add(req)
		}
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := es.bulkRequest.Do(ctx)
		if err != nil {
			logger.Runtime.Error(err.Error())
		}
	}
}
