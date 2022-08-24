package output

import (
	"context"
	"fmt"
	"golang.org/x/sync/semaphore"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"
	"log"
	"simple_stash/config"
	"simple_stash/logger"
)

type elasticSearch struct {
	client       *elastic.Client
	bulkRequest  *elastic.BulkService
	dataSlice    []interface{}
	lock         sync.RWMutex
	bulkMaxCount int
	index        string
}

const EsOutputer = "es"

var (
	ElasticHandler        = &elasticSearch{}
	goroutineLimit  int64 = 100
	goroutineWeight int64 = 1
	sema                  = semaphore.NewWeighted(goroutineLimit)
)

func init() {
	register(EsOutputer, ElasticHandler)
}

func (es elasticSearch) new(config config.ClientOutput) Output {
	esConfig := config.EsConf
	errorLog := log.New(logger.Runtime, "app", log.LstdFlags)
	client, err := elastic.NewClient(
		elastic.SetErrorLog(errorLog),
		elastic.SetURL(fmt.Sprintf("%s:%s", esConfig.Host, esConfig.Port)),
		// 将sniff设置为false后，便不会自动转换地址
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
	for {
		select {
		case data := <-read():
			ElasticHandler.dataSlice = append(ElasticHandler.dataSlice, data)
			ElasticHandler.lock.RLock()
			if len(ElasticHandler.dataSlice) >= es.bulkMaxCount {
				bulkData := ElasticHandler.dataSlice[:es.bulkMaxCount]
				ElasticHandler.dataSlice = ElasticHandler.dataSlice[es.bulkMaxCount:]
				_ = sema.Acquire(context.Background(), goroutineWeight)
				go func(bulkData []interface{}) {
					es.bulkCreate(bulkData)
				}(bulkData)
			}
			ElasticHandler.lock.RUnlock()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (es elasticSearch) bulkCreate(bulkData []interface{}) {
	for _, data := range bulkData {
		req := elastic.NewBulkIndexRequest().Index(es.index).Type("_doc").Doc(data)
		es.bulkRequest.Add(req)
	}
	if len(bulkData) > 0 {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := es.bulkRequest.Do(ctx)
		if err != nil {
			logger.Runtime.Error(err.Error())
		}
	}
	sema.Release(goroutineWeight)
}
