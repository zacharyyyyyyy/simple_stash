package output

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	"log"
	"simple_stash/config"
	"simple_stash/logger"
)

type elasticSearch struct {
	client *elastic.Client
}

const esOutput = "es"

var ElasticHandler = &elasticSearch{}

func init() {
	register(esOutput, ElasticHandler)
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
	return ElasticHandler
}

func (es elasticSearch) write(ctx context.Context, data interface{}) {

}
