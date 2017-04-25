# go-dynamodb-stream-subscriber
Go channel for streaming Dynamodb Updates

```go

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/urakozz/go-dynamodb-stream-subscriber/stream"
)
func main(){
  cfg := aws.NewConfig().WithRegion("eu-west-1")
  sess := session.New()
  streamSvc := dynamodbstreams.New(sess, cfg)
  dynamoSvc := dynamodb.New(sess, cfg)
  table := "tableName"
  
  streamSubscriber := stream.NewStreamSubscriber(dynamoSvc, streamSvc, table)
  ch , errCh :=  streamProvider.GetStreamDataAsync()
  
  go func(ch <- chan *dynamodbstreams.Record){
  	for record := range ch {
  		fmt.Println("from channel:", record)
  	}
  }(ch)
  go func(errCh <- chan error){
  	for err := range errCh {
  		fmt.Println("Stream Subscriber error: ", err)
  	}
  }(errCh)
}

```
