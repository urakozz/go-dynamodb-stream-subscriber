
// Yury Kozyrev (urakozz)
// MIT License
package stream

import (
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"time"
)

type StreamSubscriber struct {
	dynamoSvc *dynamodb.DynamoDB
	streamSvc *dynamodbstreams.DynamoDBStreams
	table     string
}

func NewStreamSubscriber(
	dynamoSvc *dynamodb.DynamoDB,
	streamSvc *dynamodbstreams.DynamoDBStreams,
	table string) *StreamSubscriber {
	return &StreamSubscriber{dynamoSvc, streamSvc, table}
}

func (r *StreamSubscriber) GetStreamData() (<-chan *dynamodbstreams.Record, <-chan error) {

	ch := make(chan *dynamodbstreams.Record, 1)
	errCh := make(chan error, 1)

	go func(ch chan<- *dynamodbstreams.Record, errCh chan<- error) {
		var shardId *string
		var streamArn *string
		var err error

		for {
			shardId, streamArn, err = r.findProperShardId(shardId)
			if err != nil {
				errCh <- err
			}
			if shardId != nil {
				r.processShard(shardId, streamArn, ch)
				if err != nil {
					errCh <- err
				}
			}

			fmt.Println("sleep before next shard")
			time.Sleep(time.Second * 5)

		}
	}(ch, errCh)

	return ch, errCh
}

func (r *StreamSubscriber) getLatestStreamArn() (*string, error) {
	tableInfo, err := r.dynamoSvc.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(r.table)})
	if err != nil {
		return nil, err
	}
	if nil == tableInfo.Table.LatestStreamArn {
		return nil, errors.New("empty table stream arn")
	}
	return tableInfo.Table.LatestStreamArn, nil
}

func (r *StreamSubscriber) findProperShardId(previousShardId *string) (shadrId *string, streamArn *string, err error) {
	streamArn, err = r.getLatestStreamArn()
	if err != nil {
		fmt.Println("Error: get latest stream: ", err)
		return nil, nil, err
	}
	fmt.Printf("Stream arn: %s\n", *streamArn)
	des, err := r.streamSvc.DescribeStream(&dynamodbstreams.DescribeStreamInput{
		StreamArn: streamArn,
	})
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}

	//fmt.Println(des.StreamDescription.Shards)

	if 0 == len(des.StreamDescription.Shards) {
		fmt.Println("no shards")
		return nil, nil, nil
	}

	if previousShardId == nil {
		fmt.Println("nil previous, return first")
		shadrId = des.StreamDescription.Shards[0].ShardId
		return
	}

	for i, shard := range des.StreamDescription.Shards {
		shadrId = shard.ShardId
		if shard.ParentShardId != nil && *shard.ParentShardId == *previousShardId {
			fmt.Println("have found shard ", i)
			return
		}
	}

	fmt.Println("nothing found (previousShard == last active), return last")

	return

}

func (r *StreamSubscriber) processShard(shardId, lastStreamArn *string, ch chan<- *dynamodbstreams.Record) error {
	fmt.Printf("Shard: %+v\n", *shardId)
	iter, err := r.streamSvc.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
		StreamArn:         lastStreamArn,
		ShardId:           shardId,
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon), // ->Lastest
	})
	if err != nil {
		fmt.Printf("Shard error: %s\n", err)
		return err
	}
	if iter.ShardIterator == nil {
		fmt.Println("nil shard iterator, continue")
		return nil
	}

	nextIterator := iter.ShardIterator

	for nextIterator != nil {
		fmt.Printf("Iterator id: %+v\n", *nextIterator)
		recs, err := r.streamSvc.GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: nextIterator,
			Limit:         aws.Int64(100),
		})
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "TrimmedDataAccessException" {
			//Trying to request data older than 24h, that's ok
			//http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/API_GetShardIterator.html -> Errors
			return nil
		}
		if err != nil {
			//
			fmt.Println(err)
			return err
		}

		//		fmt.Printf("Records %+v", recs)
		fmt.Printf("Records len: %d, %s\n", len(recs.Records), err)

		for i, record := range recs.Records {
			fmt.Printf("Id: %d => %s [%s]\n", i, *record.EventID, *record.Dynamodb.SequenceNumber)
			ch <- record
		}

		nextIterator = recs.NextShardIterator

		if nextIterator == nil {
			fmt.Println("Nil next itarator, shard is closed")
		}
		if len(recs.Records) == 0 && nextIterator != nil {
			fmt.Println("Empty set, but shard is not closed -> sleep a little")
			time.Sleep(time.Second * 5)
		}

		time.Sleep(time.Second)
	}
	return nil
}

