package consumer

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams/dynamodbstreamsiface"
)

type ddbstreamsClient struct {
	svc    dynamodbstreamsiface.DynamoDBStreamsAPI
	stream string
}

func newDdbStreamsClient(stream string) *ddbstreamsClient {
	return &ddbstreamsClient{
		svc:    dynamodbstreams.New(session.New(aws.NewConfig().WithRegion("ap-northeast-1"))),
		stream: stream,
	}
}

func (c *ddbstreamsClient) getShards() ([]string, error) {
	shards := make([]string, 0)

	input := &dynamodbstreams.DescribeStreamInput{
		StreamArn: aws.String(c.stream),
	}

	for {
		resp, err := c.svc.DescribeStream(input)
		if err != nil {
			return shards, err
		}

		for _, v := range resp.StreamDescription.Shards {
			shards = append(shards, *v.ShardId)
		}

		if resp.StreamDescription.LastEvaluatedShardId == nil {
			break
		}
		input.ExclusiveStartShardId = resp.StreamDescription.LastEvaluatedShardId
	}

	return shards, nil
}

func (c *ddbstreamsClient) getShardIterator(shardID, seqNum string) (string, error) {
	input := &dynamodbstreams.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAfterSequenceNumber),
		StreamArn:         aws.String(c.stream),
	}
	if seqNum == "" {
		input.ShardIteratorType = aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon)
	} else {
		input.SequenceNumber = aws.String(seqNum)
	}

	resp, err := c.svc.GetShardIterator(input)
	if err != nil {
		return "", err
	}
	return *resp.ShardIterator, nil
}

func (c *ddbstreamsClient) getRecords(iterator string) (*dynamodbstreams.GetRecordsOutput, error) {
	input := &dynamodbstreams.GetRecordsInput{
		ShardIterator: aws.String(iterator),
	}
	return c.svc.GetRecords(input)
}
