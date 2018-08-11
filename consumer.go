package consumer

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

type Consumer interface {
	SetMaxWait(i int) error
	SetChannelCap(i int) error
	GetChannel() <-chan *dynamodbstreams.Record
}

type ddbstreamsConsumer struct {
	app, stream, table string
	shards             []string
	maxWait            int
	recordCh           chan *dynamodbstreams.Record

	ddbsvc      *ddbClient
	dstreamssvc *ddbstreamsClient
}

func New(appName, streamName, tableName string) (Consumer, error) {
	dsc := newDdbStreamsClient(streamName)

	ss, err := dsc.getShards()
	if err != nil {
		return nil, err
	}

	dc := newDdbClient(appName, streamName, tableName)

	go dc.runSave()

	return &ddbstreamsConsumer{
		app:         appName,
		stream:      streamName,
		table:       tableName,
		shards:      ss,
		maxWait:     20,
		recordCh:    make(chan *dynamodbstreams.Record),
		ddbsvc:      dc,
		dstreamssvc: dsc,
	}, nil
}

func (c *ddbstreamsConsumer) SetMaxWait(i int) error {
	if i < 0 {
		return ErrInvalidMaxWait
	}
	c.maxWait = i
	return nil
}

func (c *ddbstreamsConsumer) SetChannelCap(i int) error {
	if i < 0 {
		return ErrInvalidChannelCap
	}
	c.recordCh = make(chan *dynamodbstreams.Record, i)
	return nil
}

func (c *ddbstreamsConsumer) GetChannel() <-chan *dynamodbstreams.Record {
	for _, v := range c.shards {
		go c.readLoop(v)
	}
	return c.recordCh
}

func (c *ddbstreamsConsumer) readLoop(shardID string) {
	seqNum, err := c.ddbsvc.getSequenceNumber(shardID)
	if err != nil {
		// TODO:
		fmt.Println(err)
	}
	iterator, err := c.dstreamssvc.getShardIterator(shardID, seqNum)
	if err != nil {
		// TODO:
		fmt.Println(err)
	}

	sleepTime := 0
	for {
		resp, err := c.dstreamssvc.getRecords(iterator)
		if err != nil {
			// TODO:
			fmt.Println(err)
			seqNum, _ = c.ddbsvc.getSequenceNumber(shardID)
			iterator, _ = c.dstreamssvc.getShardIterator(shardID, seqNum)
			continue
		}
		for _, v := range resp.Records {
			c.recordCh <- v
			c.ddbsvc.setSequenceNumber(shardID, *v.Dynamodb.SequenceNumber)
		}

		if resp.NextShardIterator != nil {
			iterator = *resp.NextShardIterator
		}

		if len(resp.Records) > 0 {
			sleepTime = 0
		} else {
			if sleepTime == 0 {
				sleepTime = 1
			} else if sleepTime < c.maxWait {
				sleepTime = sleepTime * 2
			}
		}

		time.Sleep(time.Duration(sleepTime) * time.Second)
	}
}
