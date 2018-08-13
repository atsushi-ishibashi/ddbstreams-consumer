# ddbstreams-consumer
[![GoDoc][1]][2]

[1]: https://godoc.org/github.com/atsushi-ishibashi/ddbstreams-consumer?status.svg
[2]: https://godoc.org/github.com/atsushi-ishibashi/ddbstreams-consumer

## Usage
```
import (
	consumer "github.com/atsushi-ishibashi/ddbstreams-consumer"
)

func main() {
	c, err := consumer.New("<AppName>", "<DynamoDBStreams ARN>", "<DynamoDB TableName>")
	if err != nil {
		log.Fatalln(err)
	}

	queue := c.GetChannel()
	for v := range queue {
		// do something
	}
}
```
