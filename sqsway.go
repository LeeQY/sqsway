package sqsway

import (
	"fmt"
	"log"
	"sync"

	"gopkg.in/fatih/set.v0"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

type ReceiveHandle func(*sync.WaitGroup, []byte, *set.Set, int)
type QueueInfo struct {
	Queue *string
}

var (
	thisSQS   *sqs.SQS
	waitTime  = int64(20)
	binary    = "Binary"
	body      = "1"
	valueName = "value"
	maxMsg    = int64(5)
	region    = "cn-north-1"
)

func init() {
	thisSQS = sqs.New(&aws.Config{Region: &region})
}

func New(q *string) *QueueInfo {
	return &QueueInfo{
		Queue: q,
	}
}

func (q *QueueInfo) SendMessage(msgString []byte) {
	var mav sqs.MessageAttributeValue
	mav.BinaryValue = msgString
	mav.DataType = &binary

	var msg sqs.SendMessageInput
	msg.MessageAttributes = make(map[string]*sqs.MessageAttributeValue)
	msg.MessageAttributes[valueName] = &mav
	msg.QueueUrl = q.Queue
	msg.MessageBody = &body
	_, err := thisSQS.SendMessage(&msg)
	if err != nil {
		fmt.Println(err)
	}
}

func (q *QueueInfo) ReceiveMessage(h ReceiveHandle) {
	var input sqs.ReceiveMessageInput
	input.QueueUrl = q.Queue
	input.WaitTimeSeconds = &waitTime
	input.MessageAttributeNames = []*string{&valueName}
	input.MaxNumberOfMessages = &maxMsg

	ro, _ := thisSQS.ReceiveMessage(&input)
	l := len(ro.Messages)
	if l > 0 {
		needDele := set.New()
		wg := new(sync.WaitGroup)
		for i := 0; i < l; i++ {
			msg := ro.Messages[i]
			atts := msg.MessageAttributes
			wg.Add(1)
			go h(wg, atts["value"].BinaryValue, needDele, i)
		}
		wg.Wait()

		indexes := set.IntSlice(needDele)
		var entries []*sqs.DeleteMessageBatchRequestEntry
		for _, index := range indexes {
			msg := ro.Messages[index]

			var entry sqs.DeleteMessageBatchRequestEntry
			entry.Id = msg.MessageId
			entry.ReceiptHandle = msg.ReceiptHandle
			entries = append(entries, &entry)
		}

		q.deleteMessage(&entries)
	}
	q.ReceiveMessage(h)
}

//Delete the messages.
func (q *QueueInfo) deleteMessage(entries *[]*sqs.DeleteMessageBatchRequestEntry) {
	var delMessage sqs.DeleteMessageBatchInput
	delMessage.QueueUrl = q.Queue
	delMessage.Entries = *entries

	_, err := thisSQS.DeleteMessageBatch(&delMessage)
	if err != nil {
		log.Println(err)
	}
}
