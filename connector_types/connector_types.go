package connector_types

type PublishMessage struct {
	MsgHeader *map[string]interface{}
	MsgBody   *[]byte
	MessageId string
	MsgUserId string
}

type Consumer struct {
	Exchange         string
	RoutingKey       string
	QueName          string
	MessageId        string
	MsgUserId        string
	QueueDeclareType string
	ShowLog          bool
}

type Publisher struct {
	ExchangeType     string
	Exchange         string
	RoutingKey       string
	QueName          string
	MessageId        string
	MsgUserId        string
	QueueDeclareType string
	ShowLog          bool
}
