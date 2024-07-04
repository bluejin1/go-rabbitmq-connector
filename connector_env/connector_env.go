package connector_env

const (
	MQ_TYPE       string = ""
	MQ_HOST       string = ""
	MQ_PORT       string = "5672"
	MQ_USER       string = ""
	MQ_PW         string = ""
	MQ_QUEUE_TYPE string = ""

	// MQ_PREFIX_USE : preFix 사용 여부
	MQ_PREFIX_USE    string = "false"
	MQ_PREFIX_SEP    string = "_"
	MQ_PREFIX_PREFIX string = ""

	MQ_CLUSTERS      string = ""
	MQ_TIMEOUT       string = "180"
	MQ_POOL_SIZE     string = "10000"
	MQ_POOL_SHOW_LOG string = "true"

	// AWS TLS
	MQ_USE_TLS          string = "false"
	MQ_TLS_ROOTCAS_FILE string = ""
	MQ_TLS_CERT_FILE    string = ""
	MQ_TLS_KEY_FILE     string = ""
)
