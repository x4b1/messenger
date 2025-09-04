package aws

// WithDefaultOrderingKey returns an option to configure the default ordering key for SNS or SQS publishers.
func WithDefaultOrderingKey(key string) DefaultOrderingKeyOption {
	return DefaultOrderingKeyOption(key)
}

// DefaultOrderingKeyOption is an option type for setting the default ordering key for SNS or SQS publishers.
type DefaultOrderingKeyOption string

func (d DefaultOrderingKeyOption) applySNSPublisher(p *SNSPublisher) {
	p.defaultOrdKey = string(d)
}

func (d DefaultOrderingKeyOption) applySQSPublisher(p *SQSPublisher) {
	p.defaultOrdKey = string(d)
}

// WithMetaOrderingKey returns an option to configure the metadata ordering key for SNS or SQS publishers.
func WithMetaOrderingKey(key string) MetaOrderingKeyOption {
	return MetaOrderingKeyOption(key)
}

// MetaOrderingKeyOption is an option type for setting the metadata ordering key for SNS or SQS publishers.
type MetaOrderingKeyOption string

func (m MetaOrderingKeyOption) applySNSPublisher(p *SNSPublisher) {
	p.metaOrdKey = string(m)
}

func (m MetaOrderingKeyOption) applySQSPublisher(p *SQSPublisher) {
	p.metaOrdKey = string(m)
}

// WithMessageIDKey returns an option to configure the message ID key for SNS or SQS publishers, or SQS subscribers.
func WithMessageIDKey(key string) MessageIDKeyOption {
	return MessageIDKeyOption(key)
}

// MessageIDKeyOption is an option type for setting the message ID key for SNS or SQS publishers, or SQS subscribers.
type MessageIDKeyOption string

func (m MessageIDKeyOption) applySNSPublisher(p *SNSPublisher) {
	p.msgIDKey = string(m)
}

func (m MessageIDKeyOption) applySQSPublisher(p *SQSPublisher) {
	p.msgIDKey = string(m)
}

// applySQSSubscriber applies the message ID key to an SQSSubscriber.
func (m MessageIDKeyOption) applySQSSubscriber(p *SQSSubscriber) {
	p.msgIDKey = string(m)
}

// WithFifoQueue returns an option to enable or disable FIFO queue usage for SNS or SQS publishers.
func WithFifoQueue(fifo bool) FifoQueueOption {
	return FifoQueueOption(fifo)
}

// FifoQueueOption is an option type for enabling or disabling FIFO queue usage for SNS or SQS publishers.
type FifoQueueOption bool

func (f FifoQueueOption) applySNSPublisher(p *SNSPublisher) {
	p.fifo = bool(f)
}

func (f FifoQueueOption) applySQSPublisher(p *SQSPublisher) {
	p.fifo = bool(f)
}

// WithMaxWaitSeconds returns an option to set the maximum wait time (in seconds) for SQS subscribers.
func WithMaxWaitSeconds(waitSec int) MaxWaitSecondsOption {
	return MaxWaitSecondsOption(waitSec)
}

// MaxWaitSecondsOption is an option type for setting the maximum wait time (in seconds) for SQS subscribers.
type MaxWaitSecondsOption int

func (m MaxWaitSecondsOption) applySQSSubscriber(p *SQSSubscriber) {
	p.maxWaitSeconds = int(m)
}

// WithMaxMessages returns an option to set the maximum number of messages to receive for SQS subscribers.
func WithMaxMessages(msgs int) MaxMessagesOption {
	return MaxMessagesOption(msgs)
}

// MaxMessagesOption is an option type for setting the maximum number of messages to receive for SQS subscribers.
type MaxMessagesOption int

func (m MaxMessagesOption) applySQSSubscriber(p *SQSSubscriber) {
	p.maxMessages = int(m)
}
