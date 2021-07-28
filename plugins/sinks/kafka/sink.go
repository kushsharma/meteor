package kafka

import (
	"context"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/jhump/protoreflect/desc"

	"github.com/jhump/protoreflect/desc/protoparse"

	"github.com/jhump/protoreflect/dynamic"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"github.com/jhump/protoreflect/desc/builder"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/golang/protobuf/proto"
	"github.com/odpf/meteor/core"
	"github.com/odpf/meteor/core/sink"
	"github.com/odpf/meteor/utils"
	"github.com/pkg/errors"
)

type Sink struct{}

func New() core.Syncer {
	return new(Sink)
}

type Config struct {
	Brokers      string `mapstructure:"brokers" validate:"required"`
	Topic        string `mapstructure:"topic" validate:"required"`
	KeyPath      string `mapstructure:"key_path"`
	ProtoFile    string `mapstructure:"proto_file"`
	ProtoMessage string `mapstructure:"proto_message"`
}

func (s *Sink) Sink(ctx context.Context, config map[string]interface{}, in <-chan interface{}) (err error) {
	kafkaConf := &Config{}
	if err := utils.BuildConfig(config, kafkaConf); err != nil {
		return err
	}
	producer, err := getProducer(kafkaConf.Brokers)
	if err != nil {
		return errors.Wrapf(err, "failed to create kafka producer")
	}
	defer producer.Flush(5000)

	// prepare kafka key message wrapper if possible
	var keyMessageFieldName string
	var kafkaKeyDescriptor *desc.MessageDescriptor
	if kafkaConf.KeyPath != "" {
		keyMessageFieldName, err = s.getTopLevelKeyFromPath(kafkaConf.KeyPath)
		if err != nil {
			return err
		}
		kafkaKeyDescriptor, err = s.buildKeyDescriptor(kafkaConf.ProtoFile, kafkaConf.ProtoMessage, keyMessageFieldName)
		if err != nil {
			return err
		}
	}

	for val := range in {
		if err := s.push(producer, kafkaConf, val, kafkaKeyDescriptor, keyMessageFieldName); err != nil {
			return err
		}
	}
	return nil
}

func getProducer(brokers string) (*kafka.Producer, error) {
	producerConf := &kafka.ConfigMap{}
	producerConf.SetKey("bootstrap.servers", brokers)
	producerConf.SetKey("acks", "all")
	return kafka.NewProducer(producerConf)
}

func (s *Sink) push(producer *kafka.Producer, conf *Config, payload interface{}, keyDescriptor *desc.MessageDescriptor, keyMessageFieldName string) error {
	kafkaValue, err := s.buildValue(payload)
	if err != nil {
		return err
	}

	var kafkaKey []byte = nil
	if keyDescriptor != nil {
		kafkaKey, err = s.buildKey(payload, keyDescriptor, keyMessageFieldName)
		if err != nil {
			return err
		}
	}

	return producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &conf.Topic},
		Key:            kafkaKey,
		Value:          kafkaValue,
	}, nil)
}

func (s *Sink) buildValue(value interface{}) ([]byte, error) {
	protoBytes, err := proto.Marshal(value.(proto.Message))
	if err != nil {
		return nil, errors.Wrap(err, "failed to serialize payload as a protobuf message")
	}
	return protoBytes, nil
}

func (s *Sink) buildKey(payload interface{}, keyDescriptor *desc.MessageDescriptor, keyMessageFieldName string) ([]byte, error) {
	keyString, err := s.extractKeyFromPayload(keyMessageFieldName, payload)
	if err != nil {
		return nil, err
	}

	// populate message
	dynamicMsgKey := dynamic.NewMessage(keyDescriptor)
	dynamicMsgKey.SetFieldByName(keyMessageFieldName, keyString)
	return dynamicMsgKey.Marshal()
}

func (s *Sink) buildKeyDescriptor(protoFilePath, protoMessageName, messageFieldName string) (*desc.MessageDescriptor, error) {
	if protoMessageName == "" || messageFieldName == "" {
		// user doesn't want key to be pushed
		return nil, nil
	}
	if protoFilePath != "" {
		keyProtobufFile, err := ioutil.ReadFile(protoFilePath)
		if err != nil {
			return nil, err
		}
		// parse descriptor
		protoParser := protoparse.Parser{
			Accessor: protoparse.FileContentsFromMap(map[string]string{
				protoMessageName: string(keyProtobufFile),
			}),
		}
		fileDescriptors, err := protoParser.ParseFiles(protoMessageName)
		if err != nil {
			return nil, errors.Wrap(err, "failed to parse proto file")
		}
		kafkaKeyFileDescriptor := fileDescriptors[0]
		kafkaKeyDescriptor := kafkaKeyFileDescriptor.FindMessage(protoMessageName)
		if kafkaKeyDescriptor == nil {
			return nil, errors.Errorf("failed to find %s key in proto", protoMessageName)
		}
		return kafkaKeyDescriptor, nil
	}

	// strip fully qualified name
	protoMessageParts := strings.Split(protoMessageName, "")
	protoMessageName = protoMessageParts[len(protoMessageParts)-1]

	// build descriptor
	return builder.NewMessage(protoMessageName).
		AddField(builder.NewField(messageFieldName, builder.FieldTypeScalar(descriptor.FieldDescriptorProto_TYPE_STRING))).
		Build()
}

func (s *Sink) extractKeyFromPayload(fieldName string, value interface{}) (string, error) {
	valueOf := reflect.ValueOf(value)
	if valueOf.Kind() == reflect.Ptr {
		valueOf = valueOf.Elem()
	}
	if valueOf.Kind() != reflect.Struct {
		return "", errors.New("invalid data")
	}

	fieldVal := valueOf.FieldByName(fieldName)
	if !fieldVal.IsValid() || fieldVal.IsZero() {
		return "", errors.New("invalid path, unknown field")
	}
	if fieldVal.Type().Kind() != reflect.String {
		return "", errors.Errorf("unsupported key type, should be string found: %s", fieldVal.Type().String())
	}

	return fieldVal.String(), nil
}

func (s *Sink) getTopLevelKeyFromPath(keyPath string) (string, error) {
	keyPaths := strings.Split(keyPath, ".")
	if len(keyPaths) < 2 {
		return "", errors.New("invalid path, require at least one field name e.g.: .URN")
	}
	if len(keyPaths) > 2 {
		return "", errors.New("invalid path, doesn't support nested field names yet")
	}
	return keyPaths[1], nil
}

func init() {
	if err := sink.Catalog.Register("kafka", New()); err != nil {
		panic(err)
	}
}
