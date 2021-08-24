package schema_registry

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/linkedin/goavro"
	"github.com/tryfix/log"
)

type Encoder struct {
	subject  *Subject
	registry *SchemaRegistry
}

func NewEncoder(reg *SchemaRegistry, subject *Subject) *Encoder {
	return &Encoder{
		subject:  subject,
		registry: reg,
	}
}

func (s *Encoder) Encode(data interface{}) ([]byte, error) {
	codec, err := goavro.NewCodec(s.subject.Schema)
	if err != nil {
		log.Error(log.WithPrefix(`avro.serializer`, `codec failed`), err)
		return nil, err
	}

	byt, err := json.Marshal(data)
	if err != nil {
		log.Error(log.WithPrefix(`avro.serializer`, `json marshal failed`), err)
		return nil, err
	}

	native, _, err := codec.NativeFromTextual(byt)
	if err != nil {
		log.Error(log.WithPrefix(`avro.serializer`, `native from textual failed`), err)
		return nil, err
	}

	magic := s.encodePrefix(s.subject.Id)

	return codec.BinaryFromNative(magic, native)
}

func (s *Encoder) Decode(data []byte) (interface{}, error) {

	if len(data) < 5 {
		log.Error(log.WithPrefix(`avro.serializer.Deserialize`, `message length is zero`))
		return nil, errors.New(`avro.serializer.Deserialize: message length is zero`)
	}

	schemaId, err := s.decodePrefix(data)
	if err != nil {
		return nil, err
	}

	schema, err := s.registry.GetOrFetch(schemaId)
	if err != nil {
		log.Error(string(data))
		return nil, err
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Error(log.WithPrefix(`avro.serializer.Deserialize`, `codec failed`), err)
		return nil, err
	}

	native, _, err := codec.NativeFromBinary(data[5:])
	if err != nil {
		log.Error(log.WithPrefix(`avro.serializer.Deserialize`, `native from binary failed`), err)
		return nil, err
	}

	byt, err := codec.TextualFromNative(nil, native)
	if err != nil {
		log.Error(log.WithPrefix(`avro.serializer.Deserialize`, `textual from native failed`), err)
		return nil, err
	}

	encoder, ok := s.registry.jsonEncoders[schemaId]
	if !ok {
		return nil, errors.New(fmt.Sprintf(`schema id [%d] dose not have a json decoder`, schemaId))
	}

	return encoder(byt)
}

func (s *Encoder) encodePrefix(id int) []byte {
	byt := make([]byte, 5)
	binary.BigEndian.PutUint32(byt[1:], uint32(id))
	return byt
}

func (s *Encoder) decodePrefix(byt []byte) (int, error) {
	if len(byt) < 5 {
		return 0, errors.New("Cannot decode prefix, slice length is less than 5")
	}
	return int(binary.BigEndian.Uint32(byt[1:5])), nil
}

func (s *Encoder) Schema() string {
	return s.subject.Schema
}
