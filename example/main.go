package main

import (
	"encoding/json"
	"github.com/danakum/schema_registry"
	"github.com/google/uuid"
	"github.com/tryfix/log"
	"time"
)

type BD struct {
	Id int `json:"id"`
}

type BDV2 struct {
	Id int `json:"id"`
}

type BD2 struct {
	Name string `json:"name"`
}

type CompatibilityTest struct {
	Id        string `json:"id"`
	Type      string `json:"type"`
	Body      BD     `json:"body"`
	CreatedAt int64  `json:"created_at"`
	Expiry    int64  `json:"expiry"`
	Version   int    `json:"version"`
}

type CompatibilityTestV2 struct {
	Id        string `json:"id"`
	Type      string `json:"type"`
	Body      BDV2   `json:"body"`
	CreatedAt int64  `json:"created_at"`
	Expiry    int64  `json:"expiry"`
	Version   int    `json:"version"`
}

type compatibilityTest2 struct {
	Id        string `json:"id"`
	Type      string `json:"type"`
	Body      BD2    `json:"body"`
	CreatedAt int64  `json:"created_at"`
	Expiry    int64  `json:"expiry"`
	Version   int    `json:"version"`
}

func main() {

	// init a new schema registry instance and connect
	registry := schema_registry.NewSchemaRegistry(`http://35.184.181.97:8089/`)

	// register schemas with versions
	registry.Register(`compatibility_test_1`, 6, func(data []byte) (v interface{}, err error) {
		v = CompatibilityTest{}
		err = json.Unmarshal(data, &v)
		if err != nil {
			log.Fatal(err)
		}

		return
	})

	registry.Register(`compatibility_test_2`, 2, func(data []byte) (v interface{}, err error) {
		v = compatibilityTest2{}
		err = json.Unmarshal(data, &v)
		if err != nil {
			log.Fatal(err)
		}

		return
	})

	bytMsgOne, err := registry.WithSchema(`compatibility_test_1`).Encode(CompatibilityTest{
		Id:   uuid.New().String(),
		Type: `driver_created`,
		Body: BD{
			Id: 1000,
		},
		CreatedAt: time.Now().UnixNano() / 1000000,
		Version:   0,
	})

	registry.Register(`compatibility_test_1`, 7, func(data []byte) (v interface{}, err error) {
		v = CompatibilityTest{}
		err = json.Unmarshal(data, &v)
		if err != nil {
			log.Fatal(err)
		}

		return
	})

	bytMsgOneV2, err := registry.WithSchema(`compatibility_test_1`).Encode(CompatibilityTestV2{
		Id:   uuid.New().String(),
		Type: `driver_created`,
		Body: BDV2{
			Id: 1000,
		},
		CreatedAt: time.Now().UnixNano() / 1000000,
		Version:   0,
	})

	bytMsgTwo, err := registry.WithSchema(`compatibility_test_2`).Encode(compatibilityTest2{
		Id:   uuid.New().String(),
		Type: `driver_created_other`,
		Body: BD2{
			Name: `gayan 123`,
		},
		CreatedAt: time.Now().UnixNano() / 1000000,
		Version:   0,
	})

	if err != nil {
		log.Fatal(err)
	}

	decode(registry, bytMsgOne)
	decode(registry, bytMsgTwo)
	decode(registry, bytMsgOneV2)

}

func decode(reg *schema_registry.SchemaRegistry, message []byte) {

	data, err := reg.WithSchema(`compatibility_test_1`).Decode(message)
	if err != nil {
		log.Fatal(err)
	}

	log.Info(data)
}
