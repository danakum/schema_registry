package schema_registry

import (
	"encoding/json"
	"fmt"
	schemaregistry "github.com/datamountaineer/schema-registry"
	"github.com/tryfix/log"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"
)

type Subject struct {
	Schema  string `json:"subject"` // The actual AVRO subject
	Subject string `json:"subject"` // Subject where the subject is registered for
	Version int    `json:"version"` // Version within this subject
	Id      int    `json:"id"`      // Registry's unique id
}

const Latest = -1

type SchemaRegistry struct {
	client       *schemaregistry.Client
	httpClient   *http.Client
	schemas      map[string]*Encoder
	schemaIds    map[int]*Encoder
	jsonEncoders map[int]jsonEncoder
	mu           *sync.Mutex
	url          *url.URL
}

func NewSchemaRegistry(path string) *SchemaRegistry {
	client, err := schemaregistry.NewClient(path)
	if err != nil {
		log.Fatal(log.WithPrefix(`avro.registry`, `client failed`), err)
	}

	u, err := url.Parse(path)
	if err != nil {
		log.Fatal(err)
	}

	return &SchemaRegistry{
		schemas:      make(map[string]*Encoder),
		schemaIds:    make(map[int]*Encoder),
		jsonEncoders: make(map[int]jsonEncoder),
		client:       client,
		mu:           &sync.Mutex{},
		url:          u,
		httpClient:   http.DefaultClient,
	}
}

type jsonEncoder func(data []byte) (v interface{}, err error)

func (r *SchemaRegistry) Register(subject string, version int, encoder jsonEncoder) {
	if _, ok := r.schemas[subject]; ok {
		log.Warn(log.WithPrefix(`avro.registry`, fmt.Sprintf(`subject [%s] already registred`, subject)))
	}

	sub, err := r.GetBySubject(subject, version)
	if err != nil {
		log.Fatal(log.WithPrefix(`avro.registry`, fmt.Sprintf(`cannot get subject for [%s]`, subject)), err)
	}

	s := NewEncoder(r, sub)
	r.schemas[subject] = s
	r.schemaIds[sub.Id] = s
	r.jsonEncoders[sub.Id] = encoder
}

func (r *SchemaRegistry) RegisterLatest(subject string, encoder jsonEncoder) {
	if _, ok := r.schemas[subject]; ok {
		log.Fatal(log.WithPrefix(`avro.registry`, fmt.Sprintf(`subject [%s] already registred`, subject)))
	}

	sub, err := r.GetLatest(subject)
	if err != nil {
		log.Fatal(log.WithPrefix(`avro.registry`, fmt.Sprintf(`cannot get latest subject for [%s]`, subject)), err)
	}

	s := NewEncoder(r, sub)
	r.schemas[subject] = s
	r.schemaIds[sub.Id] = s
	r.jsonEncoders[sub.Id] = encoder
}

func (r *SchemaRegistry) WithSchema(subject string) *Encoder {
	r.mu.Lock()
	defer r.mu.Unlock()

	s, ok := r.schemas[subject]
	if !ok {
		log.Fatal(log.WithPrefix(`avro.registry`, fmt.Sprintf(`unregistred subject [%s]`, subject)))
	}

	return s
}

func (r *SchemaRegistry) GetBySubject(subject string, version int) (*Subject, error) {
	sub, err := r.client.GetSchemaBySubject(subject, version)
	if err != nil {
		return nil, err
	}

	return &Subject{
		Schema:  sub.Schema,
		Id:      sub.ID,
		Version: sub.Version,
		Subject: sub.Subject,
	}, nil
}

func (r *SchemaRegistry) GetLatest(subject string) (*Subject, error) {
	sub, err := r.client.GetLatestSchema(subject)
	if err != nil {
		return nil, err
	}

	return &Subject{
		Schema:  sub.Schema,
		Id:      sub.ID,
		Version: sub.Version,
		Subject: sub.Subject,
	}, nil
}

func (r *SchemaRegistry) Get(schemaId int) (string, error) {

	// if locally available get from local registry
	r.mu.Lock()
	sub, ok := r.schemaIds[schemaId]
	r.mu.Unlock()
	if !ok {
		log.Error(log.WithPrefix(`avro.registry`,
			fmt.Sprintf(`schemaId [%d] dose not exist`, schemaId)))

	}

	return sub.Schema(), nil
}

func (r *SchemaRegistry) GetOrFetch(schemaId int) (string, error) {

	// if locally available get from local registry
	r.mu.Lock()
	sub, ok := r.schemaIds[schemaId]
	r.mu.Unlock()
	if !ok {
		log.Warn(log.WithPrefix(`avro.registry`,
			fmt.Sprintf(`schemaId [%d] dose not exist, fetching from remote registry`, schemaId)))

		return r.client.GetSchemaById(schemaId)

	}

	return sub.Schema(), nil
}

func (r *SchemaRegistry) IsCompatible(schema string, subject string, version int) (bool, error) {

	var resp struct {
		IsCompatible bool `json:"is_compatible"`
	}

	err := r.do(
		"POST",
		fmt.Sprintf("/compatibility/subjects/%s/versions/%d", subject, version),
		fmt.Sprintf(`{"schema": %s}`, schema),
		&resp)

	if err != nil {
		log.Error(log.WithPrefix(`avro.registry`,
			fmt.Sprintf(`cannot check compatibility for %s[%d]`, subject, version)))
		return false, err
	}

	return resp.IsCompatible, nil
}

func (r *SchemaRegistry) do(method, urlPath string, in interface{}, out interface{}) error {
	u := r.url
	u.Path = path.Join(u.Path, urlPath)
	var rdp io.Reader
	if in != nil {
		var wr *io.PipeWriter
		rdp, wr = io.Pipe()
		go func() {
			wr.CloseWithError(json.NewEncoder(wr).Encode(in))
		}()
	}
	req, err := http.NewRequest(method, u.String(), rdp)
	req.Header.Add("Accept", "application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json")
	if err != nil {
		return err
	}
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseSchemaRegistryError(resp)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type confluentError struct {
	ErrorCode int    `json:"error_code"`
	Message   string `json:"message"`
}

func (ce confluentError) Error() string {
	return fmt.Sprintf("%s (%d)", ce.Message, ce.ErrorCode)
}

func parseSchemaRegistryError(resp *http.Response) error {
	var ce confluentError
	if err := json.NewDecoder(resp.Body).Decode(&ce); err != nil {
		return err
	}
	return ce
}
