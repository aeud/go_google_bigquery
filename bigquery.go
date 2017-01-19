package go_google_bigquery

import (
	"io/ioutil"
	"log"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery "google.golang.org/api/bigquery/v2"
)

const (
	SourceFormat     string = "NEWLINE_DELIMITED_JSON"
	WriteDisposition string = "WRITE_TRUNCATE"
)

type BQField struct {
	Ref *bigquery.TableFieldSchema
}

func NewBQField(n, t, d string) *BQField {
	f := new(BQField)
	f.Ref = new(bigquery.TableFieldSchema)
	f.Ref.Mode = "NULLABLE"
	f.Ref.Name = n
	f.Ref.Type = t
	f.Ref.Description = d
	return f
}

func NewBQFieldWithNested(n, d string, nest *BQSchema) *BQField {
	f := new(BQField)
	f.Ref = new(bigquery.TableFieldSchema)
	f.Ref.Mode = "NULLABLE"
	f.Ref.Name = n
	f.Ref.Type = "RECORD"
	f.Ref.Description = d
	f.Ref.Fields = nest.getRefFields()
	return f
}

func NewBQFieldWithRepeated(n, d string, nest *BQSchema) *BQField {
	f := new(BQField)
	f.Ref = new(bigquery.TableFieldSchema)
	f.Ref.Mode = "REPEATED"
	f.Ref.Name = n
	f.Ref.Type = "RECORD"
	f.Ref.Description = d
	f.Ref.Fields = nest.getRefFields()
	return f
}

type BQSchema struct {
	Fields []*BQField
}

func (s *BQSchema) getRefFields() []*bigquery.TableFieldSchema {
	fs := s.Fields
	rs := make([]*bigquery.TableFieldSchema, len(fs))
	for i := 0; i < len(fs); i++ {
		rs[i] = fs[i].Ref
	}
	return rs
}

func (s *BQSchema) getRefSchema() *bigquery.TableSchema {
	t := new(bigquery.TableSchema)
	t.Fields = s.getRefFields()
	return t
}

func NewBQSchema(fs []*BQField) *BQSchema {
	s := new(BQSchema)
	s.Fields = fs
	return s
}

func NewEmptyBQSchema() *BQSchema {
	s := new(BQSchema)
	s.Fields = make([]*BQField, 0)
	return s
}

func (s *BQSchema) AddField(f *BQField) {
	s.Fields = append(s.Fields, f)
}

type BQService struct {
	Service  *bigquery.JobsService
	ProjetID string
	Schema   *BQSchema
}

func NewBQService(projectID, keyFile string) *BQService {
	s := new(BQService)
	data, err := ioutil.ReadFile(keyFile)
	if err != nil {
		log.Fatal(err)
	}
	conf, err := google.JWTConfigFromJSON(data, []string{bigquery.BigqueryScope}...)
	if err != nil {
		log.Fatal(err)
	}
	client := conf.Client(oauth2.NoContext)
	service, err := bigquery.New(client)
	if err != nil {
		log.Fatal(err)
	}
	s.Service = bigquery.NewJobsService(service)
	s.ProjetID = projectID
	return s
}

func (s *BQService) NewJob(dataset, table, source string, schema *BQSchema) *BQJob {
	job := new(BQJob)
	job.Service = s
	job.Schema = schema
	job.Dataset = dataset
	job.Table = table
	job.Source = source
	job.try = 0
	return job
}

type BQJob struct {
	Service *BQService
	Schema  *BQSchema
	Dataset string
	Table   string
	Source  string
	try     int
}

func (j *BQJob) GetTableRef() *bigquery.TableReference {
	t := new(bigquery.TableReference)
	t.DatasetId = j.Dataset
	t.ProjectId = j.Service.ProjetID
	t.TableId = j.Table
	return t
}

func (j *BQJob) GetRefJob() *bigquery.Job {
	load := new(bigquery.JobConfigurationLoad)
	load.DestinationTable = j.GetTableRef()
	load.Schema = j.Schema.getRefSchema()
	load.SourceFormat = SourceFormat
	load.SourceUris = []string{j.Source}
	load.WriteDisposition = WriteDisposition
	conf := new(bigquery.JobConfiguration)
	conf.Load = load
	job := new(bigquery.Job)
	job.Configuration = conf
	return job
}

func (j *BQJob) Do() {
	service := j.Service.Service
	projectID := j.Service.ProjetID

	insertJob, err := service.Insert(projectID, j.GetRefJob()).Do()
	if err != nil {
		log.Fatal(err)
	}

	CheckJob(service.Get(projectID, insertJob.JobReference.JobId), j)
}

func CheckJob(c *bigquery.JobsGetCall, j *BQJob) {
	job, err := c.Do()
	if err != nil {
		if j.try < 5 {
			j.try = j.try + 1
			time.Sleep(time.Second)
			j.Do()
		} else {
			log.Fatalf("Error when checking the job: %v", err)
		}
	}
	log.Println(job.Status.State)
	if job.Status.State != "DONE" {
		time.Sleep(time.Second)
		CheckJob(c, j)
	} else if job.Status.ErrorResult != nil {
		log.Println(job.Status.ErrorResult)
		for i := 0; i < len(job.Status.Errors); i++ {
			log.Println(job.Status.Errors[i])
		}
	}
}
