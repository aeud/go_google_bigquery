package v2

import (
	"encoding/json"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	bigquery "google.golang.org/api/bigquery/v2"
	"io/ioutil"
	"log"
	"time"
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

type BQService struct {
	Service  *bigquery.JobsService
	ProjetID string
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

func (s *BQService) NewJob(dataset, table, source string, schema string) *BQJob {
	bqSchema := new(bigquery.TableSchema)
	err := json.Unmarshal([]byte(schema), bqSchema)
	if err != nil {
		log.Fatalln(err)
	}
	job := new(BQJob)
	job.Service = s
	job.Schema = bqSchema
	job.Dataset = dataset
	job.Table = table
	job.Source = source
	return job
}

type BQJob struct {
	Service *BQService
	Schema  *bigquery.TableSchema
	Dataset string
	Table   string
	Source  string
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
	load.Schema = j.Schema
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

	CheckJob(service.Get(projectID, insertJob.JobReference.JobId))
}

func CheckJob(c *bigquery.JobsGetCall) {
	job, err := c.Do()
	if err != nil {
		log.Fatal(err)
	}
	log.Println(job.Status.State)
	if job.Status.State != "DONE" {
		time.Sleep(time.Second)
		CheckJob(c)
	} else if job.Status.ErrorResult != nil {
		log.Println(job.Status.ErrorResult)
		for i := 0; i < len(job.Status.Errors); i++ {
			log.Println(job.Status.Errors[i])
		}
	}
}
