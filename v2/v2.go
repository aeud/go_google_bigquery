package v2

import (
	"encoding/json"
	"errors"
	"fmt"
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
		log.Fatalf("IO: %v\n", err)
	}
	conf, err := google.JWTConfigFromJSON(data, []string{bigquery.BigqueryScope}...)
	if err != nil {
		log.Fatalf("JWT: %v\n", err)
	}
	client := conf.Client(oauth2.NoContext)
	service, err := bigquery.New(client)
	if err != nil {
		log.Fatalf("Service: %v\n", err)
	}
	s.Service = bigquery.NewJobsService(service)
	s.ProjetID = projectID
	return s
}

func (s *BQService) NewJob(dataset, table, source, schema string) *BQJob {
	bqSchema := new(bigquery.TableSchema)
	err := json.Unmarshal([]byte(schema), bqSchema)
	if err != nil {
		log.Fatalf("Job: %v\n", err)
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

func (j *BQJob) Do() *BQResult {
	service := j.Service.Service
	projectID := j.Service.ProjetID

	insertJob, err := service.Insert(projectID, j.GetRefJob()).Do()
	if err != nil {
		log.Fatalf("Job do: %v\n", err)
	}

	return CheckJob(service.Get(projectID, insertJob.JobReference.JobId), j.Source)
}

func CheckJob(c *bigquery.JobsGetCall, mess string) *BQResult {
	job, err := c.Do()
	if err != nil {
		log.Fatalf("Check job: %v\n", err)
	}
	// log.Println(job.Status.State)
	if job.Status.State != "DONE" {
		time.Sleep(time.Second)
		return CheckJob(c, mess)
	} else {
		r := NewBQResult(job)
		r.Message = "Success"
		if job.Status.ErrorResult != nil {
			r.Error = errors.New(fmt.Sprintf("%v", job.Status.ErrorResult.Message))
			r.Message = fmt.Sprintf("Error: %v", job.Status.ErrorResult.Message)
			// log.Println(job.Status.ErrorResult)
			// log.Println(mess)
			// for i := 0; i < len(job.Status.Errors); i++ {
			// log.Println(job.Status.Errors[i])
			//}
		}
		return r
	}
}

type BQQueryJob struct {
	Service            *BQService
	CreateDisposition  string
	DestinationDataset string
	DestinationTable   string
	Q                  string
	UseLegacySql       bool
	WriteDisposition   string
}

func (s *BQService) Query(query, dataset, table string) *BQQueryJob {
	j := new(BQQueryJob)
	j.Service = s
	j.DestinationDataset = dataset
	j.DestinationTable = table
	j.Q = query
	return j
}

func (j *BQQueryJob) GetRefJob() *bigquery.Job {
	q := new(bigquery.JobConfigurationQuery)
	q.UseLegacySql = false
	q.CreateDisposition = "CREATE_IF_NEEDED"
	q.WriteDisposition = "WRITE_TRUNCATE"
	q.Query = j.Q
	q.ForceSendFields = []string{"UseLegacySql"}
	dataset := new(bigquery.DatasetReference)
	dataset.DatasetId = j.DestinationDataset
	dataset.ProjectId = j.Service.ProjetID
	q.DefaultDataset = dataset
	table := new(bigquery.TableReference)
	table.DatasetId = j.DestinationDataset
	table.ProjectId = j.Service.ProjetID
	table.TableId = j.DestinationTable
	q.DestinationTable = table
	conf := new(bigquery.JobConfiguration)
	conf.Query = q
	job := new(bigquery.Job)
	job.Configuration = conf
	return job
}

func (j *BQQueryJob) Do() *BQResult {
	service := j.Service.Service
	projectID := j.Service.ProjetID
	fmt.Println(j.DestinationTable)
	insertJob, err := service.Insert(projectID, j.GetRefJob()).Do()
	if err != nil {
		log.Fatalf("Job Query Do: %v\n", err)
	}

	return CheckJob(service.Get(projectID, insertJob.JobReference.JobId), j.Q)
}

type BQResult struct {
	job     *bigquery.Job
	Message string
	Error   error
}

func NewBQResult(job *bigquery.Job) *BQResult {
	r := new(BQResult)
	r.job = job
	return r
}
