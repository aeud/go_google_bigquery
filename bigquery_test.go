package go_google_bigquery

import (
	"testing"
)

func TestEmptySchema(t *testing.T) {
	s := NewEmptyBQSchema()
	s.AddField(NewBQField("foo", "STRING", "Test description"))
}

func TestSchema(t *testing.T) {
	s := NewBQSchema([]*BQField{
		NewBQField("foo", "STRING", "Test description"),
	})
	s.AddField(NewBQField("foo2", "STRING", "Test description 2"))
}

func TestNestedSchema(t *testing.T) {
	s := NewBQSchema([]*BQField{
		NewBQField("foo", "STRING", "Test description"),
		NewBQFieldWithNested("foo", "Test description", NewBQSchema([]*BQField{
			NewBQField("foo3", "STRING", "Test description 3"),
		})),
	})
	s.AddField(NewBQField("foo2", "STRING", "Test description 2"))
}

func TestRepeatedSchema(t *testing.T) {
	s := NewBQSchema([]*BQField{
		NewBQField("foo", "STRING", "Test description"),
		NewBQFieldWithRepeated("foo", "Test description", NewBQSchema([]*BQField{
			NewBQField("foo3", "STRING", "Test description 3"),
		})),
	})
	s.AddField(NewBQField("foo2", "STRING", "Test description 2"))
}

//func TestExecuteJob(t *testing.T) {
//	schema := NewBQSchema([]*BQField{
//		schema.AddField(NewBQField("foo", "STRING", "Test description")),
//	})//

//	service := NewBQService("luxola.com:luxola-analytics", "/Users/adrien/.ssh/google.json")
//	job := service.NewJob("go", "test_table", "gs://lx-ga/test.json.gz", schema)//

//	job.Do()
//}
