package v2

import (
	"fmt"
	"testing"
)

func TestDo(t *testing.T) {
	doc := `
{
    "fields": [
        { "name": "date", "type": "TIMESTAMP", "mode": "NULLABLE", "description": "Date" },
        { "name": "currency", "type": "STRING", "mode": "NULLABLE", "description": "Currency" },
        {
            "name": "unitsPer",
            "type": "RECORD",
            "description": "1 XXX in Currency",
            "fields": [
                { "name": "SGD", "type": "FLOAT", "mode": "NULLABLE" },
                { "name": "EUR", "type": "FLOAT", "mode": "NULLABLE" }
            ]
        },
        {
            "name": "perUnit",
            "type": "RECORD",
            "description": "1 Currency in XXX",
            "fields": [
                { "name": "SGD", "type": "FLOAT", "mode": "NULLABLE" },
                { "name": "EUR", "type": "FLOAT", "mode": "NULLABLE" }
            ]
        }
    ]
}
    `
	service := NewBQService("luxola.com:luxola-analytics", "/Users/adrien/.ssh/google.json")
	service.NewJob("go", "xe", "gs://lx-ga/dwh/xe/*", doc).Do()
	r := service.NewJob("go", "xe", "gs://lx-ga/dwh/xe/*", doc).Do()
	fmt.Println(r)
}
