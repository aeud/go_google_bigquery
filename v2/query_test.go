package v2

import (
	"log"
	"testing"
)

func TestQuery(t *testing.T) {
	log.Println("test")
	service := NewBQService("luxola.com:luxola-analytics", "/Users/adrien/.ssh/google.json")
	service.Query(`select account, orderId from dwh.orders limit 100`, "go", "test_orders").Do()
}
