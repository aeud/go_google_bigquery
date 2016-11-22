package v2

import (
	"log"
	"testing"
)

func TestQuery(t *testing.T) {
	log.Println("test")
	service := NewBQService("luxola.com:luxola-analytics", "/Users/adrien/.ssh/google.json")
	service.Query(`
WITH orders AS (
    select account, id from lx.orders
)
SELECT
  a.account,
  a.id AS local_address_id,
  a.addressable_id AS local_order_id,
  a.first_name,
  a.last_name,
  CONCAT(a.address_1, '||', address_2, '||', address_3) AS address,
  a.city,
  a.postal_code,
  a.state,
  c.name country_name,
  a.mobile_phone
FROM
  lx.addresses a
INNER JOIN
  lx.countries c
ON
  c.account = a.account
  AND c.id = a.country_id
WHERE
  addressable_type = 'Order'
  AND addressable_id IS NOT NULL
    `, "go", "test_addresses").Do()
}
