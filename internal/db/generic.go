package db

import (
	"fmt"

	"github.com/hashicorp/go-multierror"
	"github.com/jmoiron/sqlx"
)

type Queryer interface {
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
}

// getRows is a generic helper function to iterate over
// rows returned by a query to return the an array of the
// parameter type. It is based on the sqlx.StructScan API
// hence the parameter type can hold `db` tag on its fields
// to configure the field name column mapping.
func getRows[T any](db Queryer, query string) (t []T, ret error) {
	rows, err := db.Queryx(query)
	if err != nil {
		return nil, fmt.Errorf("cannot query the database: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t, ret = nil, multierror.Append(ret, err)
		}
	}()

	for rows.Next() {
		var singleT T
		if err := rows.StructScan(&singleT); err != nil {
			return nil, err
		}
		t = append(t, singleT)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("cannot browse rows: %w", err)
	}
	return
}
