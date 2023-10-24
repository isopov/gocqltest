package main

import (
	"sync"

	"github.com/gocql/gocql"
)

// on my machine this test passes with 50_000 workers and 100 queries but fails with 100_000 workers
const workers = 100_000
const queries = 100

func main() {
	cluster := gocql.NewCluster("localhost:9042")
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	execRelease(session.Query("drop keyspace if exists pargettest"))
	execRelease(session.Query("create keyspace pargettest with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}"))
	execRelease(session.Query("drop table if exists pargettest.test"))
	execRelease(session.Query("create table pargettest.test (a text, b int, primary key(a))"))
	execRelease(session.Query("insert into pargettest.test (a, b) values ( 'a', 1)"))

	var wg sync.WaitGroup

	for i := 1; i <= workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for j := 0; j < queries; j++ {
				iterRelease(session.Query("select * from pargettest.test where a='a'"))
			}
		}()
	}

	wg.Wait()
}

func iterRelease(query *gocql.Query) {
	_, err := query.Iter().SliceMap()
	if err != nil {
		println(err.Error())
		panic(err)
	}
	query.Release()
}

func execRelease(query *gocql.Query) {
	if err := query.Exec(); err != nil {
		println(err.Error())
		panic(err)
	}
	query.Release()
}
