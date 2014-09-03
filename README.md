```
this is the toofar branch, decided to pursue a different approach and stick more to the go sql spec
```

#sqlpg

sqlpg is a library which provides a set of extensions on go's standard
`database/sql` library to harness the awesomeness known as `postgresql`.  
The sqlpg versions of `sql.DB`, `sql.TX`, `sql.Stmt`, et al. all leave the 
underlying interfaces untouched, so that their interfaces are a superset 
on the standard ones.  This makes it relatively painless to integrate 
existing codebases using database/sql with sqlpg.

##Why sqlpg?

Go has superfast JSON mapping built-in and postgres is quickly becomming a JSON 
first class citizen.  So rather than trying to make an ORM-like experience, sqlpg 
aims to marry Go-Postgres-JSON.

This has many advantages, such as retrieving an entire object (struct) graph in 
a single query.

In addition to the JSON love, `database/sql` conforms to the lowest possible 
denominator, so `sql.DB` and friends have been proxied to better support transactions, 
savepoints, and a comming `sqlpg.Driver` interface so that data access functions can 
act on a reciever to the `sqlpg.Driver` interface allowing the same func to operate on
the connection regardless of transaction/prepared statement state. 

##Status

sqlpg is in flux, being developed to replace Rails/ActiveRecord stacks in current 
busy production deployments.  I'll post some examples of how sqlpg is making our 
lives better, but in the meantime if the idea catches your fancy please jump in 
and issues a pull request.

##Roadmap for v1.0
- [x] Conform sql.DB, sql.Tx and sql.Stmt to single interface (sqlpg.Driver)
- [x] Nested tranaction support with savepoints 
- [x] Query helpers, GetInt, GetString and Get(scanTo interface{} -- via json)
- [ ] Custom types to support timestamptz, hstore, point and other pg awesomeness
- [ ] Advanced query helpers GetSlice, GetMap
- [x] Select Query Helper (in progress, converts $1,$2 and sqlbuilder)
- [ ] Update Query Helper
- [ ] Insert Query Helper
- [x] Stored Procedure Helper
- [ ] Prepared statement cache
- [ ] Logging
- [ ] AutoExplain, PgStatStatment profile query usage/performance in realtime
- [ ] Fully tested

