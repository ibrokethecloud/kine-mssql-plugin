package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/sqllog"
	"github.com/k3s-io/kine/pkg/tls"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/sirupsen/logrus"
)

/*
A valid plugin to be used by kine needs to implement the following method signatures

	Start(ctx context.Context) error
	Get(ctx context.Context, key string, revision int64) (int64, *KeyValue, error)
	Create(ctx context.Context, key string, value []byte, lease int64) (int64, error)
	Delete(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*KeyValue, error)
	Count(ctx context.Context, prefix string) (int64, int64, error)
	Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error)
	Watch(ctx context.Context, key string, revision int64) <-chan []*Event
	New(context.Context, string, string, tls.Config, generic.ConnectionPoolConfig) error
}

In addition the plugin should expose a variable ExternalDriver which implements these methods
*/
const (
	defaultMaxIdleConns = 2 // copied from database/sql
)

var (
	schema = []string{
		`if not exists (SELECT * FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_NAME = N'kine')
		begin
 			create table kine (
				id int primary key identity (1, 1),
				name varchar(630),
				created int,
				deleted int,
				create_revision int,
				prev_revision int,
				lease int,
				value varbinary(max),
				old_value varbinary(max) )
		end 
		`,
		`if not exists ( select * from sys.indexes
			where name = 'kine_name_index' and
			object_id = OBJECT_ID('kine')) 
		begin
    		create nonclustered index kine_name_index on kine (name)
		end
		`,
		`if not exists (
			select *
			from sys.indexes
			where name = 'kine_name_prev_revision_uindex' and
			object_id = OBJECT_ID('kine')
		) begin
		create unique index kine_name_prev_revision_uindex on kine (name, prev_revision)
		end
		`,
	}
	createDB = "create database "
	columns  = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"
	revSQL   = `
		SELECT TOP 1 rkv.id
		FROM kine rkv
		ORDER BY rkv.id
		DESC`
	compactRevSQL = `
		SELECT TOP 1 crkv.prev_revision
		FROM kine crkv
		WHERE crkv.name = 'compact_rev_key'
		ORDER BY crkv.id DESC`

	idOfKey = `
		AND mkv.id <= ? AND mkv.id > (
			SELECT TOP 1 ikv.id
			FROM kine ikv
			WHERE
				ikv.name = ? AND
				ikv.id <= ?
			ORDER BY ikv.id DESC )`

	listSQL = fmt.Sprintf(`SELECT TOP 100 PERCENT (%s)[a], (%s)[b], %s
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine mkv
			WHERE
				mkv.name LIKE ?
				%%s
			GROUP BY mkv.name) maxkv
	    ON maxkv.id = kv.id
		WHERE
			  ( kv.deleted = 0 OR 'true' = ? )
		ORDER BY kv.id ASC
		`, revSQL, compactRevSQL, columns)
)

func New(ctx context.Context, dataSourceName string, tlsInfo tls.Config,
	connectionPool generic.ConnectionPoolConfig) (backend server.Backend, err error) {
	parsedDSN, err := prepareDSN(dataSourceName, tlsInfo)
	backend = &logstructured.LogStructured{}
	if err != nil {
		return backend, err
	}

	if err := createDBIfNotExist(parsedDSN); err != nil {
		return backend, err
	}
	dialect, err := setupGenericDriver(ctx, "sqlserver", parsedDSN, "@p", connectionPool, true)
	if err != nil {
		return backend, err
	}

	dialect.TranslateErr = func(err error) error {
		// Need to verify msqql error code for unique constraint violation
		if err, ok := err.(mssql.Error); ok && err.Number == 2627 {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.CompactSQL = `
		DELETE FROM kine AS kv
		USING	(
			SELECT kp.prev_revision AS id
			FROM kine AS kp
			WHERE
				kp.name != 'compact_rev_key' AND
				kp.prev_revision != 0 AND
				kp.id <= @p1
			UNION
			SELECT kd.id AS id
			FROM kine AS kd
			WHERE
				kd.deleted != 0 AND
				kd.id <= @p2
		) AS ks
		WHERE kv.id = ks.id`
	if err := setup(dialect.DB); err != nil {
		return backend, err
	}

	dialect.Migrate(context.Background())
	backend = logstructured.New(sqllog.New(dialect))
	return backend, nil
}

func setupGenericDriver(ctx context.Context, driverName, dataSourceName string, paramCharacter string, connectionPool generic.ConnectionPoolConfig,
	numbered bool) (*generic.Generic, error) {
	var (
		db  *sql.DB
		err error
	)

	for i := 0; i < 300; i++ {
		db, err = OpenAndTest(driverName, dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	configureConnectionPooling(connectionPool, db, driverName)
	return &generic.Generic{
		DB: db,
		GetRevisionSQL: QueryBuilder(fmt.Sprintf(`
			SELECT
			0, 0, %s
			FROM kine kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        QueryBuilder(fmt.Sprintf(listSQL, ""), paramCharacter, numbered),
		ListRevisionStartSQL: QueryBuilder(fmt.Sprintf(listSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:  QueryBuilder(fmt.Sprintf(listSQL, idOfKey), paramCharacter, numbered),

		CountSQL: QueryBuilder(fmt.Sprintf(`
			SELECT (%s), COUNT(c.theid)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "")), paramCharacter, numbered),

		AfterSQL: QueryBuilder(fmt.Sprintf(`
			SELECT (%s), (%s), %s
			FROM kine kv
			WHERE
				kv.name LIKE ? AND
				kv.id > ?
			ORDER BY kv.id ASC`, revSQL, compactRevSQL, columns), paramCharacter, numbered),

		DeleteSQL: QueryBuilder(`
			DELETE FROM kine
			WHERE id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: QueryBuilder(`
			UPDATE kine
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: QueryBuilder(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?);select SCOPE_IDENTITY()`, paramCharacter, numbered),

		InsertSQL: QueryBuilder(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?); select SCOPE_IDENTITY()`, paramCharacter, numbered),

		FillSQL: QueryBuilder(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			values(?, ?, ?, ?, ?, ?, ?, ?, ?);select SCOPE_IDENTITY()`, paramCharacter, numbered),
	}, err

}

func setup(db *sql.DB) error {
	for _, stmt := range schema {
		_, err := db.Exec(stmt)
		if err != nil {
			return err
		}
	}

	return nil
}

func generateConnector(dataSourceName string) (*mssql.Connector, error) {
	conn, err := mssql.NewConnector(dataSourceName)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func createDBIfNotExist(dataSourceName string) error {
	u, err := url.Parse(dataSourceName)
	if err != nil {
		return err
	}

	dbName := u.Query().Get("database")
	db, err := sql.Open("sqlserver", dataSourceName)
	if err != nil {
		return err
	}
	defer db.Close()

	err = db.Ping()

	if _, ok := err.(mssql.Error); !ok {
		return err
	}

	if err := err.(mssql.Error); err.Number != 1801 { // 1801 = database already exists
		db, err := sql.Open("sqlserver", u.String())
		if err != nil {
			return err
		}
		defer db.Close()
		_, err = db.Exec(createDB + dbName + ":")
		if err != nil {
			return err
		}
	}
	return nil
}

func prepareDSN(dataSourceName string, tlsInfo tls.Config) (string, error) {
	if len(dataSourceName) == 0 {
		return "", fmt.Errorf("invalid dsn")
	} else {
		dataSourceName = "sqlserver://" + dataSourceName
	}

	u, err := url.Parse(dataSourceName)
	if err != nil {
		return "", err
	}

	queryMap := u.Query()
	params := url.Values{}

	if _, ok := queryMap["certificate"]; tlsInfo.CertFile != "" && !ok {
		params.Add("certificate", tlsInfo.CAFile)
	}

	if _, ok := queryMap["database"]; !ok {
		params.Add("database", "kubernetes")
	}

	for k, v := range queryMap {
		params.Add(k, v[0])
	}

	u.RawQuery = params.Encode()
	return u.String(), nil
}

func QueryBuilder(sql, param string, numbered bool) string {
	if param == "?" && !numbered {
		return sql
	}

	regex := regexp.MustCompile(`\?`)
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		if numbered {
			n++
			return param + strconv.Itoa(n)
		}
		return param
	})
}

func OpenAndTest(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func configureConnectionPooling(connPoolConfig generic.ConnectionPoolConfig, db *sql.DB, driverName string) {
	// behavior copied from database/sql - zero means defaultMaxIdleConns; negative means 0
	if connPoolConfig.MaxIdle < 0 {
		connPoolConfig.MaxIdle = 0
	} else if connPoolConfig.MaxIdle == 0 {
		connPoolConfig.MaxIdle = defaultMaxIdleConns
	}

	logrus.Infof("Configuring %s database connection pooling: maxIdleConns=%d, maxOpenConns=%d, connMaxLifetime=%s", driverName, connPoolConfig.MaxIdle, connPoolConfig.MaxOpen, connPoolConfig.MaxLifetime)
	db.SetMaxIdleConns(connPoolConfig.MaxIdle)
	db.SetMaxOpenConns(connPoolConfig.MaxOpen)
	db.SetConnMaxLifetime(connPoolConfig.MaxLifetime)
}
