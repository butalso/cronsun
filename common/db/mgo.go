package db

import (
	"github.com/butalso/cronsun/common/conf"
	"gopkg.in/mgo.v2"
	"net/url"
	"strings"

	"gopkg.in/mgo.v2/bson"
)

type Mdb struct {
	*conf.MgoConfig
	*mgo.Session
}

func NewMdb(c *conf.MgoConfig) (*Mdb, error) {
	m := &Mdb{
		MgoConfig: c,
	}
	return m, m.connect()
}

func (m *Mdb) connect() error {
	// connectionString: [mongodb://][user:pass@]host1[:port1][,host2[:port2],...][/database][?options]
	// via: https://docs.mongodb.com/manual/reference/connection-string/
	connectionString := strings.Join(m.MgoConfig.Hosts, ",")
	if len(m.MgoConfig.UserName) > 0 && len(m.MgoConfig.Password) > 0 {
		connectionString = m.MgoConfig.UserName + ":" + url.QueryEscape(m.MgoConfig.Password) + "@" + connectionString
	}

	if len(m.MgoConfig.Database) > 0 {
		connectionString += "/" + m.MgoConfig.Database
	}

	if len(m.MgoConfig.AuthSource) > 0 {
		connectionString += "?authSource=" + m.MgoConfig.AuthSource
	}

	session, err := mgo.DialWithTimeout(connectionString, m.MgoConfig.Timeout)
	if err != nil {
		return err
	}

	m.Session = session
	return nil
}

func (m *Mdb) WithC(collection string, job func(*mgo.Collection) error) error {
	s := m.Session.New()
	err := job(s.DB(m.MgoConfig.Database).C(collection))
	s.Close()
	return err
}

func (self *Mdb) Upsert(collection string, selector interface{}, change interface{}) error {
	return self.WithC(collection, func(c *mgo.Collection) error {
		_, err := c.Upsert(selector, change)
		return err
	})
}

func (self *Mdb) Insert(collection string, data ...interface{}) error {
	return self.WithC(collection, func(c *mgo.Collection) error {
		return c.Insert(data...)
	})
}

func (self *Mdb) FindId(collection string, id interface{}, result interface{}) error {
	return self.WithC(collection, func(c *mgo.Collection) error {
		return c.Find(bson.M{"_id": id}).One(result)
	})
}

func (self *Mdb) FindOne(collection string, query interface{}, result interface{}) error {
	return self.WithC(collection, func(c *mgo.Collection) error {
		return c.Find(query).One(result)
	})
}

func (self *Mdb) RemoveId(collection string, id interface{}) error {
	return self.WithC(collection, func(c *mgo.Collection) error {
		return c.RemoveId(id)
	})
}
