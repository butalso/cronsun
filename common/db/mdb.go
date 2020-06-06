package db

var (
	mgoDB *Mdb
)

func GetDb() *Mdb {
	return mgoDB
}
