package main

import (
	"container/list"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

type API interface {
	returns(*sql.DB, string, string)
}

type QueryRow struct {
	N          string `csv:"n"`
	Mqtt       string `csv:"mqtt"`
	Invid      string `csv:"invid"`
	Unit_guid  string `csv:"unit_guid"`
	Msg_id     string `csv:"msg_id"`
	Text       string `csv:"text"`
	Context    string `csv:"context"`
	Class      string `csv:"class"`
	Level      string `csv:"level"`
	Area       string `csv:"area"`
	Addr       string `csv:"addr"`
	Block      string `csv:"block"`
	Type       string `csv:"type"`
	Bit        string `csv:"bit"`
	Invert_bit string `csv:"invert_bit"`
}

type APIQueryRow1 struct {
	Invid  string `csv:"invid"`
	Msg_id string `csv:"msg_id"`
	Text   string `csv:"text"`
}

type APIQueryRow2 struct {
	Invid     string `csv:"invid"`
	Msg_id    string `csv:"msg_id"`
	Text      string `csv:"text"`
	Class     string `csv:"class"`
	Level     string `csv:"level"`
	Area      string `csv:"area"`
	Addr      string `csv:"addr"`
}
type APIQueryRow3 struct {
	Text      string `csv:"text"`
}

var db *sql.DB
var err error
var address string
var current_queue *list.List

func init() {

	db, err = DatabaseConnect()

	if err != nil {
		fmt.Println("Не удалось подключиться к базе данных.")
		log.Fatal(err)
	}

	err = CreateTable(db, tablename)

	if err != nil {
		fmt.Println("Таблица 1 не создалась")
		log.Fatal(err)
	}

	err = CreateErrorTable(db, errortablename)

	if err != nil {
		fmt.Println("Таблица 2 не создалась")
		log.Fatal(err)
	}

	current_queue = list.New()

}

func main() {
	for {
		adding_queue := finder()
		for adding_queue.Len() > 0 {
			e := adding_queue.Front()
			current_queue.PushBack(e.Value)
			adding_queue.Remove(e)
		}

		for current_queue.Len() > 0 {
			e := current_queue.Front()
			fileName := fmt.Sprintf("%s", e.Value)
			Worker(fileName)
			current_queue.Remove(e)
			err = os.Rename(DirectoryOfQueuedFiles+fileName, DirectoryOfProcessedFiles+fileName)
			if err != nil {
				log.Println(err)
			}
		}
		// var k API = &APIQueryRow1{}
		// k.returns(db, tablename, "01749246-95f6-57db-b7c3-2ae0e8be671f")
		time.Sleep(time.Second * delay)
	}

}
