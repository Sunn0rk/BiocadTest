package main

import (
	"container/list"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/gocarina/gocsv"
)

func DatabaseConnect() (*sql.DB, error) {
	connStr := fmt.Sprintf("%s://%s:%s@%s:%s/%s?sslmode=disable", database, username, password, server, port, DatabaseName)
	db, err := sql.Open(driverName, connStr)
	if err != nil {
		fmt.Println("Error is DatabaseConnect")
		return db, err
	}
	return db, nil
}

func CreateTable(db *sql.DB, tablename string) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
		mqtt text,
		invid text,
		unit_guid text,        
		msg_id text,
		text text,               
		context text,
		class text,
		level text,
		area text,
		addr text,  
		block text,
		type text,
		bit text,
		invert_bit text
	);`, tablename)

	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func CreateErrorTable(db *sql.DB, tablename string) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
		fileName text,
		error text
	);`, tablename)

	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func Worker(fileName string) {
	queryRows := []*QueryRow{}

	queryRows = Parser(queryRows, fileName)

	for _, row := range queryRows {
		*row = DeleteSpace(*row)
		fmt.Println(row)
		AddData(db, tablename, row)
		address = fmt.Sprintf("%s%s", DirectoryOutFile, row.Unit_guid)
		CreateFileUnit_guid(address, row)
	}
}

func AddData(db *sql.DB, tablename string, queryRows *QueryRow) {

	query := fmt.Sprintf(`INSERT INTO %s(mqtt, invid, unit_guid, msg_id, text, context, class, level, area, addr, block, type, bit, invert_bit)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`, tablename)

	_, err := db.Exec(query, queryRows.Mqtt, queryRows.Invid, queryRows.Unit_guid, queryRows.Msg_id, queryRows.Text, queryRows.Context,
		queryRows.Class, queryRows.Level, queryRows.Area, queryRows.Addr, queryRows.Block, queryRows.Type, queryRows.Bit, queryRows.Invert_bit)

	if err != nil {
		log.Fatal(err)
	}
}

func AddErrorData(db *sql.DB, tablename string, erro error, filename string) {

	query := fmt.Sprintf(`INSERT INTO %s(fileName, error)
		VALUES ($1, $2)`, tablename)

	_, err := db.Exec(query, filename, erro.Error())

	if err != nil {
		log.Fatal(err)
	}
}

func CreateFileUnit_guid(Addres string, queryRows *QueryRow) {
	Name := fmt.Sprintf("%s.txt", Addres)

	if _, err := os.Stat(Name); errors.Is(err, os.ErrNotExist) {
		file, _ := os.Create(Name)
		defer file.Close()

		data := queryRows.PrintData()
		file.WriteString(data)
	} else {
		var file_text string
		file, _ := os.Open(Name)
		defer file.Close()
		databytes := make([]byte, 512)
		for {
			n, err := file.Read(databytes)
			if err == io.EOF {
				break
			}
			file_text = fmt.Sprintf("%s%s", file_text, string(databytes[:n]))
		}
		data := queryRows.PrintData()

		file, _ = os.Create(Name)
		file.WriteString(file_text + data)
	}
}

func Parser(queryRows []*QueryRow, fileName string) []*QueryRow {
	in, err := os.Open(DirectoryOfQueuedFiles + fileName)

	if err != nil {
		log.Println(err)

		AddErrorData(db, errortablename, err, fileName)

		in.Close()
		err = os.Rename(DirectoryOfQueuedFiles+fileName, DyrectoryErrorFiles+fileName)

		if err != nil {
			log.Println(err)
		}

		return nil
	}

	defer in.Close()

	gocsv.SetCSVReader(func(in io.Reader) gocsv.CSVReader {
		r := csv.NewReader(in)
		r.Comma = '\t'
		return r
	})

	if err := gocsv.UnmarshalFile(in, &queryRows); err != nil {
		log.Println(err)

		AddErrorData(db, errortablename, err, fileName)

		in.Close()
		err = os.Rename(DirectoryOfQueuedFiles+fileName, DyrectoryErrorFiles+fileName)

		if err != nil {
			log.Println(err)
		}

		return nil
	}

	return queryRows
}

func DeleteSpace(row QueryRow) QueryRow {
	row.N = strings.TrimSpace(row.N)
	row.Mqtt = strings.TrimSpace(row.Mqtt)
	row.Invid = strings.TrimSpace(row.Invid)
	row.Unit_guid = strings.TrimSpace(row.Unit_guid)
	row.Msg_id = strings.TrimSpace(row.Msg_id)
	row.Text = strings.TrimSpace(row.Text)
	row.Context = strings.TrimSpace(row.Context)
	row.Class = strings.TrimSpace(row.Class)
	row.Level = strings.TrimSpace(row.Level)
	row.Area = strings.TrimSpace(row.Area)
	row.Addr = strings.TrimSpace(row.Addr)
	row.Block = strings.TrimSpace(row.Block)
	row.Type = strings.TrimSpace(row.Type)
	row.Bit = strings.TrimSpace(row.Bit)
	row.Invert_bit = strings.TrimSpace(row.Invert_bit)
	return row
}

func finder() *list.List {
	queue := list.New()
	dir, err := os.Open(DirectoryInputFile)
	if err != nil {
		log.Println(err)
	}
	defer dir.Close()

	files, err := dir.Readdir(-1)
	if err != nil {
		log.Println(err)
	}

	for _, file := range files {
		queue.PushBack(file.Name())
		err = os.Rename(DirectoryInputFile+file.Name(), DirectoryOfQueuedFiles+file.Name())
		if err != nil {
			log.Println(err)
		}
	}

	return queue
}

func (r *APIQueryRow1) returns(db *sql.DB, tablename string, Unit_guid string) {
	data := []APIQueryRow1{}
	query := fmt.Sprintf("SELECT invid, msg_id, text FROM public.%s WHERE unit_guid = '%s'", tablename, Unit_guid)
	rows, err := db.Query(query)

	if err != nil {
		log.Println(err)
	}

	defer rows.Close()

	var invid string
	var msg_id string
	var text string

	for rows.Next() {
		err := rows.Scan(&invid, &msg_id, &text)
		if err != nil {
			log.Println(err)
		}
		data = append(data, APIQueryRow1{invid, msg_id, text})
	}

	var return_data string

	for _, row := range data {
		return_data = fmt.Sprintf("%s\n%s", return_data, row)
	}

	fmt.Println(return_data)
}

func (r *APIQueryRow2) returns(db *sql.DB, tablename string, Unit_guid string) {
	data := []APIQueryRow2{}
	query := fmt.Sprintf("SELECT invid, msg_id, text, class, level, area, addr FROM public.%s WHERE unit_guid = '%s'", tablename, Unit_guid)
	rows, err := db.Query(query)
	
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()

	var invid     string 
	var msg_id    string
	var text      string
	var class     string
	var level     string
	var area      string
	var addr      string

	for rows.Next() {
		err := rows.Scan(&invid, &msg_id, &text, &class, &level, &area, &addr)
		if err != nil {
			log.Println(err)
		}
		data = append(data, APIQueryRow2{invid, msg_id, text, class, level, area, addr})
	}

	var return_data string

	for _, row := range data {
		return_data = fmt.Sprintf("%s\n%s", return_data, row)
	}

	fmt.Println(return_data)
}

func (r *APIQueryRow3) returns(db *sql.DB, tablename string, Unit_guid string) {
	data := []APIQueryRow3{}
	query := fmt.Sprintf("SELECT text FROM public.%s WHERE unit_guid = '%s'", tablename, Unit_guid)
	rows, err := db.Query(query)
	
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()

	var text string

	for rows.Next() {
		err := rows.Scan(&text)
		if err != nil {
			log.Println(err)
		}
		data = append(data, APIQueryRow3{text})
	}

	var return_data string

	for _, row := range data {
		return_data = fmt.Sprintf("%s\n%s", return_data, row)
	}

	fmt.Println(return_data)
}

func (r *QueryRow) PrintData() string {
	return fmt.Sprintf("mqtt = %s;\ninvid = %s;\nmsg_id = %s;\ntext = %s;\ncontext = %s;\nclass = %s;\nlevel = %s;\narea = %s;\naddr = %s;\nblock = %s;\ntype = %s;\nbit = %s;\ninvert_bit = %s;\n\n",
		r.Mqtt, r.Invid, r.Msg_id, r.Text, r.Context, r.Class, r.Level, r.Area, r.Addr, r.Block, r.Type, r.Bit, r.Invert_bit)
}
