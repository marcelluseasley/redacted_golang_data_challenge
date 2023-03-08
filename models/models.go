package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

const DBCONN = "postgres://postgres:postgres@localhost:5432/process_db?sslmode=disable"
const deviceEventsTable = "device_events"

type DeviceData struct {
	Device    string    `json:"device"`
	Generated *string   `json:"generated,omitempty"`
	Heading   *int64    `json:"heading,omitempty"`
	Position  *Position `json:"position,omitempty"`
	Speed     *float64  `json:"speed,omitempty"`
}
type Position struct {
	Lat  float64 `json:"lat,omitempty"`
	Long float64 `json:"long,omitempty"`
}

func (po Position) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"lat": %f, "long": %f}`, po.Lat, po.Long)), nil
}

func (po *Position) UnmarshalJSON(b []byte) error {

	temp := struct {
		Lat  any `json:"lat,omitempty"`
		Long any `json:"long,omitempty"`
	}{}

	err := json.Unmarshal(b, &temp)
	if err != nil {
		return err
	}
	var fLat, fLong float64
	switch temp.Lat.(type) {
	case string:
		sLat := temp.Lat.(string)
		sLong := temp.Long.(string)
		fLat, err = strconv.ParseFloat(sLat, 64)
		if err != nil {
			log.Printf("unable to parse float: %v", err)
		}
		fLong, err = strconv.ParseFloat(sLong, 64)
		if err != nil {
			log.Printf("unable to parse float: %v", err)
		}
	case float64:
		fLat = temp.Lat.(float64)
		fLong = temp.Long.(float64)
	}

	*po = Position{Lat: fLat, Long: fLong}
	return nil
}

type DeviceDataStore struct {
	DB *sql.DB
}

func (ds *DeviceDataStore) InitializeDB() {
	db, err := sql.Open("postgres", DBCONN)
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}
	ds.DB = db
	ds.createTable()
}

func (ds *DeviceDataStore) createTable() {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := ds.DB.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s(
			device text, 
			generatedts timestamp, 
			speed numeric, 
			heading integer, 
			latitude numeric, 
			longitude numeric)`, deviceEventsTable))
	if err != nil {
		log.Panicf("error creating `%s` table: %v", deviceEventsTable, err)
		
	}
}

func (ds *DeviceDataStore) AddEvent(dd *DeviceData) {

	query := fmt.Sprintf("INSERT INTO %s(device, generatedts, speed, heading, latitude, longitude) VALUES ($1,$2,$3,$4,$5,$6)", deviceEventsTable)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	stmt, err := ds.DB.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("error preparing SQL statement: %v", err)
	}
	defer stmt.Close()

	floatLat, floatLong := handleNilPositionFloat64(dd)
	_, err = stmt.ExecContext(ctx, dd.Device, dd.Generated, handleNilFloat64(dd.Speed), handleNilInt64(dd.Heading), floatLat, floatLong)
	if err != nil {
		log.Printf("Error inserting row into %s table: %v", deviceEventsTable, err)
	}
}

func (ds *DeviceDataStore) UpdateState(dd *DeviceData) {
	stmt := fmt.Sprintf("UPDATE %s SET speed = $2, heading = $3, latitude = $4, longitude = $5 WHERE device = $1;", deviceEventsTable)

	floatLat, floatLong := handleNilPositionFloat64(dd)
	_, err := ds.DB.Exec(stmt, dd.Device, handleNilFloat64(dd.Speed), handleNilInt64(dd.Heading), floatLat, floatLong)
	if err != nil {
		log.Printf("Error updating row in device_states table: %v", err)
	}
}

func (ds *DeviceDataStore) GetData(deviceData DeviceData) (DeviceData, bool) {
	stmt := fmt.Sprintf(`SELECT * FROM %s WHERE device = '%s' ORDER BY generatedts desc LIMIT 1`, deviceEventsTable, deviceData.Device)
	dd := DeviceData{
		Position: &Position{},
	}

	var lat float64
	var long float64

	row := ds.DB.QueryRow(stmt)
	err := row.Scan(&dd.Device, &dd.Generated, &dd.Speed, &dd.Heading, &lat, &long)
	switch err {
	case sql.ErrNoRows:
		ds.AddEvent(&deviceData)
		return deviceData, true
	}

	// if lat and long are both zero (Atlantic Ocean), its safe to assume this is null
	if lat == 0 && long == 0 {
		dd.Position = nil
	} else {
		dd.Position.Lat = lat
		dd.Position.Long = long
	}
	return dd, false
}

func Translate(oldData *DeviceData, newData *DeviceData) *DeviceData {
	newDataTime, _ := time.Parse(time.DateTime, *newData.Generated) //from incoming JSON
	oldDataTime, _ := time.Parse(time.RFC3339, *oldData.Generated)  //from Postgres
	// maybe its possible for duplicate data to be sent (timestamp)?
	if oldData.Generated == newData.Generated {
		return newData
	}

	//check heading
	if newData.Heading == nil && oldData.Heading != nil {
		newData.Heading = oldData.Heading
	} else if newData.Heading != nil && oldData.Heading != nil && newDataTime.Before(oldDataTime) {
		newData.Heading = oldData.Heading
	}

	//check speed
	if newData.Speed == nil && oldData.Speed != nil {
		newData.Speed = oldData.Speed
	} else if newData.Speed != nil && oldData.Speed != nil && newDataTime.Before(oldDataTime) {
		newData.Speed = oldData.Speed
	}
	//check lat/long
	if newData.Position == nil && oldData.Position != nil {
		newData.Position = &Position{
			Lat:  oldData.Position.Lat,
			Long: oldData.Position.Long,
		}
	} else if newData.Position != nil && oldData.Position != nil && newDataTime.Before(oldDataTime) {
		newData.Position = oldData.Position
	}
	//correct Generated, since "newData" is the "latest" to update the table
	if newDataTime.Before(oldDataTime) {
		odt := oldDataTime.Format(time.DateTime)
		newData.Generated = &odt
	}
	return newData
}

func handleNilInt64(i *int64) sql.NullInt64 {
	if i == nil {
		return sql.NullInt64{}
	}
	return sql.NullInt64{
		Int64: *i,
		Valid: true,
	}
}

func handleNilFloat64(f *float64) sql.NullFloat64 {
	if f == nil {
		return sql.NullFloat64{}
	}
	return sql.NullFloat64{
		Float64: *f,
		Valid:   true,
	}
}

func handleNilPositionFloat64(dd *DeviceData) (sql.NullFloat64, sql.NullFloat64) {
	if dd.Position == nil {
		return sql.NullFloat64{}, sql.NullFloat64{}
	}
	return sql.NullFloat64{
			Float64: dd.Position.Lat,
			Valid:   true,
		}, sql.NullFloat64{
			Float64: dd.Position.Long,
			Valid:   true,
		}
}
