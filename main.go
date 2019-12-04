package main
import (
	"bufio"
    "fmt"
	"os"
	"encoding/csv"
	// "io"
	"strings"
	// "reflect"
	"log"
	"time"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/txn"
	"gopkg.in/mgo.v2/bson"
)

type Task struct {
	StartTime string `bson:"start_time"`
	EndTime string `bson:"end_time"`	
	Duration string `bson:"duration"`
	OrganizerID string `bson:"organizer_id"`
	AttendeesId string `bson:"attendees_id"`
}

var sess *mgo.Session
var collection *mgo.Collection
var sessUValName string
func setCollection(dbName string, collectionName string) *mgo.Collection {
	if sess == nil {
		fmt.Println("Not connected... Connecting to Mongo")
		sess = GetConnected()
	}
	collection = sess.DB(dbName).C(collectionName)
	return collection
}
// func for connection with mongodb
func GetConnected() *mgo.Session {
	dialInfo, err := mgo.ParseURL("mongodb://localhost:27017")
	dialInfo.Direct = true
	dialInfo.FailFast = true
	dialInfo.Database = "task_db"
	// dialInfo.Database = os.Getenv("Db_Name")
	// dialInfo.Username =  os.Getenv("User_Name") // username 
	// dialInfo.Password = 	db := os.Getenv("password") // password
	sess, err := mgo.DialWithInfo(dialInfo)
	if err != nil {
		fmt.Println("Can't connect to mongo, go error %v\n", err)
		panic(err)
	} else {
		return sess
		defer sess.Close()
	}
	return sess
}
// function to parse data from sample_task.env file and with the format saves to mongodb
func WriteData() {
	file , err := os.Open("./sample_task.csv")
	if err != nil {
		fmt.Println("error in csv ",err)
	}
	defer file.Close()
	// read file and stores in reads
	r := csv.NewReader(file)
	reads, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}else{
	// itareted loop overs rows and when rows length is 1 i.e records that records push help of struct
	for i, row := range reads {
	  if i == 1 {
		getId := []string{row[4]}
		substr := []rune(getId[0])
		FirstRecord := &Task{StartTime:row[0],EndTime:row[1],Duration:row[2],OrganizerID:row[3],AttendeesId:string(substr[0:5])}	
		SecondRecord := &Task{StartTime:time.Now().Format("02-01-2006 3:04:05 PM"),EndTime:time.Now().Format("02-01-2006 3:04:05 PM"),Duration:"10:20",OrganizerID:row[3],AttendeesId:string(substr[7:12])}		
		ThirdRecord := &Task{StartTime:time.Now().Format("02-01-2006 3:04:05 PM"),EndTime:time.Now().Format("02-01-2006 3:04:05 PM"),Duration:"09:20",OrganizerID:row[3],AttendeesId:string(substr[14:19])}	
		FourthRecord := &Task{StartTime:time.Now().Format("02-01-2006 3:04:05 PM"),EndTime:time.Now().Format("02-01-2006 3:04:05 PM"),Duration:"07:20",OrganizerID:row[3],AttendeesId:string(substr[21:26])}
		FifthRecord := &Task{StartTime:time.Now().Format("02-01-2006 3:04:05 PM"),EndTime:time.Now().Format("02-01-2006 3:04:05 PM"),Duration:"06:00",OrganizerID:row[3],AttendeesId:string(substr[28:33])}	
		SixthRecord := &Task{StartTime:time.Now().Format("02-01-2006 3:04:05 PM"),EndTime:time.Now().Format("02-01-2006 3:04:05 PM"),Duration:"08:00",OrganizerID:row[3],AttendeesId:string(substr[35:40])}
	// Transactions 
		runnner := txn.NewRunner(setCollection("task_db","transaction_collection"))
		ops := []txn.Op{{
			C : "task_collection",
			Id : bson.ObjectId(bson.NewObjectId()).Hex(),
			Insert : FirstRecord,
		},
		{
			C : "task_collection",
			Id : bson.ObjectId(bson.NewObjectId()).Hex(),
			Insert : SecondRecord,
		},
		{
			C : "task_collection",
			Id : bson.ObjectId(bson.NewObjectId()).Hex(),
			Insert : ThirdRecord,
		},
		{
			C : "task_collection",
			Id : bson.ObjectId(bson.NewObjectId()).Hex(),
			Insert : FourthRecord,
		},
		{
			C : "task_collection",
			Id : bson.ObjectId(bson.NewObjectId()).Hex(),
			Insert : FifthRecord,
		},
		{
			C : "task_collection",
			Id : bson.ObjectId(bson.NewObjectId()).Hex(),
			Insert : SixthRecord,
		},
	}
	id :=bson.NewObjectId()
	runnnerErr := runnner.Run(ops,id,nil)
	if runnnerErr != nil {
		fmt.Println("errr while inserting records",runnnerErr)
	}else{
		fmt.Println("all records of employee inserted")
	}
   }
  }
 }		
}
	
func ShowDataById() {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("enter Attendees ID to fetch record:")
	for scanner.Scan(){	
	fmt.Println("enter id",scanner.Text())
	text := scanner.Text()
	fmt.Println("before space remove:",text)
	fmt.Println("after sapcae remove",strings.TrimSpace(text)) 
	collection = setCollection("task_db","task_collection")
	taskIdStr := Task{}
	err := collection.Find(bson.M{"attendees_id" : strings.TrimSpace(text)}).One(&taskIdStr)
	if err != nil {
		fmt.Println("Employee with entered organizerID ",err)
		fmt.Println("enter Attendees ID to fetch record:")
	}else{
		m := map[string]interface{}{
			"StartTime":taskIdStr.StartTime,
			"EndTime":taskIdStr.EndTime,
			"Duretion":taskIdStr.Duration,
			"OrganizerID":taskIdStr.OrganizerID,
			"AttendeesId":taskIdStr.AttendeesId,
		}
		fmt.Println("data by id is ",m)
		fmt.Println("enter Attendees ID to fetch record:")
	 } 
	}
	if scanner.Err() != nil{
		fmt.Println("something went wring ....Try again")
	}
}
func main(){
	WriteData()	
	ShowDataById()
}