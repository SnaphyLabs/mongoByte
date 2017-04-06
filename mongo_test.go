package mongoByte

import (
	"testing"
	"gopkg.in/mgo.v2"
	"time"
	"log"
	"github.com/SnaphyLabs/SnaphyByte/models"
	"github.com/icrowley/fake"
	"golang.org/x/net/context"
	"encoding/json"
	"fmt"
	"github.com/SnaphyLabs/SnaphyByte/resource"
	"github.com/SnaphyLabs/SnaphyByte/schema"
)


const (
	MongoDBHosts = "localhost:27017"
	AuthDatabase = "drugcorner"
	AuthUserName = "robins"
	AuthPassword = "12345"
	Collection = "SnaphyModelDefinition"

	BOOK_TYPE = "book"
	AUTHOR_TYPE = "author"
)

var (
	handler Handler
	book1 *models.BaseModel
	book2 *models.BaseModel
	book3 *models.BaseModel
	book4 *models.BaseModel
	//Author models
	author1 *models.BaseModel
	author2 *models.BaseModel
	author3 *models.BaseModel
	ctx          context.Context
)



func init()  {
	// We need this object to establish a session to our MongoDB.
	mongoDBDialInfo := &mgo.DialInfo{
		Addrs:    []string{MongoDBHosts},
		Timeout:  60 * time.Second,
		Database: AuthDatabase,
		Username: AuthUserName,
		Password: AuthPassword,
	}

	// Create a session which maintains a pool of socket connections
	// to our MongoDB.
	mongoSession, err := mgo.DialWithInfo(mongoDBDialInfo)
	if err != nil {
		log.Fatalf("CreateSession: %s\n", err)
	}

	//Get a handler for handling data..
	handler = NewHandler(mongoSession, AuthDatabase, Collection)
	author1 = &models.BaseModel{
		Payload: map[string]interface{}{
			"firstName": fake.FirstName(),
			"lastName": fake.LastName(),
			"email": fake.EmailAddress(),
			"password": fake.SimplePassword(),
			"userName": fake.UserName(),
			"age": fake.DigitsN(2),
		},
	}

	author2 = &models.BaseModel{
		Payload: map[string]interface{}{
			"firstName": fake.FirstName(),
			"lastName": fake.LastName(),
			"email": fake.EmailAddress(),
			"password": fake.SimplePassword(),
			"userName": fake.UserName(),
			"age": fake.DigitsN(2),
		},
	}

	author3 = &models.BaseModel{
		Payload: map[string]interface{}{
			"firstName": fake.FirstName(),
			"lastName": fake.LastName(),
			"email": fake.EmailAddress(),
			"password": fake.SimplePassword(),
			"userName": fake.UserName(),
			"age": fake.DigitsN(2),
		},
	}

	book1 = &models.BaseModel{
		Payload:map[string]interface{}{
			"name": fake.Product(),
			"pages": fake.DigitsN(3),
			//"authorId":
			"price": fake.DigitsN(4),
			//"description": fake.Paragraphs(),
		},
	}

	book2 = &models.BaseModel{
		Payload:map[string]interface{}{
			"name": fake.Product(),
			"pages": fake.DigitsN(3),
			//"authorId":
			"price": fake.DigitsN(4),
			//"description": fake.Paragraphs(),
		},
	}

	book3 = &models.BaseModel{
		Payload:map[string]interface{}{
			"name": fake.Product(),
			"pages": fake.DigitsN(3),
			//"authorId":
			"price": fake.DigitsN(4),
			//"description": fake.Paragraphs(),
		},
	}

	book4 = &models.BaseModel{
		Payload:map[string]interface{}{
			"name": fake.Product(),
			"pages": fake.DigitsN(3),
			//"authorId":
			"price": fake.DigitsN(4),
			"description": fake.Paragraphs(),
		},
	}



}


///Test for mongodb insert query..
func TestInsert(t *testing.T)  {
	fmt.Print("Testing for insert")
	//Now save authors..data..
	author3.NewModel(AUTHOR_TYPE)

	_, err := json.Marshal(author1)
	if err != nil{
		t.Error(err)
	}else{
		ctx = context.Background()
		//Now save the model..
		err = handler.Insert(ctx, []*models.BaseModel{author1, author2})
		if err != nil{
			t.Error(err)
		}else{
			//Now save the
			book1.NewModel(BOOK_TYPE)


			book1.Payload["authorId"] = author1.ID
			book2.Payload["authorId"] = author2.ID
			book3.Payload["authorId"] = author1.ID
			book4.Payload["authorId"] = author2.ID
			//Now save the model..
			err := handler.Insert(ctx, []*models.BaseModel{book1, book2, book3, book4})
			if err != nil{
				t.Error(err)
			}
		}
	}
}



func TestForFindingUpdateAPortionLargeThan16Mb(t *testing.T){
	fmt.Print("Testing for insert")
	//Now save authors..data..
	author3.NewModel(AUTHOR_TYPE)

	_, err := json.Marshal(author3)
	if err != nil{
		t.Error(err)
	}else{
		ctx = context.Background()
		//Now save the model..
		err = handler.Insert(ctx, []*models.BaseModel{author3})
		if err != nil{
			t.Error(err)
		}else{
			//Adding data till mongodb memory exausts..
			for true{
				book := &models.BaseModel{
					Payload: map[string]interface{}{
						"name":  fake.Product(),
						"pages": fake.DigitsN(3),
						//"authorId":
						"price": fake.DigitsN(4),
						"description": fake.Paragraphs(),
						"anotherDescription": fake.Paragraphs(),
						"anotherDescription1": fake.Paragraphs(),
						"anotherDescription2": fake.Paragraphs(),
						"anotherDescription3": fake.Paragraphs(),
						"anotherDescription4": fake.Paragraphs(),
						"anotherDescription5": fake.Paragraphs(),
						"anotherDescription6": fake.Paragraphs(),
						"anotherDescription7": fake.Paragraphs(),
						"anotherDescription8": fake.Paragraphs(),
						"anotherDescription9": fake.Paragraphs(),
						"anotherDescription10": fake.Paragraphs(),
					},
				}

				//Now save the
				book.NewModel(BOOK_TYPE)
				//author3.Payload[book.ID] = book
				key := "Book." + string(book.ID)
				author3.Payload[key] = book
				//Now save the model..
				err := handler.Insert(ctx, []*models.BaseModel{book1, book2, book3, book4})
				if err != nil{
					t.Error(err)
				}else{
					fmt.Println("Data Updated")
				}
			}

		}
	}
}


//TODO: Test for robustness and speed...

func TestFind(t *testing.T)  {
	ctx = context.Background()
	lookup := &resource.Lookup{}

	//Preparing a raw query..
	rawQuery1 := map[string]interface{}{
		"collection": "author",
		"firstName": "Lisa",
	}

	//Preparing a raw query..
	rawQuery2 := map[string]interface{}{
		"collection": "book",
		//"firstName": "Jesse",
	}

	//Preparing a raw query..for finding book and author.. and $and query tested..
	bookAndAuthor1 := map[string]interface{}{
		"$or": []interface{}{
			rawQuery1,
			rawQuery2,
		},
	}


	query, err := schema.NewQuery(rawQuery1)
	if err != nil{
		t.Error(err)
	}

	lookup.AddQuery(query)

	if l, err := handler.Find(ctx, lookup, 0, 1); err != nil{
		t.Error(err)
	}else{
		j, err := json.Marshal(l.Models)
		if err != nil {
			t.Error(err)
		}else{
			fmt.Println(string(j))
		}
	}


	bookAndAuthorQuery, err := schema.NewQuery(bookAndAuthor1)
	/*if err != nil{
		t.Error(err)
	}else{
		j, err := json.Marshal(bookAndAuthorQuery)
		if err != nil {
			t.Error(err)
		}else{
			fmt.Println("Query print format")
			fmt.Println(string(j))
		}
	}*/

	lookup1 := &resource.Lookup{}
	lookup1.AddQuery(bookAndAuthorQuery)

	//Now translate query..
	if q, err := translateQuery(bookAndAuthorQuery); err != nil{
		t.Error("Unable to translate query")
	}else{
		j, err := json.Marshal(q)
		if err != nil {
			t.Error(err)
		}else{
			fmt.Println("Translated Query for books and author")
			fmt.Println(string(j))
		}
	}


	if l, err := handler.Find(ctx, lookup1, 0, 3); err != nil{
		t.Error(err)
	}else{
		j, err := json.Marshal(l.Models)
		if err != nil {
			t.Error(err)
		}else{
			fmt.Println("Printing books and author")
			fmt.Println(string(j))
		}
	}
}
