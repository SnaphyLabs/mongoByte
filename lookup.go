package mongoByte


import (
	"gopkg.in/mgo.v2/bson"
	"github.com/SnaphyLabs/SnaphyByte/resource"
	"github.com/SnaphyLabs/SnaphyByte/schema"
)

//Name of collection in mongo db..
const COLLECTION  = "_collection_type_byte";

// getField translate a schema field into a MongoDB field:
//
//  - id -> _id with in order to tape on the mongo primary key
func getField(f string) string {
	if f == "id" {
		return "_id"
	}
	return f
}

// getQuery transform a resource.Lookup into a Mongo query
func getQuery(l *resource.Lookup) (bson.M, error) {
	return translateQuery(l.Filter())
}

// getSort transform a resource.Lookup into a Mongo sort list.
// If the sort list is empty, fallback to _id.
func getSort(l *resource.Lookup) []string {
	ln := len(l.Sort())
	if ln == 0 {
		return []string{"_id"}
	}
	s := make([]string, ln)
	for i, sort := range l.Sort() {
		if len(sort) > 0 && sort[0] == '-' {
			s[i] = "-" + getField(sort[1:])
		} else {
			s[i] = getField(sort)
		}
	}
	return s
}




//Translate Query build in Byte Query into Schema Query.
func translateQuery(q schema.Query) (bson.M, error) {
	b := bson.M{}
	for _, exp := range q {
		switch t := exp.(type) {
		//Matching for collection type..
		case schema.COLLECTION:
			b[COLLECTION] = t.Value

		//Processing the Comparision Query Operators..
		case schema.Equal:
			b[getField(t.Field)] = t.Value
		case schema.GreaterThan:
			b[getField(t.Field)] = bson.M{"$gt": t.Value}
		case schema.GreaterOrEqual:
			b[getField(t.Field)] = bson.M{"$gte": t.Value}
		case schema.LowerThan:
			b[getField(t.Field)] = bson.M{"$lt": t.Value}
		case schema.LowerOrEqual:
			b[getField(t.Field)] = bson.M{"$lte": t.Value}
		case schema.NotEqual:
			b[getField(t.Field)] = bson.M{"$ne": t.Value}
		case schema.NotIn:
			b[getField(t.Field)] = bson.M{"$nin": t.Values}
		case schema.In:
			b[getField(t.Field)] = bson.M{"$in": t.Values}

		//LOGICAL OPERATOR..
		case schema.Or:
			//db.inventory.find( { $or: [ { quantity: { $lt: 20 } }, { price: 10 } ] } )
			//create a slice of bson
			s := []bson.M{}
			for _, subQuery := range t {
				sb, err := translateQuery(subQuery)
				if err != nil{
					return nil, err
				}
			 	s = append(s, sb)
			}
			b["$or"] = s
		case schema.And:
			s := []bson.M{}
			for _, subQuery := range t{
				sb, err := translateQuery(subQuery)
				if err != nil{
					return nil, err
				}
				s = append(s, sb)
			}
			b["$and"] = s

		case schema.Not:
			//db.inventory.find( { price: { $not: { $gt: 1.99 } } } )
			b[getField(t.Field)] = bson.M{"$not": t.Value}

		case schema.Nor:
			//db.inventory.find( { $nor: [ { price: 1.99 }, { sale: true } ]  } )
			s := []bson.M{}
			for _, subQuery := range t{
				sb, err := translateQuery(subQuery)
				if err != nil{
					return nil, err
				}
				s = append(s, sb)
			}

			b["$nor"] = s

		//Element Query Operator
		case schema.Exist:
			//db.inventory.find( { qty: { $exists: true, $nin: [ 5, 15 ] } } )
			b[getField(t.Field)] = bson.M{"$exists": t.Value}

		//Evaluation Query Operator
		case schema.Mod:
			//{ field: { $mod: [ divisor, remainder ] } }
			b[getField(t.Field)] = bson.M{"$mod": []float64{t.Divisor, t.Remainder}}

		case schema.Regex:
			//{ <field>: { $regex: 'pattern', $options: '<options>' } }
			b[getField(t.Field)] = bson.M{"$regex": t.Value.String()}
		case schema.Text:
			/*
			{
			  $text:
			    {
			      $search: <string>,
			      $language: <string>,
			      $caseSensitive: <boolean>,
			      $diacriticSensitive: <boolean>
			    }
			}
			*/

			s := bson.M{
				"$search": t.Search,
				"$language": t.Language,
				"$caseSensitive": t.CaseSensitive,
				"$diacriticSensitive": t.DiacriticSensitive,
			}

			b["$text"] = s
		default:
			return nil, resource.ErrNotImplemented
		}
	}
	return b, nil
}

func valuesToInterface(v []schema.Value) []interface{} {
	I := make([]interface{}, len(v))
	for i, _v := range v {
		I[i] = _v
	}
	return I
}


