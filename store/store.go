package store

import (
	"context"
	"fmt"
	"log"
	"sort"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	tpMongo "github.com/tidepool-org/go-common/clients/mongo"
)

const (
	usersCollectionName  = "users"
	tokensCollectionName = "tokens"
	userStoreAPIPrefix   = "api/user/store "
)

// Because the `users` collection already exists on all environments (especially `prd`),
// and MongoDB doesn't allow modification of default collation on an existing collection,
// we need to specify collation manually everywhere we generate an index, or make a query
// with the notable exception of the `_id` field
var usersCollation *options.Collation = &options.Collation{Locale: "en", Strength: 1}

// MongoStoreClient - Mongo Storage Client
type MongoStoreClient struct {
	client   *mongo.Client
	database string
}
type User struct {
	Id             string   `json:"userid,omitempty" bson:"userid,omitempty"` // map userid to id
	Username       string   `json:"username,omitempty" bson:"username,omitempty"`
	Emails         []string `json:"emails,omitempty" bson:"emails,omitempty"`
	Roles          []string `json:"roles,omitempty" bson:"roles,omitempty"`
	TermsAccepted  string   `json:"termsAccepted,omitempty" bson:"termsAccepted,omitempty"`
	EmailVerified  bool     `json:"emailVerified" bson:"authenticated"` //tag is name `authenticated` for historical reasons
	PwHash         string   `json:"-" bson:"pwhash,omitempty"`
	Hash           string   `json:"-" bson:"userhash,omitempty"`
	CreatedTime    string   `json:"createdTime,omitempty" bson:"createdTime,omitempty"`
	CreatedUserID  string   `json:"createdUserId,omitempty" bson:"createdUserId,omitempty"`
	ModifiedTime   string   `json:"modifiedTime,omitempty" bson:"modifiedTime,omitempty"`
	ModifiedUserID string   `json:"modifiedUserId,omitempty" bson:"modifiedUserId,omitempty"`
	DeletedTime    string   `json:"deletedTime,omitempty" bson:"deletedTime,omitempty"`
	DeletedUserID  string   `json:"deletedUserId,omitempty" bson:"deletedUserId,omitempty"`
}
// NewMongoStoreClient creates a new MongoStoreClient
func NewMongoStoreClient(config *tpMongo.Config) *MongoStoreClient {
	connectionString, err := config.ToConnectionString()
	log.Printf("The full connection String: %v", connectionString)
	if err != nil {
		log.Fatal(userStoreAPIPrefix, fmt.Sprintf("Invalid MongoDB configuration: %s", err))
	}

	clientOptions := options.Client().ApplyURI(connectionString)
	mongoClient, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		log.Fatal(userStoreAPIPrefix, fmt.Sprintf("Invalid MongoDB connection string: %s", err))
	}

	return &MongoStoreClient{
		client:   mongoClient,
		database: config.Database,
	}
}

func usersCollection(msc *MongoStoreClient) *mongo.Collection {
	log.Println(msc.database)
	return msc.client.Database(msc.database).Collection(usersCollectionName)
}

func tokensCollection(msc *MongoStoreClient) *mongo.Collection {
	return msc.client.Database(msc.database).Collection(tokensCollectionName)
}

// Ping the MongoDB database
func (msc *MongoStoreClient) Ping(ctx context.Context) error {
	// do we have a store session
	return msc.client.Ping(ctx, nil)
}

// Disconnect from the MongoDB database
func (msc *MongoStoreClient) Disconnect(ctx context.Context) error {
	return msc.client.Disconnect(ctx)
}

// UpsertUser - Update an existing user's details, or insert a new user if the user doesn't already exist.
func (msc *MongoStoreClient) UpsertUser(ctx context.Context, user *User) error {
	if user.Roles != nil {
		sort.Strings(user.Roles)
	}

	// if the user already exists we update otherwise we add
	opts := options.FindOneAndUpdate().SetUpsert(true).SetCollation(usersCollation)
	result := usersCollection(msc).FindOneAndUpdate(ctx, bson.M{"userid": user.Id}, bson.D{{Key: "$set", Value: user}}, opts)
	if result.Err() != mongo.ErrNoDocuments {
		return result.Err()
	}
	return nil
}

// FindUser - find and return an existing user
func (msc *MongoStoreClient) FindUser(ctx context.Context, id string) (result *User, err error) {
	if id != "" {
		opts := options.FindOne().SetCollation(usersCollation)
		log.Printf("Mongo Client: %v  Mongo database: %v ", msc.client, msc.database)
		if err = usersCollection(msc).FindOne(ctx, bson.M{"userid": id}, opts).Decode(&result); err != nil {
			return result, err
		}
	}

	return result, nil
}
// FindUsersWithIds - find and return multiple users by Tidepool User ID
func (msc *MongoStoreClient) FindUsersWithIds(ctx context.Context, ids []string) (results []*User, err error) {
	opts := options.Find().SetCollation(usersCollation)
	cursor, err := usersCollection(msc).Find(ctx, bson.M{"userid": bson.M{"$in": ids}}, opts)
	if err != nil {
		return nil, err
	}

	if err = cursor.All(ctx, &results); err != nil {
		return results, err
	}

	if results == nil {
		log.Printf("no users found: query: id: %v", ids)
		results = []*User{}
	}

	return results, nil
}
// FindUsers - find and return multiple existing users
func (msc *MongoStoreClient) FindUsers(ctx context.Context, user *User) (results []*User, err error) {
	fieldsToMatch := []bson.M{}

	if user.Id != "" {
		fieldsToMatch = append(fieldsToMatch, bson.M{"userid": user.Id})
	}
	if user.Username != "" {
		fieldsToMatch = append(fieldsToMatch, bson.M{"username": user.Username})
	}
	if len(user.Emails) > 0 {
		fieldsToMatch = append(fieldsToMatch, bson.M{"emails": bson.M{"$in": user.Emails}})
	}

	if len(fieldsToMatch) == 0 {
		return []*User{}, nil
	}

	opts := options.Find().SetCollation(usersCollation)
	cursor, err := usersCollection(msc).Find(ctx, bson.M{"$or": fieldsToMatch}, opts)
	if err != nil {
		return nil, err
	}

	if err = cursor.All(ctx, &results); err != nil {
		return results, err
	}

	if results == nil {
		log.Printf("no users found: query: (Id = %v) OR (Name ~= %v) OR (Emails IN %v)", user.Id, user.Username, user.Emails)
		results = []*User{}
	}

	return results, nil
}

// EnsureIndexes exist for the MongoDB collection. EnsureIndexes uses the Background() context, in order
// to pass back the MongoDB errors, rather than any context errors.
func (msc *MongoStoreClient) EnsureIndexes() error {
	usersIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "userid", Value: 1}},
			Options: options.Index().
				SetCollation(usersCollation).
				SetUnique(true).
				SetBackground(true),
		},
		{
			Keys: bson.D{{Key: "username", Value: 1}},
			Options: options.Index().
				SetCollation(usersCollation).
				SetBackground(true),
		},
		{
			Keys: bson.D{{Key: "emails", Value: 1}},
			Options: options.Index().
				SetCollation(usersCollation).
				SetBackground(true),
		},
	}

	if _, err := usersCollection(msc).Indexes().CreateMany(context.Background(), usersIndexes); err != nil {
		log.Fatal(userStoreAPIPrefix, fmt.Sprintf("Unable to create users indexes: %s", err))
	}

	// Add indexes for tokens
	tokenIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "expiresAt", Value: 1}},
			Options: options.Index().
				SetName("ExpireTokens").
				SetExpireAfterSeconds(0).
				SetBackground(true),
		},
	}

	if _, err := tokensCollection(msc).Indexes().CreateMany(context.Background(), tokenIndexes); err != nil {
		log.Fatal(userStoreAPIPrefix, fmt.Sprintf("Unable to create token indexes: %s", err))
	}

	return nil
}