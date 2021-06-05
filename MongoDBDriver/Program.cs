using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace MongoDBDriver
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // my ref:https://developer.mongodb.com/quickstart/csharp-crud-tutorial/
            //The first step is to pass in the MongoDB Atlas connection string into a MongoClient object, then we can get the list of databases and print them out.
            //var client = new MongoClient("mongodb+srv://mmohsen:28101994@mflx.b0for.mongodb.net/test");
            //  var database = client.GetDatabase("test");;

            //MongoClient dbClient = new MongoClient("mongodb+srv://mmohsen:28101994M@cluster0.a3oui.mongodb.net/test");
            //var dbList = dbClient.ListDatabases().ToList();
            //Console.WriteLine("The list of databases on this server is: ");
            //foreach (var db in dbList)
            //{
            //    Console.WriteLine(db);
            //}

            #region let work on sample_restaurants db 
            var client = new MongoClient("mongodb+srv://mmohsen:28101994M@mflx.b0for.mongodb.net/test?retryWrites=true&w=majority");
            var database = client.GetDatabase("sample_training");
            var collection = database.GetCollection<BsonDocument>("grades");

            var document = new BsonDocument
            {
                { "student_id", 10000 },
                { "scores", new BsonArray
                    {
                    new BsonDocument{ {"type", "exam"}, {"score", 88.12334193287023 } },
                    new BsonDocument{ {"type", "quiz"}, {"score", 74.92381029342834 } },
                    new BsonDocument{ {"type", "homework"}, {"score", 89.97929384290324 } },
                    new BsonDocument{ {"type", "homework"}, {"score", 82.12931030513218 } }
                    }
                },
                { "class_id", 480}
            };
            // explore writting new Document in two way InsertOne and InsertMany
            try
            {
                await collection.InsertOneAsync(document);
            }
            catch (MongoWriteException ex)
            {
                Console.WriteLine(ex.Message);
            }
            var firstDocument = collection.Find(new BsonDocument()).FirstOrDefault();
            Console.WriteLine(firstDocument.ToString());

            var filter = Builders<BsonDocument>.Filter.Eq("student_id", 10000);
            var studentDocument = collection.Find(filter).FirstOrDefault();
            Console.WriteLine(studentDocument.ToString());
            #endregion

            #region Reading All Documents
            var documents = collection.Find(new BsonDocument()).ToList();
            //We're filtering on documents in which inside the scores array there is an exam subdocument with a score value greater than or equal to 95.
            var highExamScoreFilter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>(
                "scores", new BsonDocument { { "type", "exam" },
                { "score", new BsonDocument { { "$gte", 95 } } }
              });
            var highExamScores = collection.Find(highExamScoreFilter).ToList();


            var cursor = collection.Find(highExamScoreFilter).ToCursor();
            foreach (var doc in cursor.ToEnumerable())
            {
                Console.WriteLine(doc);
            }
            await collection.Find(highExamScoreFilter).ForEachAsync(document => Console.WriteLine(document));

            #endregion

            #region Sorting 
            var sort = Builders<BsonDocument>.Sort.Descending("student_id");
            var highestScores = collection.Find(highExamScoreFilter).Sort(sort);

            var highestScore = collection.Find(highExamScoreFilter).Sort(sort).First();
            Console.WriteLine(highestScore);
            #endregion
            #region Updating exists data 
            // we have UpdateOneAsync , UpdateManyAsync and FindOneAndUpdateAsync
            //To update a document we need two bits to pass into an Update command.
            //We need a filter to determine which documents will be updated. Second, we need what we're wanting to update.
            //findOneAndUpdate returns a document, updateOne doesn't (it just returns the id if it has created a new document).
            //So the use case of updateOne is when you don't need the document and want to save a bit of time and bandwidth.
            var filterDocument = Builders<BsonDocument>.Filter.Eq("student_id", 10000);
            var update = Builders<BsonDocument>.Update.Set("class_id", 483);
            collection.UpdateOne(filter, update);
            #endregion

            #region Deleteing  
            var deleteFilter = Builders<BsonDocument>.Filter.Eq("student_id", 10000);
            collection.DeleteOne(deleteFilter);

            var deleteLowExamFilter = Builders<BsonDocument>.Filter.ElemMatch<BsonValue>("scores",
             new BsonDocument { { "type", "exam" }, {"score", new BsonDocument { { "$lt", 60 }}}
            });
            collection.DeleteMany(deleteLowExamFilter);
            #endregion


            #region How to build an aggregation pipeline with C# Driven 

            var matchStage = new BsonDocument("$match", new BsonDocument("type", "quiz"));
            var sortStage = new BsonDocument("$sort", new BsonDocument("score", -1));
            var projectionStage = new BsonDocument("$project", new BsonDocument
            {
                {"_student_id", 0}, {"Score Student", "$score" }
            });
            // we put the stages together in pipeline.
            // Note:pipeline definition requires us to specifiy the input and output.
            var pipeline = PipelineDefinition<BsonDocument, BsonDocument>
                .Create(new BsonDocument[]{
                    matchStage, 
                    sortStage,
                    projectionStage
                });
            var result = collection.Aggregate(pipeline).ToList();
            #endregion
        }
    }
}
