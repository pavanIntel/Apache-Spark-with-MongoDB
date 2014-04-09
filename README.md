Apache-Spark-with-MongoDB
=========================
Apache Spark built on Hadoop and HDFS, it is compatible with any HDFS data source. so
the same is used as the Mongo-Hadoop Conncetor, which allows reading and writing of
the data directly from a Mongo database and Stored back after performing the Required
operations.

Versions and APIs:

Spark : spark-0.9.0-incubatin-bin-hadoop2
hadoop: hadoop-2.2.0
Spark supported hadoop API: spark-assembly_2.10-0.9.0-incubating-hadoop2.2.0
Mongo-Hadoop Connector: mongo-hadoop-core_2.2.0-1.2.0

SparkNameCount Application:

The sample usecase for word count of a person name accross multiple collections in
Mongo DB
Note: Here name is the key in Mongo DB input collections

Create mongoSpark maven/Java Project in eclipse. And build with required jars.(listed in
the above API section)

Create a Class MongoSparkNamecount and Configure mongo-hadoop propeties by using
MultiCollectionSplitBuilder , MultiMongoCollectionSplitter and MongoConfigUtil to
process(count name) multiple collections at a time by Specifying MongoDB input and output collections.
 

                MultiCollectionSplitBuilder mcsb=new MultiCollectionSplitBuilder();
		
		mcsb.add(new MongoURI("mongodb://localhost:27017/"+collection[1]), (MongoURI)null, true, (DBObject)null                 ,(DBObject)null, (DBObject)null, false, null)
	        .add(new MongoURI("mongodb://localhost:27017/"+collection[2]), (MongoURI)null, true,                                     (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		.add(new MongoURI("mongodb://localhost:27017/"+collection[3]), (MongoURI)null, true,                                     (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		.add(new MongoURI("mongodb://localhost:27017/"+collection[4]), (MongoURI)null, true,                                     (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		.add(new MongoURI("mongodb://localhost:27017/"+collection[5]), (MongoURI)null, true,                                     (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		.add(new MongoURI("mongodb://localhost:27017/"+collection[6]), (MongoURI)null, true,                                     (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		.add(new MongoURI("mongodb://localhost:27017/"+collection[7]), (MongoURI)null, true,                                     (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		 .add(new MongoURI("mongodb://localhost:27017/"+collection[8]), (MongoURI)null, true,                                     (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		    .add(new MongoURI("mongodb://localhost:27017/"+collection[9]), (MongoURI)null, true, (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		    .add(new MongoURI("mongodb://localhost:27017/"+collection[0]), (MongoURI)null, true, (DBObject)null,(DBObject)null, (DBObject)null, false, null)
		    .add(new MongoURI("mongodb://localhost:27017/"+collection[2]), (MongoURI)null, true, (DBObject)null,(DBObject)null, (DBObject)null, false, null);
		   
		
        config.set(MultiMongoCollectionSplitter.MULTI_COLLECTION_CONF_KEY, mcsb.toJSON());
        MongoConfigUtil.setSplitterClass(config, MultiMongoCollectionSplitter.class);
        
     
	MongoConfigUtil.setOutputURI(config,"mongodb://localhost:27017/"+output );
		
Access mongo Input with newApiHadoopRDD() method and then process with flatMap()
inorder to list all the names from multiple collecitons into sigle list.
Note: Here name is the key in mongodb input collection

        JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config,
				com.mongodb.hadoop.MongoInputFormat.class, Object.class,
				BSONObject.class);
				
        JavaRDD<String> words = mongoRDD
				.flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, String>() {

					@Override
					public Iterable<String> call(Tuple2<Object, BSONObject> arg) {
						//System.out.println(arg._2.get("name"));
						//System.exit(1);
						
						DBObject name=(DBObject) arg._2.get("name");
						String first=name.get("first").toString();
						String last=name.get("last").toString(); 
						String text=first+" "+last;
						
						
						Object o = text;
						if (o instanceof String) {
							String str = (String) o;
							str = str.toLowerCase().replaceAll("[.,!?\n]", " ");

							return Arrays.asList(str.split(" "));
						} else {
							return Collections.emptyList();
						}
					}

				});
				
Then perform map() method on list of words so that we will get <String,Integer> pair as
output. Then perform reduceByKey() method to caluculate count of words.	

            JavaPairRDD<String, Integer> ones = words
				.map(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						
						
						
						return new Tuple2<String, Integer>(s, 1);
					}
				});

		JavaPairRDD<String, Integer> counts = ones
				.reduceByKey(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {
						return i1 + i2;
					}
				});
				

				
Prepare a BsonObject with <word,count> as key and value pairs. Then save that bson object
back to mongoDB.

              JavaPairRDD<Object, BSONObject> save = counts
				.map(new PairFunction<Tuple2<String, Integer>, Object, BSONObject>() {
					@Override
					public Tuple2<Object, BSONObject> call(
							Tuple2<String, Integer> tuple) {
						BSONObject bson = new BasicBSONObject();
						bson.put("word", tuple._1);
						bson.put("count", tuple._2);
						System.out.println(tuple._1+"  "+tuple._2);
						return new Tuple2<Object, BSONObject>(null, bson);
					}
				});

		// Only MongoOutputFormat and config are relevant
		save.saveAsNewAPIHadoopFile("file:///empty", Object.class,
				Object.class, MongoOutputFormat.class, config);
				
Run the Application and check the output collection in mongodb.



				


