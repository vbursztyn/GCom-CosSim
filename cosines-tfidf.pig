REGISTER CosinesUDF.jar

raw = LOAD 'tfidf.csv' USING PigStorage(',') AS (id, p, s);
vectors = GROUP raw BY id;
doubled = FOREACH vectors GENERATE *;
crossed = CROSS vectors, doubled;
cosines = FOREACH crossed GENERATE $0,$2,com.globo.pig.udf.Cosines(TOTUPLE($1,$3));

STORE cosines INTO 'cossim-results.csv' USING PigStorage(',');
